package sanecache

import (
	"context"
	"errors"
	"maps"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestViewGetManyOrLoad(t *testing.T) {
	r := newBatchRecorder(map[string]int{"a": 1, "b": 2, "c": 3, "t": 4}, false)
	c := New(Options[string, any]{TTL: time.Minute, NegativeTTL: time.Minute, Shards: 4})
	defer c.Close()
	v := NewView(c, ViewOptions[int]{Name: "n", BatchLoader: r.load})

	if err := v.Set("a", 10); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := v.SetNegative("x"); err != nil {
		t.Fatalf("SetNegative: %v", err)
	}
	// Another type under the view's key is a miss the batch replaces.
	if err := c.Set("n:t", "text"); err != nil {
		t.Fatalf("Set: %v", err)
	}

	got, err := v.GetManyOrLoad(context.Background(), []string{"a", "b", "x", "t", "gone", "b"})
	if err != nil {
		t.Fatalf("GetManyOrLoad: %v", err)
	}
	if want := map[string]int{"a": 10, "b": 2, "t": 4}; !maps.Equal(got, want) {
		t.Fatalf("got %v; want %v", got, want)
	}
	calls := r.calls()
	if len(calls) != 1 || !slices.Equal(sorted(calls[0]), []string{"b", "gone", "t"}) {
		t.Fatalf("loader calls = %v; want one call for the view's own keys b, gone, t", calls)
	}

	// Stored under the prefix as the view's type, the absent key as a negative.
	if raw, ok := c.Get("n:b"); !ok || raw != 2 {
		t.Fatalf("cache holds n:b = %v, %v", raw, ok)
	}
	if _, st := v.Lookup("gone"); st != StatusNegative {
		t.Fatalf("gone: %v; want negative", st)
	}

	if st := v.Stats(); st.Batches != 1 || st.Loads != 3 || st.LoadNotFound != 1 || st.TypeMisses != 1 {
		t.Fatalf("view stats: %+v", st)
	}
	if st := c.Stats(); st.Batches != 1 || st.Loads != 3 {
		t.Fatalf("cache stats: %+v", st)
	}

	if _, err := v.GetManyOrLoad(context.Background(), []string{"a", "b", "t", "gone"}); err != nil {
		t.Fatalf("second GetManyOrLoad: %v", err)
	}
	if n := len(r.calls()); n != 1 {
		t.Fatalf("loader ran %d times; want 1", n)
	}
}

// TestViewBatchAndSingleLoadsShareFlights: with only a BatchLoader, GetOrLoad
// loads through it, and a batch joins that load rather than repeating it.
func TestViewBatchAndSingleLoadsShareFlights(t *testing.T) {
	r := newBatchRecorder(map[string]int{"a": 1, "b": 2}, true)
	c := New(Options[string, any]{TTL: time.Minute})
	defer c.Close()
	v := NewView(c, ViewOptions[int]{Name: "n", BatchLoader: r.load})

	single := make(chan error, 1)
	go func() {
		_, err := v.GetOrLoad(context.Background(), "a")
		single <- err
	}()
	<-r.entered

	batch := make(chan map[string]int, 1)
	go func() {
		got, err := v.GetManyOrLoad(context.Background(), []string{"a", "b"})
		if err != nil {
			t.Errorf("GetManyOrLoad: %v", err)
		}
		batch <- got
	}()
	<-r.entered
	waitFor(t, "the batch to join a", func() bool { return v.Stats().Coalesced == 1 })

	close(r.gate)
	if err := <-single; err != nil {
		t.Fatalf("GetOrLoad: %v", err)
	}
	if got := <-batch; !maps.Equal(got, map[string]int{"a": 1, "b": 2}) {
		t.Fatalf("batch got %v", got)
	}
	if calls := r.calls(); !slices.EqualFunc(calls, [][]string{{"a"}, {"b"}}, slices.Equal) {
		t.Fatalf("loader calls = %v; want [a] then [b]", calls)
	}
	if st := v.Stats(); st.Batches != 2 || st.Loads != 2 {
		t.Fatalf("view stats: %+v", st)
	}
}

// TestViewDeleteDuringBatch: invalidation is per storage key, so a Delete
// through the cache keeps that key of a running view batch out.
func TestViewDeleteDuringBatch(t *testing.T) {
	r := newBatchRecorder(map[string]int{"a": 1, "b": 2}, true)
	c := New(Options[string, any]{TTL: time.Minute})
	defer c.Close()
	v := NewView(c, ViewOptions[int]{Name: "n", BatchLoader: r.load})

	done := make(chan struct{})
	go func() {
		defer close(done)
		if got, err := v.GetManyOrLoad(context.Background(), []string{"a", "b"}); err != nil || len(got) != 2 {
			t.Errorf("GetManyOrLoad = %v, %v", got, err)
		}
	}()
	<-r.entered
	c.Delete("n:a")
	close(r.gate)
	<-done

	if _, ok := v.Get("a"); ok {
		t.Fatal("a deleted key was published by the batch")
	}
	if _, ok := v.Get("b"); !ok {
		t.Fatal("the rest of the batch was not cached")
	}
}

func TestViewGetManyOrLoadErrorsAndFallbacks(t *testing.T) {
	c := New(Options[string, any]{TTL: time.Minute})
	defer c.Close()

	failing := NewView(c, ViewOptions[int]{
		Name:        "fail",
		BatchLoader: func(context.Context, []string) (map[string]int, error) { return nil, errUpstream },
	})
	if _, err := failing.GetManyOrLoad(context.Background(), []string{"a"}); !errors.Is(err, errUpstream) {
		t.Fatalf("err = %v; want errUpstream", err)
	}
	if _, err := failing.GetOrLoad(context.Background(), "a"); !errors.Is(err, errUpstream) {
		t.Fatalf("GetOrLoad err = %v; want errUpstream", err)
	}
	if _, st := failing.Lookup("a"); st != StatusMiss {
		t.Fatalf("a failed batch cached %v", st)
	}

	var calls atomic.Int64
	single := NewView(c, ViewOptions[int]{
		Name: "single",
		Loader: func(_ context.Context, key string) (int, error) {
			calls.Add(1)

			return len(key), nil
		},
	})
	got, err := single.GetManyOrLoad(context.Background(), []string{"a", "bb"})
	if err != nil || !maps.Equal(got, map[string]int{"a": 1, "bb": 2}) || calls.Load() != 2 {
		t.Fatalf("GetManyOrLoad = %v, %v with %d loads", got, err, calls.Load())
	}

	none := NewView(c, ViewOptions[int]{Name: "none"})
	if _, err := none.GetManyOrLoad(context.Background(), []string{"a"}); !errors.Is(err, ErrNoLoader) {
		t.Fatalf("err = %v; want ErrNoLoader", err)
	}
}

func TestViewGetManyOrLoadWithoutACache(t *testing.T) {
	r := newBatchRecorder(map[string]int{"a": 1}, false)
	v := NewView(nil, ViewOptions[int]{Name: "off", BatchLoader: r.load})

	for range 2 {
		got, err := v.GetManyOrLoad(context.Background(), []string{"a", "gone"})
		if err != nil || !maps.Equal(got, map[string]int{"a": 1}) {
			t.Fatalf("GetManyOrLoad = %v, %v", got, err)
		}
	}
	if n := len(r.calls()); n != 2 {
		t.Fatalf("loader ran %d times; want 2, nothing being kept", n)
	}
	if st := v.Stats(); st.Batches != 2 || st.Loads != 4 || st.Misses != 4 {
		t.Fatalf("stats: %+v", st)
	}

	var calls atomic.Int64
	single := NewView(nil, ViewOptions[int]{
		Name: "off-single",
		Loader: func(context.Context, string) (int, error) {
			calls.Add(1)

			return 7, nil
		},
	})
	if got, err := single.GetManyOrLoad(context.Background(), []string{"a", "b"}); err != nil || len(got) != 2 {
		t.Fatalf("GetManyOrLoad = %v, %v", got, err)
	}
	if calls.Load() != 2 {
		t.Fatalf("loader ran %d times; want 2", calls.Load())
	}
}

func TestViewGetManyOrLoadRefreshesDueKeysInOneBatch(t *testing.T) {
	rec := newBatchRecorder(map[string]int{"a": 1, "b": 2, "c": 3}, false)
	c := New(Options[string, any]{TTL: time.Hour, DisableCleanup: true})
	defer c.Close()
	advance := manualClock(c)
	v := NewView(c, ViewOptions[int]{Name: "n", TTL: refreshTTL, RefreshAfter: refreshAfter, BatchLoader: rec.load})
	ctx := context.Background()

	if _, err := v.GetManyOrLoad(ctx, []string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	advance(refreshAfter + time.Second)

	got, err := v.GetManyOrLoad(ctx, []string{"a", "b", "c"})
	if err != nil || len(got) != 3 {
		t.Fatalf("got %v, %v", got, err)
	}
	waitFor(t, "the refresh batch", func() bool { return v.Stats().Refreshes == 2 })

	rec.mu.Lock()
	var batches []string
	for _, b := range rec.batches {
		batches = append(batches, strings.Join(sorted(b), ","))
	}
	rec.mu.Unlock()
	if want := []string{"a,b", "a,b", "c"}; !slices.Equal(sorted(batches), want) {
		t.Fatalf("batches %v, want %v", batches, want)
	}
	if st := v.Stats(); st.Batches != 3 || st.Loads != 3 {
		t.Fatalf("view stats: %+v", st)
	}
}

func TestViewGetManyOrLoadRefreshesWithLoaderKeyByKey(t *testing.T) {
	var calls atomic.Int64
	c := New(Options[string, any]{TTL: time.Hour, DisableCleanup: true})
	defer c.Close()
	advance := manualClock(c)
	v := NewView(c, ViewOptions[int]{
		Name: "n", TTL: refreshTTL, RefreshAfter: refreshAfter,
		Loader: func(context.Context, string) (int, error) { return int(calls.Add(1)), nil },
	})
	ctx := context.Background()

	_, _ = v.GetManyOrLoad(ctx, []string{"a", "b"})
	advance(refreshAfter + time.Second)
	_, _ = v.GetManyOrLoad(ctx, []string{"a", "b"})
	waitFor(t, "both refreshes", func() bool { return v.Stats().Refreshes == 2 })
	if st := v.Stats(); st.Batches != 0 || calls.Load() != 4 {
		t.Fatalf("stats %+v after %d loader calls", st, calls.Load())
	}
}

func TestBatchesWithoutStats(t *testing.T) {
	r := newBatchRecorder(map[string]int{"a": 1}, false)
	c := New(Options[string, any]{TTL: time.Minute, DisableStats: true})
	defer c.Close()
	v := NewView(c, ViewOptions[int]{Name: "n", BatchLoader: r.load})
	if got, err := v.GetManyOrLoad(context.Background(), []string{"a"}); err != nil || got["a"] != 1 {
		t.Fatalf("GetManyOrLoad = %v, %v", got, err)
	}

	cr := newBatchRecorder(map[string]int{"a": 1}, false)
	ci := New(Options[string, int]{TTL: time.Minute, DisableStats: true, BatchLoader: cr.load})
	defer ci.Close()
	if got, err := ci.GetManyOrLoad(context.Background(), []string{"a"}); err != nil || got["a"] != 1 {
		t.Fatalf("GetManyOrLoad = %v, %v", got, err)
	}

	if v.Stats() != (ViewStats{}) || ci.Stats().Batches != 0 {
		t.Fatalf("counted with stats off: %+v, %+v", v.Stats(), ci.Stats())
	}
}
