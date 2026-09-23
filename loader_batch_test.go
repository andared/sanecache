package sanecache

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// batchRecorder is a BatchLoader that answers from a fixed table and remembers
// every batch it was asked for. When gate is set, it parks each call until gate
// is closed or its context ends.
type batchRecorder struct {
	table map[string]int
	gate  chan struct{}

	entered chan struct{}
	mu      sync.Mutex
	batches [][]string
	ctxErrs []error
}

func newBatchRecorder(table map[string]int, gated bool) *batchRecorder {
	r := &batchRecorder{table: table, entered: make(chan struct{}, 64)}
	if gated {
		r.gate = make(chan struct{})
	}

	return r
}

func (r *batchRecorder) load(ctx context.Context, keys []string) (map[string]int, error) {
	r.mu.Lock()
	r.batches = append(r.batches, slices.Clone(keys))
	r.mu.Unlock()
	r.entered <- struct{}{}

	if r.gate != nil {
		select {
		case <-r.gate:
		case <-ctx.Done():
			r.mu.Lock()
			r.ctxErrs = append(r.ctxErrs, ctx.Err())
			r.mu.Unlock()

			return nil, ctx.Err()
		}
	}

	out := make(map[string]int, len(keys))
	for _, k := range keys {
		if v, ok := r.table[k]; ok {
			out[k] = v
		}
	}

	return out, nil
}

func (r *batchRecorder) calls() [][]string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return slices.Clone(r.batches)
}

func sorted(keys []string) []string {
	keys = slices.Clone(keys)
	slices.Sort(keys)

	return keys
}

// TestGetManyOrLoadAsksOnlyForWhatIsMissing is the point of a batch loader: one
// upstream call for everything the cache cannot answer, and nothing else.
func TestGetManyOrLoadAsksOnlyForWhatIsMissing(t *testing.T) {
	r := newBatchRecorder(map[string]int{"a": 1, "b": 2, "c": 3, "d": 4}, false)
	c := New(Options[string, int]{TTL: time.Minute, NegativeTTL: time.Minute, BatchLoader: r.load})
	defer c.Close()

	if err := c.Set("a", 10); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := c.SetNegative("x"); err != nil {
		t.Fatalf("SetNegative: %v", err)
	}

	got, err := c.GetManyOrLoad(context.Background(), []string{"a", "b", "x", "c"})
	if err != nil {
		t.Fatalf("GetManyOrLoad: %v", err)
	}
	want := map[string]int{"a": 10, "b": 2, "c": 3}
	if !maps.Equal(got, want) {
		t.Fatalf("got %v; want %v", got, want)
	}

	calls := r.calls()
	if len(calls) != 1 || !slices.Equal(calls[0], []string{"b", "c"}) {
		t.Fatalf("loader calls = %v; want one call for [b c]", calls)
	}

	st := c.Stats()
	if st.Batches != 1 || st.Loads != 2 || st.LoadErrors != 0 {
		t.Fatalf("batches/loads/errors = %d/%d/%d; want 1/2/0", st.Batches, st.Loads, st.LoadErrors)
	}

	// Everything it loaded is cached before it returns.
	if _, err := c.GetManyOrLoad(context.Background(), []string{"a", "b", "c", "x"}); err != nil {
		t.Fatalf("second GetManyOrLoad: %v", err)
	}
	if n := len(r.calls()); n != 1 {
		t.Fatalf("loader ran %d times; want 1", n)
	}
}

func TestGetManyOrLoadRemembersAbsentKeys(t *testing.T) {
	r := newBatchRecorder(map[string]int{"a": 1}, false)
	c := New(Options[string, int]{TTL: time.Minute, NegativeTTL: time.Minute, BatchLoader: r.load})
	defer c.Close()

	got, err := c.GetManyOrLoad(context.Background(), []string{"a", "gone"})
	if err != nil {
		t.Fatalf("GetManyOrLoad: %v", err)
	}
	if !maps.Equal(got, map[string]int{"a": 1}) {
		t.Fatalf("got %v; want only a", got)
	}
	if _, st := c.Lookup("gone"); st != StatusNegative {
		t.Fatalf("absent key status = %v; want negative", st)
	}
	if _, err := c.GetOrLoad(context.Background(), "gone"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetOrLoad of the absent key = %v; want ErrNotFound", err)
	}
	if n := len(r.calls()); n != 1 {
		t.Fatalf("loader ran %d times; want 1", n)
	}
	if st := c.Stats(); st.Loads != 2 || st.LoadErrors != 1 {
		t.Fatalf("loads/errors = %d/%d; want 2/1, a missing key counts as a failed load as it does for Loader",
			st.Loads, st.LoadErrors)
	}
}

func TestGetManyOrLoadWithoutNegativeTTLKeepsAsking(t *testing.T) {
	r := newBatchRecorder(map[string]int{}, false)
	c := New(Options[string, int]{TTL: time.Minute, BatchLoader: r.load})
	defer c.Close()

	for range 2 {
		got, err := c.GetManyOrLoad(context.Background(), []string{"gone"})
		if err != nil || len(got) != 0 {
			t.Fatalf("GetManyOrLoad = %v, %v; want empty, nil", got, err)
		}
	}
	if n := len(r.calls()); n != 2 {
		t.Fatalf("loader ran %d times; want 2", n)
	}
}

// TestGetManyOrLoadIgnoresKeysItDidNotAskFor: a loader that answers more than
// it was asked must not fill the cache behind the invalidation that guards the
// keys it was asked for.
func TestGetManyOrLoadIgnoresKeysItDidNotAskFor(t *testing.T) {
	c := New(Options[string, int]{
		TTL: time.Minute,
		BatchLoader: func(context.Context, []string) (map[string]int, error) {
			return map[string]int{"a": 1, "extra": 2}, nil
		},
	})
	defer c.Close()

	got, err := c.GetManyOrLoad(context.Background(), []string{"a"})
	if err != nil || !maps.Equal(got, map[string]int{"a": 1}) {
		t.Fatalf("GetManyOrLoad = %v, %v; want only a", got, err)
	}
	if _, ok := c.Get("extra"); ok {
		t.Fatal("a key nobody asked for was cached")
	}
}

func TestGetManyOrLoadDeduplicatesAndSkipsEmptyInput(t *testing.T) {
	r := newBatchRecorder(map[string]int{"a": 1, "b": 2}, false)
	c := New(Options[string, int]{TTL: time.Minute, BatchLoader: r.load})
	defer c.Close()

	got, err := c.GetManyOrLoad(context.Background(), nil)
	if err != nil || len(got) != 0 {
		t.Fatalf("GetManyOrLoad(nil) = %v, %v; want empty, nil", got, err)
	}
	if n := len(r.calls()); n != 0 {
		t.Fatalf("loader ran %d times for no keys", n)
	}

	got, err = c.GetManyOrLoad(context.Background(), []string{"a", "b", "a", "b"})
	if err != nil || !maps.Equal(got, map[string]int{"a": 1, "b": 2}) {
		t.Fatalf("GetManyOrLoad = %v, %v", got, err)
	}
	if calls := r.calls(); len(calls) != 1 || !slices.Equal(calls[0], []string{"a", "b"}) {
		t.Fatalf("loader calls = %v; want one call for [a b]", calls)
	}
}

// TestGetManyOrLoadDoesNotCacheFailures: a failed batch fails as a whole, the
// part of an answer that came with the error included.
func TestGetManyOrLoadDoesNotCacheFailures(t *testing.T) {
	var calls atomic.Int64
	c := New(Options[string, int]{
		TTL:         time.Minute,
		NegativeTTL: time.Minute,
		BatchLoader: func(context.Context, []string) (map[string]int, error) {
			if calls.Add(1) == 1 {
				return map[string]int{"a": 1}, errUpstream
			}

			return map[string]int{"a": 1, "b": 2}, nil
		},
	})
	defer c.Close()

	if err := c.Set("cached", 7); err != nil {
		t.Fatalf("Set: %v", err)
	}

	got, err := c.GetManyOrLoad(context.Background(), []string{"cached", "a", "b"})
	if !errors.Is(err, errUpstream) {
		t.Fatalf("err = %v; want the loader's error", err)
	}
	if !maps.Equal(got, map[string]int{"cached": 7}) {
		t.Fatalf("got %v; want only what was cached", got)
	}
	for _, k := range []string{"a", "b"} {
		if _, st := c.Lookup(k); st != StatusMiss {
			t.Fatalf("%s after a failed batch: %v; want miss", k, st)
		}
	}

	got, err = c.GetManyOrLoad(context.Background(), []string{"a", "b"})
	if err != nil || !maps.Equal(got, map[string]int{"a": 1, "b": 2}) {
		t.Fatalf("retry = %v, %v; want a and b", got, err)
	}
	if st := c.Stats(); st.Batches != 2 || st.Loads != 4 || st.LoadErrors != 2 {
		t.Fatalf("batches/loads/errors = %d/%d/%d; want 2/4/2", st.Batches, st.Loads, st.LoadErrors)
	}
}

// TestBatchAndSingleLoadsShareFlights: a key is loaded once whichever way it
// is asked for, and a batch only carries the keys nobody is loading yet.
func TestBatchAndSingleLoadsShareFlights(t *testing.T) {
	r := newBatchRecorder(map[string]int{"a": 1, "b": 2, "c": 3}, true)
	c := New(Options[string, int]{TTL: time.Minute, BatchLoader: r.load})
	defer c.Close()

	// A single GetOrLoad starts a batch of one for a.
	single := make(chan error, 1)
	go func() {
		v, err := c.GetOrLoad(context.Background(), "a")
		if err == nil && v != 1 {
			err = fmt.Errorf("got %d; want 1", v)
		}
		single <- err
	}()
	<-r.entered

	// A batch joins a and loads b alone.
	batch1 := make(chan map[string]int, 1)
	go func() {
		got, err := c.GetManyOrLoad(context.Background(), []string{"a", "b"})
		if err != nil {
			t.Errorf("first batch: %v", err)
		}
		batch1 <- got
	}()
	<-r.entered

	// A second batch joins both of those and loads c alone; a single call for
	// b joins the first batch.
	batch2 := make(chan map[string]int, 1)
	go func() {
		got, err := c.GetManyOrLoad(context.Background(), []string{"b", "c", "a"})
		if err != nil {
			t.Errorf("second batch: %v", err)
		}
		batch2 <- got
	}()
	<-r.entered
	singleB := make(chan int, 1)
	go func() {
		v, _ := c.GetOrLoad(context.Background(), "b")
		singleB <- v
	}()
	waitFor(t, "every caller to join", func() bool { return c.Stats().Coalesced == 4 })

	close(r.gate)
	if err := <-single; err != nil {
		t.Fatalf("GetOrLoad(a): %v", err)
	}
	if got := <-batch1; !maps.Equal(got, map[string]int{"a": 1, "b": 2}) {
		t.Fatalf("first batch got %v", got)
	}
	if got := <-batch2; !maps.Equal(got, map[string]int{"a": 1, "b": 2, "c": 3}) {
		t.Fatalf("second batch got %v", got)
	}
	if v := <-singleB; v != 2 {
		t.Fatalf("GetOrLoad(b) = %d; want 2", v)
	}

	calls := r.calls()
	var loaded []string
	for _, b := range calls {
		loaded = append(loaded, b...)
	}
	if !slices.Equal(sorted(loaded), []string{"a", "b", "c"}) {
		t.Fatalf("loader calls = %v; want each key exactly once", calls)
	}
	if st := c.Stats(); st.Batches != 3 || st.Loads != 3 {
		t.Fatalf("batches/loads = %d/%d; want 3/3", st.Batches, st.Loads)
	}
}

// TestDeleteDuringBatchKeepsOnlyThatKeyOut: invalidation is per key, not per
// batch, exactly as it is per load for GetOrLoad.
func TestDeleteDuringBatchKeepsOnlyThatKeyOut(t *testing.T) {
	r := newBatchRecorder(map[string]int{"a": 1, "b": 2}, true)
	c := New(Options[string, int]{TTL: time.Minute, BatchLoader: r.load})
	defer c.Close()

	done := make(chan map[string]int, 1)
	go func() {
		got, _ := c.GetManyOrLoad(context.Background(), []string{"a", "b"})
		done <- got
	}()
	<-r.entered

	c.Delete("a")

	// A new caller does not join the invalidated load of a, but does join b's.
	joined := make(chan map[string]int, 1)
	go func() {
		got, _ := c.GetManyOrLoad(context.Background(), []string{"a", "b"})
		joined <- got
	}()
	<-r.entered
	close(r.gate)

	// Callers already waiting still receive the result they were waiting for.
	if got := <-done; !maps.Equal(got, map[string]int{"a": 1, "b": 2}) {
		t.Fatalf("waiting caller got %v", got)
	}
	if got := <-joined; !maps.Equal(got, map[string]int{"a": 1, "b": 2}) {
		t.Fatalf("later caller got %v", got)
	}
	calls := r.calls()
	if len(calls) != 2 || !slices.Equal(calls[1], []string{"a"}) {
		t.Fatalf("loader calls = %v; want the second batch to reload a only", calls)
	}
	if _, ok := c.Get("b"); !ok {
		t.Fatal("b from the first batch was not cached")
	}
}

func TestDeleteDuringBatchDoesNotPublishTheStaleValue(t *testing.T) {
	r := newBatchRecorder(map[string]int{"a": 1, "b": 2}, true)
	c := New(Options[string, int]{TTL: time.Minute, BatchLoader: r.load})
	defer c.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = c.GetManyOrLoad(context.Background(), []string{"a", "b"})
	}()
	<-r.entered
	c.Delete("a")
	close(r.gate)
	<-done

	if _, ok := c.Get("a"); ok {
		t.Fatal("a load invalidated by Delete put its value back")
	}
	if v, ok := c.Get("b"); !ok || v != 2 {
		t.Fatalf("b = %v, %v; want 2, true", v, ok)
	}
}

// TestBatchSurvivesACallerWhoGivesUp: one caller's deadline does not cancel
// the upstream call that another caller is still waiting for, even when that
// other caller wants a different key of the same batch.
func TestBatchSurvivesACallerWhoGivesUp(t *testing.T) {
	r := newBatchRecorder(map[string]int{"a": 1, "b": 2}, true)
	c := New(Options[string, int]{TTL: time.Minute, BatchLoader: r.load})
	defer c.Close()

	first, cancelFirst := context.WithCancel(context.Background())
	firstErr := make(chan error, 1)
	go func() {
		_, err := c.GetManyOrLoad(first, []string{"a", "b"})
		firstErr <- err
	}()
	<-r.entered

	second := make(chan int, 1)
	go func() {
		v, err := c.GetOrLoad(context.Background(), "b")
		if err != nil {
			t.Errorf("second caller: %v", err)
		}
		second <- v
	}()
	waitFor(t, "the second caller to join", func() bool { return c.Stats().Coalesced == 1 })

	cancelFirst()
	if err := <-firstErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("first caller err = %v; want context.Canceled", err)
	}

	close(r.gate)
	if v := <-second; v != 2 {
		t.Fatalf("second caller got %d; want 2", v)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.ctxErrs) != 0 {
		t.Fatalf("the batch was cancelled with a caller still waiting: %v", r.ctxErrs)
	}
}

func TestBatchStopsWhenEveryCallerGivesUp(t *testing.T) {
	r := newBatchRecorder(map[string]int{"a": 1, "b": 2}, true)
	c := New(Options[string, int]{TTL: time.Minute, BatchLoader: r.load})
	defer c.Close()

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() {
		_, err := c.GetManyOrLoad(ctx, []string{"a", "b"})
		errc <- err
	}()
	<-r.entered
	cancel()
	if err := <-errc; !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v; want context.Canceled", err)
	}

	waitFor(t, "the batch to be cancelled", func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()

		return len(r.ctxErrs) == 1
	})

	// The abandoned calls are gone: the next caller starts a batch of its own
	// rather than joining one nobody is finishing.
	close(r.gate)
	got, err := c.GetManyOrLoad(context.Background(), []string{"a", "b"})
	if err != nil || !maps.Equal(got, map[string]int{"a": 1, "b": 2}) {
		t.Fatalf("GetManyOrLoad after the abandoned batch = %v, %v", got, err)
	}
	if n := len(r.calls()); n != 2 {
		t.Fatalf("loader ran %d times; want 2", n)
	}
}

func TestGetManyOrLoadWithAnAlreadyCancelledContext(t *testing.T) {
	r := newBatchRecorder(map[string]int{"a": 1}, false)
	c := New(Options[string, int]{TTL: time.Minute, BatchLoader: r.load})
	defer c.Close()

	if err := c.Set("cached", 3); err != nil {
		t.Fatalf("Set: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	got, err := c.GetManyOrLoad(ctx, []string{"cached", "a"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v; want context.Canceled", err)
	}
	if !maps.Equal(got, map[string]int{"cached": 3}) {
		t.Fatalf("got %v; want what was cached", got)
	}
	if n := len(r.calls()); n != 0 {
		t.Fatalf("loader ran %d times for a caller that had already given up", n)
	}
}

func TestGetManyOrLoadCarriesAPanicToTheCaller(t *testing.T) {
	var calls atomic.Int64
	c := New(Options[string, int]{
		TTL: time.Minute,
		BatchLoader: func(_ context.Context, keys []string) (map[string]int, error) {
			if calls.Add(1) == 1 {
				panic("boom")
			}
			out := make(map[string]int, len(keys))
			for _, k := range keys {
				out[k] = len(k)
			}

			return out, nil
		},
	})
	defer c.Close()

	func() {
		defer func() {
			r := recover()
			err, ok := r.(error)
			if !ok || !strings.Contains(err.Error(), "boom") {
				t.Fatalf("recovered %v; want an error naming the panic", r)
			}
			if !strings.Contains(err.Error(), "sanecache.(*core[...]).runBatch") {
				t.Fatalf("recovered error lost the loader's stack: %v", err)
			}
		}()
		_, _ = c.GetManyOrLoad(context.Background(), []string{"a", "bb"})
	}()

	got, err := c.GetManyOrLoad(context.Background(), []string{"a", "bb"})
	if err != nil || !maps.Equal(got, map[string]int{"a": 1, "bb": 2}) {
		t.Fatalf("GetManyOrLoad after a panic = %v, %v", got, err)
	}
	if st := c.Stats(); st.Batches != 2 || st.Loads != 4 || st.LoadErrors != 2 {
		t.Fatalf("batches/loads/errors = %d/%d/%d; want 2/4/2", st.Batches, st.Loads, st.LoadErrors)
	}
}

// TestGetOrLoadWithOnlyABatchLoader: one loader is enough for both paths.
func TestGetOrLoadWithOnlyABatchLoader(t *testing.T) {
	r := newBatchRecorder(map[string]int{"a": 1}, false)
	c := New(Options[string, int]{TTL: time.Minute, NegativeTTL: time.Minute, BatchLoader: r.load})
	defer c.Close()

	if v, err := c.GetOrLoad(context.Background(), "a"); err != nil || v != 1 {
		t.Fatalf("GetOrLoad(a) = %v, %v; want 1, nil", v, err)
	}
	if _, err := c.GetOrLoad(context.Background(), "gone"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetOrLoad(gone) err = %v; want ErrNotFound", err)
	}
	if _, st := c.Lookup("gone"); st != StatusNegative {
		t.Fatalf("gone status = %v; want negative", st)
	}
	calls := r.calls()
	if len(calls) != 2 || !slices.Equal(calls[0], []string{"a"}) || !slices.Equal(calls[1], []string{"gone"}) {
		t.Fatalf("loader calls = %v; want [[a] [gone]]", calls)
	}
	if st := c.Stats(); st.Batches != 2 || st.Loads != 2 {
		t.Fatalf("batches/loads = %d/%d; want 2/2", st.Batches, st.Loads)
	}
}

// TestGetManyOrLoadWithOnlyALoader: without a batch loader the missing keys
// still load in parallel, not one after another.
func TestGetManyOrLoadWithOnlyALoader(t *testing.T) {
	release := make(chan struct{})
	var inFlight atomic.Int64
	c := New(Options[string, int]{
		TTL: time.Minute,
		Loader: func(ctx context.Context, key string) (int, error) {
			inFlight.Add(1)
			select {
			case <-release:
			case <-ctx.Done():
				return 0, ctx.Err()
			}
			if key == "gone" {
				return 0, ErrNotFound
			}

			return len(key), nil
		},
	})
	defer c.Close()

	done := make(chan map[string]int, 1)
	go func() {
		got, err := c.GetManyOrLoad(context.Background(), []string{"a", "bb", "gone"})
		if err != nil {
			t.Errorf("GetManyOrLoad: %v", err)
		}
		done <- got
	}()
	waitFor(t, "every key to be loading at once", func() bool { return inFlight.Load() == 3 })
	close(release)

	if got := <-done; !maps.Equal(got, map[string]int{"a": 1, "bb": 2}) {
		t.Fatalf("got %v", got)
	}
	if st := c.Stats(); st.Loads != 3 || st.Batches != 0 {
		t.Fatalf("loads/batches = %d/%d; want 3/0", st.Loads, st.Batches)
	}
}

func TestGetManyOrLoadNeedsALoader(t *testing.T) {
	c := New(Options[string, int]{})
	defer c.Close()

	if _, err := c.GetManyOrLoad(context.Background(), []string{"a"}); !errors.Is(err, ErrNoLoader) {
		t.Fatalf("err = %v; want ErrNoLoader", err)
	}
}

// TestGetManyOrLoadUnderConcurrency: overlapping batches and single loads from
// many goroutines at once load every key exactly once.
func TestGetManyOrLoadUnderConcurrency(t *testing.T) {
	const keys = 64
	var mu sync.Mutex
	loaded := make(map[string]int)
	c := New(Options[string, int]{
		TTL:    time.Minute,
		Shards: 8,
		BatchLoader: func(_ context.Context, ks []string) (map[string]int, error) {
			time.Sleep(time.Millisecond)
			out := make(map[string]int, len(ks))
			mu.Lock()
			defer mu.Unlock()
			for _, k := range ks {
				loaded[k]++
				out[k] = len(k)
			}

			return out, nil
		},
	})
	defer c.Close()

	var wg sync.WaitGroup
	for g := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(g), 0))
			for range 50 {
				if rng.IntN(4) == 0 {
					k := fmt.Sprint(rng.IntN(keys))
					if v, err := c.GetOrLoad(context.Background(), k); err != nil || v != len(k) {
						t.Errorf("GetOrLoad(%s) = %v, %v", k, v, err)
					}

					continue
				}
				batch := make([]string, 1+rng.IntN(8))
				for i := range batch {
					batch[i] = fmt.Sprint(rng.IntN(keys))
				}
				got, err := c.GetManyOrLoad(context.Background(), batch)
				if err != nil {
					t.Errorf("GetManyOrLoad: %v", err)
				}
				for _, k := range batch {
					if got[k] != len(k) {
						t.Errorf("GetManyOrLoad(%v)[%s] = %d", batch, k, got[k])
					}
				}
			}
		}()
	}
	wg.Wait()

	for k, n := range loaded {
		if n != 1 {
			t.Fatalf("key %s loaded %d times; want 1", k, n)
		}
	}
}
