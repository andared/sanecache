package sanecache

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	refreshTTL   = 10 * time.Minute
	refreshAfter = 5 * time.Minute
)

// flying reports whether a load for key is registered, which refresh does
// before it returns: a test can tell "no refresh was started" without waiting.
func flying[V any](c *Cache[string, V], key string) bool {
	g := c.core.flights[c.core.shardIndex(key)]
	g.mu.Lock()
	defer g.mu.Unlock()
	_, ok := g.calls[key]

	return ok
}

// versionLoader returns 1, 2, 3… on successive calls. Calls after the first park
// until gate is closed, so that a test can look at the cache while a refresh is
// still running.
type versionLoader struct {
	calls atomic.Int64
	gate  chan struct{}
}

func newVersionLoader() *versionLoader { return &versionLoader{gate: make(chan struct{})} }

func (l *versionLoader) load(ctx context.Context, _ string) (int, error) {
	n := l.calls.Add(1)
	if n > 1 {
		select {
		case <-l.gate:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}

	return int(n), nil
}

func newRefreshCache(t *testing.T, policy Policy, loader func(context.Context, string) (int, error)) (*Cache[string, int], func(time.Duration)) {
	t.Helper()
	c := New(Options[string, int]{
		TTL: refreshTTL, NegativeTTL: time.Minute, RefreshAfter: refreshAfter,
		Policy: policy, Loader: loader, DisableCleanup: true,
	})
	t.Cleanup(c.Close)

	return c, manualClock(c)
}

// TestRefreshServesTheOldValueAndReloadsOnce is what RefreshAfter is for: a key
// in steady use is replaced in the background, its readers never wait for the
// upstream, and however many of them see it due, the upstream is asked once.
func TestRefreshServesTheOldValueAndReloadsOnce(t *testing.T) {
	for _, policy := range []Policy{LRU, ClearOnFull} {
		t.Run(policy.String(), func(t *testing.T) {
			l := newVersionLoader()
			c, advance := newRefreshCache(t, policy, l.load)
			ctx := context.Background()

			if v, err := c.GetOrLoad(ctx, "k"); v != 1 || err != nil {
				t.Fatalf("first load: %d, %v", v, err)
			}
			advance(refreshAfter + time.Second)

			var wg sync.WaitGroup
			for range 50 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					if v, err := c.GetOrLoad(ctx, "k"); v != 1 || err != nil {
						t.Errorf("a due value is served while it refreshes: got %d, %v", v, err)
					}
				}()
			}
			wg.Wait()

			waitFor(t, "the refresh to start", func() bool { return l.calls.Load() == 2 })
			close(l.gate)
			waitFor(t, "the refreshed value", func() bool { v, _ := c.Get("k"); return v == 2 })

			if n := l.calls.Load(); n != 2 {
				t.Fatalf("loader called %d times, want 2: one load and one refresh", n)
			}
			st := c.Stats()
			if st.Loads != 1 || st.Refreshes != 1 || st.RefreshErrors != 0 || st.Misses != 1 || st.Expirations != 0 {
				t.Fatalf("stats: %+v", st)
			}
		})
	}
}

func TestRefreshWaitsUntilTheValueIsDue(t *testing.T) {
	l := newVersionLoader()
	c, advance := newRefreshCache(t, LRU, l.load)
	ctx := context.Background()

	_, _ = c.GetOrLoad(ctx, "k")
	advance(refreshAfter - time.Second)
	if v, _ := c.GetOrLoad(ctx, "k"); v != 1 {
		t.Fatalf("got %d", v)
	}
	if flying(c, "k") || l.calls.Load() != 1 {
		t.Fatal("a value refreshed before RefreshAfter")
	}
}

// TestRefreshFailureKeepsTheValueUntilItExpires pins the failure policy: one
// attempt per value, the value served to its TTL, and the error reaching a
// caller only when the TTL is over, exactly as it would without refresh.
func TestRefreshFailureKeepsTheValueUntilItExpires(t *testing.T) {
	var calls atomic.Int64
	c, advance := newRefreshCache(t, LRU, func(context.Context, string) (int, error) {
		if calls.Add(1) == 1 {
			return 1, nil
		}

		return 0, errUpstream
	})
	ctx := context.Background()

	_, _ = c.GetOrLoad(ctx, "k")
	advance(refreshAfter + time.Second)
	if v, err := c.GetOrLoad(ctx, "k"); v != 1 || err != nil {
		t.Fatalf("got %d, %v", v, err)
	}
	waitFor(t, "the refresh to fail", func() bool { return c.Stats().RefreshErrors == 1 })

	for range 5 {
		if v, err := c.GetOrLoad(ctx, "k"); v != 1 || err != nil {
			t.Fatalf("after a failed refresh: got %d, %v", v, err)
		}
	}
	if flying(c, "k") || calls.Load() != 2 {
		t.Fatalf("a failed refresh was retried: %d loader calls", calls.Load())
	}

	advance(refreshTTL)
	if _, err := c.GetOrLoad(ctx, "k"); !errors.Is(err, errUpstream) {
		t.Fatalf("past the TTL the caller loads and sees the error, got %v", err)
	}
	if st := c.Stats(); st.Refreshes != 1 || st.RefreshErrors != 1 || st.Loads != 2 || st.LoadErrors != 1 {
		t.Fatalf("stats: %+v", st)
	}
}

func TestRefreshPanicIsCountedAndContained(t *testing.T) {
	var calls atomic.Int64
	c, advance := newRefreshCache(t, LRU, func(context.Context, string) (int, error) {
		if calls.Add(1) > 1 {
			panic("boom")
		}

		return 1, nil
	})

	_, _ = c.GetOrLoad(context.Background(), "k")
	advance(refreshAfter + time.Second)
	if v, _ := c.Get("k"); v != 1 {
		t.Fatalf("got %d", v)
	}
	waitFor(t, "the refresh to fail", func() bool { return c.Stats().RefreshErrors == 1 })
	if v, ok := c.Get("k"); v != 1 || !ok {
		t.Fatalf("the value did not survive a panicking refresh: %d, %v", v, ok)
	}
}

// TestRefreshKeepsOutOfInvalidatedKeys holds a refresh to the 0.4 guarantee: a
// Delete or a successful write while it runs keeps its result out.
func TestRefreshKeepsOutOfInvalidatedKeys(t *testing.T) {
	for _, mutate := range []string{"delete", "set", "clear"} {
		t.Run(mutate, func(t *testing.T) {
			l := newVersionLoader()
			c, advance := newRefreshCache(t, LRU, l.load)

			_, _ = c.GetOrLoad(context.Background(), "k")
			advance(refreshAfter + time.Second)
			_, _ = c.Get("k")
			waitFor(t, "the refresh to start", func() bool { return l.calls.Load() == 2 })

			switch mutate {
			case "delete":
				c.Delete("k")
			case "set":
				_ = c.Set("k", 100)
			case "clear":
				c.Clear()
			}
			close(l.gate)
			waitFor(t, "the refresh to finish", func() bool { return c.Stats().Refreshes == 1 })

			want, wantOK := 0, false
			if mutate == "set" {
				want, wantOK = 100, true
			}
			if v, ok := c.Get("k"); v != want || ok != wantOK {
				t.Fatalf("got %d, %v; want %d, %v", v, ok, want, wantOK)
			}
		})
	}
}

// TestExpiredKeyJoinsTheRunningRefresh: a refresh slower than what was left of
// the TTL must not be followed by a second load of the same key, and a caller
// who joins it and gives up must not cancel it.
func TestExpiredKeyJoinsTheRunningRefresh(t *testing.T) {
	l := newVersionLoader()
	c, advance := newRefreshCache(t, LRU, l.load)

	_, _ = c.GetOrLoad(context.Background(), "k")
	advance(refreshAfter + time.Second)
	_, _ = c.Get("k")
	waitFor(t, "the refresh to start", func() bool { return l.calls.Load() == 2 })
	advance(refreshTTL)

	impatient, cancel := context.WithCancel(context.Background())
	gaveUp := make(chan error, 1)
	go func() {
		_, err := c.GetOrLoad(impatient, "k")
		gaveUp <- err
	}()
	waitFor(t, "the caller to join the refresh", func() bool { return c.Stats().Coalesced == 1 })
	cancel()
	if err := <-gaveUp; !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}

	got := make(chan int, 1)
	go func() {
		v, _ := c.GetOrLoad(context.Background(), "k")
		got <- v
	}()
	waitFor(t, "a second caller to join the refresh", func() bool { return c.Stats().Coalesced == 2 })
	close(l.gate)
	if v := <-got; v != 2 {
		t.Fatalf("the joined caller got %d, want the refreshed 2", v)
	}
	if n := l.calls.Load(); n != 2 {
		t.Fatalf("loader called %d times, want 2", n)
	}
}

func TestRefreshNotFound(t *testing.T) {
	for _, negativeTTL := range []time.Duration{time.Minute, 0} {
		t.Run(fmt.Sprint(negativeTTL), func(t *testing.T) {
			var calls atomic.Int64
			c := New(Options[string, int]{
				TTL: refreshTTL, NegativeTTL: negativeTTL, RefreshAfter: refreshAfter, DisableCleanup: true,
				Loader: func(context.Context, string) (int, error) {
					if calls.Add(1) == 1 {
						return 1, nil
					}

					return 0, ErrNotFound
				},
			})
			defer c.Close()
			advance := manualClock(c)

			_, _ = c.GetOrLoad(context.Background(), "k")
			advance(refreshAfter + time.Second)
			_, _ = c.Get("k")
			waitFor(t, "the refresh to finish", func() bool { return c.Stats().Refreshes == 1 })

			want, wantStatus := 1, StatusHit
			if negativeTTL > 0 {
				want, wantStatus = 0, StatusNegative
			}
			if v, st := c.Lookup("k"); v != want || st != wantStatus {
				t.Fatalf("got %d, %v; want %d, %v", v, st, want, wantStatus)
			}
			if st := c.Stats(); st.RefreshErrors != 0 {
				t.Fatalf("ErrNotFound counted as a refresh error: %+v", st)
			}
		})
	}
}

// TestRefreshNeedsSomethingToLoadWith: a read with no loader leaves a due value
// for one that has one, rather than claiming a refresh it cannot run.
func TestRefreshNeedsSomethingToLoadWith(t *testing.T) {
	c := New(Options[string, int]{TTL: refreshTTL, RefreshAfter: refreshAfter, DisableCleanup: true})
	defer c.Close()
	advance := manualClock(c)

	var calls atomic.Int64
	load := func(context.Context) (int, error) { return int(calls.Add(1)), nil }
	ctx := context.Background()

	_, _ = c.GetOrLoadFunc(ctx, "k", load)
	_ = c.Set("plain", 7)
	advance(refreshAfter + time.Second)

	if v, ok := c.Get("k"); v != 1 || !ok || flying(c, "k") {
		t.Fatal("Get without Options.Loader started a refresh")
	}
	if v, _ := c.GetOrLoadFunc(ctx, "k", load); v != 1 {
		t.Fatalf("got %d", v)
	}
	waitFor(t, "GetOrLoadFunc to refresh with its own function", func() bool { v, _ := c.Get("k"); return v == 2 })

	if v, ok := c.Get("plain"); v != 7 || !ok || flying(c, "plain") {
		t.Fatal("a value written with Set was refreshed without a loader")
	}
}

func TestGetRefreshesWithTheOptionsLoader(t *testing.T) {
	l := newVersionLoader()
	close(l.gate)
	c, advance := newRefreshCache(t, LRU, l.load)

	_ = c.Set("k", 0)
	advance(refreshAfter + time.Second)
	if v, ok := c.Get("k"); v != 0 || !ok {
		t.Fatalf("got %d, %v", v, ok)
	}
	waitFor(t, "Get to refresh with Options.Loader", func() bool { v, _ := c.Get("k"); return v == 1 })
}

// TestSetTTLShorterThanRefreshAfterIsNotRefreshed: a value whose own TTL leaves
// no room for a refresh before it is gone is not refreshed.
func TestSetTTLShorterThanRefreshAfterIsNotRefreshed(t *testing.T) {
	l := newVersionLoader()
	c, advance := newRefreshCache(t, LRU, l.load)

	_ = c.SetTTL("short", 0, refreshAfter)
	_ = c.SetTTL("forever", 0, 0)
	advance(refreshAfter - time.Second)
	_, _ = c.Get("short")
	advance(time.Hour)
	_, _ = c.Get("forever")
	if flying(c, "short") || flying(c, "forever") || l.calls.Load() != 0 {
		t.Fatal("refreshed a value whose TTL has no room for it")
	}
}

func TestGetManyOrLoadRefreshesDueKeysInOneBatch(t *testing.T) {
	rec := newBatchRecorder(map[string]int{"a": 1, "b": 2, "c": 3, "d": 4}, false)
	c := New(Options[string, int]{
		TTL: refreshTTL, RefreshAfter: refreshAfter, BatchLoader: rec.load, DisableCleanup: true,
	})
	defer c.Close()
	advance := manualClock(c)
	ctx := context.Background()

	if _, err := c.GetManyOrLoad(ctx, []string{"a", "b", "c"}); err != nil {
		t.Fatal(err)
	}
	advance(refreshAfter + time.Second)

	got, err := c.GetManyOrLoad(ctx, []string{"a", "b", "c", "d", "a"})
	if err != nil || len(got) != 4 {
		t.Fatalf("got %v, %v", got, err)
	}
	waitFor(t, "the refresh batch", func() bool { return c.Stats().Refreshes == 3 })

	rec.mu.Lock()
	var batches []string
	for _, b := range rec.batches {
		batches = append(batches, strings.Join(sorted(b), ","))
	}
	rec.mu.Unlock()
	if want := []string{"a,b,c", "a,b,c", "d"}; !slices.Equal(sorted(batches), want) {
		t.Fatalf("batches %v, want %v", batches, want)
	}
	if st := c.Stats(); st.Batches != 3 || st.Loads != 4 {
		t.Fatalf("stats: %+v", st)
	}
}

// TestViewRefresh: a view refreshes its own values with its own loader, and the
// cache underneath neither refreshes them with its loader nor takes the refresh
// away from the view.
func TestViewRefresh(t *testing.T) {
	var parentCalls atomic.Int64
	c := New(Options[string, any]{
		TTL: time.Hour, RefreshAfter: 30 * time.Minute, DisableCleanup: true,
		Loader: func(context.Context, string) (any, error) { parentCalls.Add(1); return "parent", nil },
	})
	defer c.Close()
	advance := manualClock(c)

	var viewCalls atomic.Int64
	v := NewView(c, ViewOptions[int]{
		Name: "n", TTL: refreshTTL, RefreshAfter: refreshAfter,
		Loader: func(context.Context, string) (int, error) { return int(viewCalls.Add(1)), nil },
	})
	plain := NewView(c, ViewOptions[int]{Name: "plain"})

	_, _ = v.GetOrLoad(context.Background(), "k")
	_ = plain.Set("k", 5)
	advance(refreshAfter + time.Second)

	if _, ok := c.Get("n:k"); !ok || flying(c, "n:k") || parentCalls.Load() != 0 {
		t.Fatal("the cache refreshed a view's value with its own loader")
	}
	if got, _ := v.Get("k"); got != 1 {
		t.Fatalf("got %d", got)
	}
	waitFor(t, "the view's refresh", func() bool { got, _ := v.Get("k"); return got == 2 })
	if st := v.Stats(); st.Refreshes != 1 || st.Loads != 1 {
		t.Fatalf("view stats: %+v", st)
	}
	if st := c.Stats(); st.Refreshes != 1 {
		t.Fatalf("cache stats: %+v", st)
	}

	advance(refreshAfter + time.Second)
	if got, _ := plain.Get("k"); got != 5 || parentCalls.Load() != 0 {
		t.Fatal("a view without RefreshAfter was refreshed")
	}
}

func TestRefreshAfterValidation(t *testing.T) {
	for name, o := range map[string]Options[string, int]{
		"negative":           {TTL: time.Minute, RefreshAfter: -1},
		"without TTL":        {RefreshAfter: time.Minute},
		"equal to TTL":       {TTL: time.Minute, RefreshAfter: time.Minute},
		"longer than TTL":    {TTL: time.Minute, RefreshAfter: time.Hour},
		"negative, with TTL": {TTL: time.Minute, RefreshAfter: -time.Second},
	} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("New accepted it")
				}
			}()
			New(o)
		})
	}

	c := New(Options[string, any]{TTL: time.Hour, DisableCleanup: true})
	defer c.Close()
	for name, o := range map[string]ViewOptions[int]{
		"negative":                 {Name: "a", RefreshAfter: -1},
		"not shorter than own TTL": {Name: "b", TTL: time.Minute, RefreshAfter: time.Minute},
		"not shorter than cache's": {Name: "c", RefreshAfter: time.Hour},
	} {
		t.Run("view "+name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("NewView accepted it")
				}
			}()
			NewView(c, o)
		})
	}

	// The view's TTL may come from the cache; RefreshAfter is checked against it.
	NewView(c, ViewOptions[int]{Name: "inherits", RefreshAfter: time.Minute})
	// Without a cache nothing is stored, so there is nothing to refresh either.
	NewView(nil, ViewOptions[int]{Name: "off", RefreshAfter: time.Minute})
}

// TestRefreshStaysInsideTheJitteredLife: with jitter, the refresh moves with the
// expiry it belongs to and never lands at or past it.
func TestRefreshStaysInsideTheJitteredLife(t *testing.T) {
	c := New(Options[string, int]{TTL: refreshTTL, RefreshAfter: refreshAfter, Jitter: 100, DisableCleanup: true})
	defer c.Close()
	manualClock(c)
	now := c.core.now()

	for range 10_000 {
		expiresAt, refreshAt := c.core.deadlines(refreshTTL, refreshAfter)
		if refreshAt <= now || refreshAt >= expiresAt {
			t.Fatalf("refresh at %d outside (%d, %d)", refreshAt-now, 0, expiresAt-now)
		}
		// Half way through the TTL, half way through the jittered life.
		if life, at := expiresAt-now, refreshAt-now; at < life/2-1 || at > life/2+1 {
			t.Fatalf("refresh at %d of a life of %d", at, life)
		}
	}
}

func TestGetManyOrLoadRefreshesWithLoaderKeyByKey(t *testing.T) {
	var calls atomic.Int64
	c := New(Options[string, int]{
		TTL: refreshTTL, RefreshAfter: refreshAfter, DisableCleanup: true,
		Loader: func(context.Context, string) (int, error) { return int(calls.Add(1)), nil },
	})
	defer c.Close()
	advance := manualClock(c)
	ctx := context.Background()

	_, _ = c.GetManyOrLoad(ctx, []string{"a", "b"})
	advance(refreshAfter + time.Second)
	_, _ = c.GetManyOrLoad(ctx, []string{"a", "b"})
	waitFor(t, "both refreshes", func() bool { return c.Stats().Refreshes == 2 })
	if st := c.Stats(); st.Batches != 0 || calls.Load() != 4 {
		t.Fatalf("stats %+v after %d loader calls", st, calls.Load())
	}
}

// TestRefreshLeavesARunningLoadToPublish: a load of the key that is already
// running will publish a newer value than a refresh started now could, so the
// refresh is not started. A test has to set that state up by hand: it only
// arises when a value comes due between a load's publication and its exit.
func TestRefreshLeavesARunningLoadToPublish(t *testing.T) {
	never := func(context.Context, string) (int, error) {
		t.Error("a refresh started next to a running load")

		return 0, nil
	}
	running := func(g *flightGroup[string, int], key string) *call[int] {
		cl := &call[int]{done: make(chan struct{}), token: &loadToken{}}
		cl.token.valid.Store(true)
		g.calls[key] = cl

		return cl
	}

	c := New(Options[string, int]{TTL: refreshTTL, RefreshAfter: refreshAfter, Loader: never, DisableCleanup: true})
	defer c.Close()
	g := c.core.flights[c.core.shardIndex("k")]
	cl := running(g, "k")
	c.core.refresh(context.Background(), "k", unsized(never))
	if g.calls["k"] != cl {
		t.Fatal("refresh replaced the running load")
	}

	b := New(Options[string, int]{
		TTL: refreshTTL, RefreshAfter: refreshAfter, DisableCleanup: true,
		BatchLoader: func(context.Context, []string) (map[string]int, error) {
			t.Error("a batch refresh started for keys that are all loading")

			return nil, nil
		},
	})
	defer b.Close()
	running(b.core.flights[b.core.shardIndex("x")], "x")
	running(b.core.flights[b.core.shardIndex("y")], "y")
	b.core.refreshMany(context.Background(), []string{"x", "y"})

	vc := New(Options[string, any]{TTL: refreshTTL, DisableCleanup: true})
	defer vc.Close()
	v := NewView(vc, ViewOptions[int]{Name: "v", RefreshAfter: refreshAfter, Loader: never})
	vg := v.flights[vc.core.shardIndex("v:k")]
	vcl := running(vg, "k")
	v.refresh(context.Background(), "k", unsized(never))
	if vg.calls["k"] != vcl {
		t.Fatal("a view refresh replaced the running load")
	}

	// Give a wrongly started refresh the chance to call never.
	time.Sleep(10 * time.Millisecond)
}

func TestViewRefreshFailureIsCountedForTheView(t *testing.T) {
	c := New(Options[string, any]{TTL: refreshTTL, DisableCleanup: true})
	defer c.Close()
	advance := manualClock(c)

	var calls atomic.Int64
	v := NewView(c, ViewOptions[int]{
		Name: "v", RefreshAfter: refreshAfter,
		Loader: func(context.Context, string) (int, error) {
			if calls.Add(1) == 1 {
				return 1, nil
			}

			return 0, errUpstream
		},
	})

	_, _ = v.GetOrLoad(context.Background(), "k")
	advance(refreshAfter + time.Second)
	_, _ = v.GetOrLoad(context.Background(), "k")
	waitFor(t, "the view's refresh to fail", func() bool { return v.Stats().RefreshErrors == 1 })
	if got, ok := v.Get("k"); got != 1 || !ok {
		t.Fatalf("got %d, %v", got, ok)
	}
	if st := c.Stats(); st.RefreshErrors != 1 {
		t.Fatalf("cache stats: %+v", st)
	}
}
