package sanecache

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var errUpstream = errors.New("upstream is down")

// blockingLoader returns a loader that parks until release is closed, along with
// a counter of how many times it was entered.
func blockingLoader(release <-chan struct{}, value int) (func(context.Context, string) (int, error), *atomic.Int64) {
	var calls atomic.Int64

	return func(ctx context.Context, _ string) (int, error) {
		calls.Add(1)
		select {
		case <-release:
			return value, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}, &calls
}

// TestGetOrLoadRunsOneLoadPerKey is the reason the loader exists: on a cold key
// every concurrent caller used to go to the upstream on its own.
func TestGetOrLoadRunsOneLoadPerKey(t *testing.T) {
	release := make(chan struct{})
	load, calls := blockingLoader(release, 42)

	c := New(Options[string, int]{TTL: time.Minute, Loader: load})
	defer c.Close()

	const callers = 8
	var wg sync.WaitGroup
	values := make([]int, callers)
	errs := make([]error, callers)
	for i := range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			values[i], errs[i] = c.GetOrLoad(context.Background(), "k")
		}()
	}

	waitFor(t, "every caller to join the load", func() bool {
		return c.Stats().Coalesced == callers-1
	})
	close(release)
	wg.Wait()

	if got := calls.Load(); got != 1 {
		t.Fatalf("loader ran %d times; want 1", got)
	}
	for i := range callers {
		if errs[i] != nil || values[i] != 42 {
			t.Fatalf("caller %d got %v, %v; want 42, nil", i, values[i], errs[i])
		}
	}

	st := c.Stats()
	if st.Loads != 1 || st.LoadErrors != 0 || st.Coalesced != callers-1 {
		t.Fatalf("loads/errors/coalesced = %d/%d/%d; want 1/0/%d",
			st.Loads, st.LoadErrors, st.Coalesced, callers-1)
	}
	// The next caller finds it cached rather than starting a second load.
	if _, err := c.GetOrLoad(context.Background(), "k"); err != nil {
		t.Fatalf("GetOrLoad after the load: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("loader ran %d times after the value was cached; want 1", got)
	}
}

// TestGetOrLoadPublishesBeforeItReturns keeps the library's central promise on
// the load path too: what a caller has in hand is readable from the cache.
func TestGetOrLoadPublishesBeforeItReturns(t *testing.T) {
	c := New(Options[string, int]{
		TTL:    time.Minute,
		Loader: func(context.Context, string) (int, error) { return 5, nil },
	})
	defer c.Close()

	if _, err := c.GetOrLoad(context.Background(), "k"); err != nil {
		t.Fatalf("GetOrLoad: %v", err)
	}
	// Not a formality: if the value were published after the waiters were let
	// go, a caller arriving a moment later would start a second load for a value
	// that had already been fetched.
	if v, ok := c.Get("k"); !ok || v != 5 {
		t.Fatalf("Get = %v, %v; want 5, true", v, ok)
	}
}

// TestLoadRechecksTheCacheBeforeStartingOver covers the window between a
// caller's lookup and its place in the queue for the key: another caller's load
// can finish in there, and without a second look this one would fetch again what
// was just fetched. core.load is entered directly because that is precisely the
// state to reproduce — a caller whose own lookup missed a moment ago.
func TestLoadRechecksTheCacheBeforeStartingOver(t *testing.T) {
	var calls atomic.Int64
	c := New(Options[string, int]{
		TTL:         time.Minute,
		NegativeTTL: time.Minute,
		Loader: func(context.Context, string) (int, error) {
			calls.Add(1)

			return 1, nil
		},
	})
	defer c.Close()

	if err := c.Set("k", 7); err != nil {
		t.Fatalf("Set: %v", err)
	}
	v, err := c.core.load(context.Background(), "k")
	if err != nil || v != 7 {
		t.Fatalf("load = %v, %v; want 7, nil", v, err)
	}

	// The same for an answer that arrived as a negative entry.
	if err := c.SetNegative("gone"); err != nil {
		t.Fatalf("SetNegative: %v", err)
	}
	if _, err := c.core.load(context.Background(), "gone"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v; want ErrNotFound", err)
	}

	if got := calls.Load(); got != 0 {
		t.Fatalf("loader ran %d times for keys that had been cached in the meantime", got)
	}
	if got := c.Stats().Coalesced; got != 2 {
		t.Fatalf("Coalesced = %d; want 2", got)
	}
}

func TestGetOrLoadCachesNotFound(t *testing.T) {
	var calls atomic.Int64
	c := New(Options[string, int]{
		TTL:         time.Minute,
		NegativeTTL: time.Minute,
		Loader: func(_ context.Context, key string) (int, error) {
			calls.Add(1)

			return 0, fmt.Errorf("article %s: %w", key, ErrNotFound)
		},
	})
	defer c.Close()

	// The caller that triggered the load gets the loader's own error, wrapping
	// and all.
	_, err := c.GetOrLoad(context.Background(), "gone")
	if !errors.Is(err, ErrNotFound) || !strings.Contains(err.Error(), "article gone") {
		t.Fatalf("err = %v; want the loader's error wrapping ErrNotFound", err)
	}

	// Afterwards the answer comes from the negative entry, and reads the same
	// way: handling of "no such object" does not depend on cache state.
	if _, err := c.GetOrLoad(context.Background(), "gone"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v; want ErrNotFound", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("loader ran %d times; want 1, the second answer was cached", got)
	}
	if st := c.Stats(); st.Negatives != 1 || st.LoadErrors != 1 {
		t.Fatalf("negatives/loadErrors = %d/%d; want 1/1", st.Negatives, st.LoadErrors)
	}
}

func TestGetOrLoadWithoutNegativeTTLKeepsAsking(t *testing.T) {
	var calls atomic.Int64
	c := New(Options[string, int]{
		TTL: time.Minute,
		Loader: func(context.Context, string) (int, error) {
			calls.Add(1)

			return 0, ErrNotFound
		},
	})
	defer c.Close()

	for range 2 {
		if _, err := c.GetOrLoad(context.Background(), "gone"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("err = %v; want ErrNotFound", err)
		}
	}
	// Nothing was remembered, because remembering it was never enabled.
	if got := calls.Load(); got != 2 {
		t.Fatalf("loader ran %d times; want 2", got)
	}
	if c.Len() != 0 {
		t.Fatalf("Len = %d; want 0", c.Len())
	}
}

func TestGetOrLoadDoesNotCacheOtherErrors(t *testing.T) {
	var calls atomic.Int64
	c := New(Options[string, int]{
		TTL:         time.Minute,
		NegativeTTL: time.Minute,
		Loader: func(context.Context, string) (int, error) {
			calls.Add(1)

			return 0, errUpstream
		},
	})
	defer c.Close()

	for range 3 {
		if _, err := c.GetOrLoad(context.Background(), "k"); !errors.Is(err, errUpstream) {
			t.Fatalf("err = %v; want errUpstream", err)
		}
	}
	// A failure is not an answer: caching it would turn a blip into an outage
	// that outlives it.
	if got := calls.Load(); got != 3 {
		t.Fatalf("loader ran %d times; want 3", got)
	}
	if st := c.Stats(); st.Loads != 3 || st.LoadErrors != 3 {
		t.Fatalf("loads/errors = %d/%d; want 3/3", st.Loads, st.LoadErrors)
	}
}

func TestGetOrLoadReturnsAValueTooLargeToCache(t *testing.T) {
	c := New(Options[string, []byte]{
		MaxBytes: 16,
		Cost:     func(b []byte) int64 { return int64(len(b)) },
		Loader: func(context.Context, string) ([]byte, error) {
			return make([]byte, 1024), nil
		},
	})
	defer c.Close()

	// The caller asked for the value, not for it to be cached. Withholding it
	// would be inventing a failure the upstream did not report.
	v, err := c.GetOrLoad(context.Background(), "k")
	if err != nil || len(v) != 1024 {
		t.Fatalf("GetOrLoad = %d bytes, %v; want 1024 bytes, nil", len(v), err)
	}
	if _, ok := c.Get("k"); ok {
		t.Fatal("a value over the budget was cached")
	}
	if got := c.Stats().Rejections; got != 1 {
		t.Fatalf("Rejections = %d; want 1", got)
	}
}

func TestGetOrLoadNeedsALoader(t *testing.T) {
	c := New(Options[string, int]{TTL: time.Minute})
	defer c.Close()

	if _, err := c.GetOrLoad(context.Background(), "k"); !errors.Is(err, ErrNoLoader) {
		t.Fatalf("err = %v; want ErrNoLoader", err)
	}
}

func TestGetOrLoadServesCachedAnswersWithoutTheLoader(t *testing.T) {
	var calls atomic.Int64
	c := New(Options[string, int]{
		TTL:         time.Minute,
		NegativeTTL: time.Minute,
		Loader: func(context.Context, string) (int, error) {
			calls.Add(1)

			return 0, errUpstream
		},
	})
	defer c.Close()

	if err := c.Set("hit", 1); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := c.SetNegative("gone"); err != nil {
		t.Fatalf("SetNegative: %v", err)
	}

	if v, err := c.GetOrLoad(context.Background(), "hit"); err != nil || v != 1 {
		t.Fatalf("GetOrLoad = %v, %v; want 1, nil", v, err)
	}
	if _, err := c.GetOrLoad(context.Background(), "gone"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v; want ErrNotFound", err)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("loader ran %d times for keys the cache could answer", got)
	}
}

// TestGetOrLoadSurvivesTheCallerThatStartedIt is where x/sync/singleflight would
// hand the second caller a cancelled context: the load belongs to everyone
// waiting on it, not to whoever happened to arrive first.
func TestGetOrLoadSurvivesTheCallerThatStartedIt(t *testing.T) {
	release := make(chan struct{})
	var loaderCtxErr error
	entered := make(chan struct{})

	c := New(Options[string, int]{
		TTL: time.Minute,
		Loader: func(ctx context.Context, _ string) (int, error) {
			close(entered)
			<-release
			loaderCtxErr = ctx.Err()

			return 9, nil
		},
	})
	defer c.Close()

	first, cancelFirst := context.WithCancel(context.Background())
	firstErr := make(chan error, 1)
	go func() {
		_, err := c.GetOrLoad(first, "k")
		firstErr <- err
	}()
	<-entered

	secondValue := make(chan int, 1)
	go func() {
		v, err := c.GetOrLoad(context.Background(), "k")
		if err != nil {
			t.Errorf("second caller: %v", err)
		}
		secondValue <- v
	}()
	waitFor(t, "the second caller to join the load", func() bool {
		return c.Stats().Coalesced == 1
	})

	cancelFirst()
	if err := <-firstErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("first caller err = %v; want context.Canceled", err)
	}

	close(release)
	if v := <-secondValue; v != 9 {
		t.Fatalf("second caller got %d; want 9", v)
	}
	if loaderCtxErr != nil {
		t.Fatalf("the load was cancelled with a caller still waiting: %v", loaderCtxErr)
	}
}

func TestGetOrLoadStopsWhenEveryCallerGivesUp(t *testing.T) {
	entered := make(chan struct{}, 4)
	loaderCtx := make(chan error, 4)
	var calls atomic.Int64

	c := New(Options[string, int]{
		TTL: time.Minute,
		Loader: func(ctx context.Context, _ string) (int, error) {
			calls.Add(1)
			entered <- struct{}{}
			<-ctx.Done()
			loaderCtx <- ctx.Err()

			return 0, ctx.Err()
		},
	})
	defer c.Close()

	// One caller that gives up as soon as the load is under way. The deadline is
	// only a backstop: if the call it is meant to join never starts, the test
	// should fail rather than hang.
	abandon := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		go func() {
			select {
			case <-entered:
				cancel()
			case <-ctx.Done():
			}
		}()
		_, err := c.GetOrLoad(ctx, "k")

		return err
	}

	if err := abandon(); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v; want context.Canceled", err)
	}

	// Nobody is waiting any more, so the load stops rather than running on to
	// fill a cache nobody asked about.
	select {
	case err := <-loaderCtx:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("loader context err = %v; want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the load kept running with no caller left")
	}

	// And the abandoned call is not left in the map for the next caller to join,
	// which would leave them waiting on a result nobody is producing.
	if err := abandon(); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v; want context.Canceled from a load of its own", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("loader ran %d times; want 2", got)
	}
}

func TestGetOrLoadWithAnAlreadyCancelledContext(t *testing.T) {
	var calls atomic.Int64
	c := New(Options[string, int]{
		TTL: time.Minute,
		Loader: func(context.Context, string) (int, error) {
			calls.Add(1)

			return 1, nil
		},
	})
	defer c.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := c.GetOrLoad(ctx, "k"); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v; want context.Canceled", err)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("loader ran %d times for a caller that had already given up", got)
	}

	// A cached answer still costs nothing to hand over.
	if err := c.Set("k", 3); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if v, err := c.GetOrLoad(ctx, "k"); err != nil || v != 3 {
		t.Fatalf("GetOrLoad = %v, %v; want 3, nil", v, err)
	}
}

// TestGetOrLoadCarriesAPanicToTheCaller: the loader runs on a goroutine of the
// cache's making, where an unrecovered panic would take the process down instead
// of the request that caused it.
func TestGetOrLoadCarriesAPanicToTheCaller(t *testing.T) {
	var calls atomic.Int64
	c := New(Options[string, int]{
		TTL: time.Minute,
		Loader: func(context.Context, string) (int, error) {
			if calls.Add(1) == 1 {
				panic("boom")
			}

			return 4, nil
		},
	})
	defer c.Close()

	func() {
		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("a panicking loader did not reach the caller")
			}
			err, ok := r.(error)
			if !ok || !strings.Contains(err.Error(), "boom") {
				t.Fatalf("recovered %v; want an error naming the panic", r)
			}
			// The stack of where it actually happened, not of the recover.
			if !strings.Contains(err.Error(), "sanecache.(*core[...]).run") {
				t.Fatalf("recovered error lost the loader's stack: %v", err)
			}
		}()
		_, _ = c.GetOrLoad(context.Background(), "k")
	}()

	// The failed call does not wedge the key: the next caller loads it.
	if v, err := c.GetOrLoad(context.Background(), "k"); err != nil || v != 4 {
		t.Fatalf("GetOrLoad after a panic = %v, %v; want 4, nil", v, err)
	}
}

func TestGetOrLoadUnderConcurrency(t *testing.T) {
	const keys = 32

	var loads atomic.Int64
	c := New(Options[int, int]{
		TTL:         50 * time.Millisecond,
		NegativeTTL: 10 * time.Millisecond,
		Jitter:      20,
		MaxBytes:    keys / 2,
		Cost:        unitCost[int](),
		Shards:      4,
		Loader: func(ctx context.Context, key int) (int, error) {
			loads.Add(1)
			switch key % 3 {
			case 0:
				return 0, ErrNotFound
			case 1:
				return 0, errUpstream
			default:
				return key, nil
			}
		},
	})
	defer c.Close()

	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 500 {
				key := (g + i) % keys
				ctx := context.Background()
				if i%5 == 0 {
					// Callers that give up mid-load are the interesting case:
					// they must not take the load down with them.
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				}
				v, err := c.GetOrLoad(ctx, key)
				if err == nil && key%3 == 2 && v != key {
					t.Errorf("GetOrLoad(%d) = %d", key, v)

					return
				}
			}
		}()
	}
	wg.Wait()

	if loads.Load() == 0 {
		t.Fatal("no loads happened at all")
	}
	if c.Bytes() < 0 || c.Len() < 0 {
		t.Fatalf("accounting went negative: %d bytes, %d entries", c.Bytes(), c.Len())
	}
}
