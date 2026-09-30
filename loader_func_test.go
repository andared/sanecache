package sanecache

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// mustNotLoad is a load function for callers that are expected to be answered
// without running their own.
func mustNotLoad[V any](t *testing.T) func(context.Context) (V, error) {
	return func(context.Context) (V, error) {
		t.Error("the caller's own load ran; want the answer from the cache or from a load already running")

		var zero V

		return zero, nil
	}
}

func TestGetOrLoadFuncRunsOneLoadPerKey(t *testing.T) {
	release := make(chan struct{})
	var calls atomic.Int64

	// No Options.Loader: the function is all the cache needs.
	c := New(Options[string, int]{TTL: time.Minute})
	defer c.Close()

	const callers = 8
	var wg sync.WaitGroup
	values := make([]int, callers)
	errs := make([]error, callers)
	for i := range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			values[i], errs[i] = c.GetOrLoadFunc(context.Background(), "k", func(context.Context) (int, error) {
				calls.Add(1)
				<-release

				return 42, nil
			})
		}()
	}

	waitFor(t, "every caller to join the load", func() bool {
		return c.Stats().Coalesced == callers-1
	})
	close(release)
	wg.Wait()

	if got := calls.Load(); got != 1 {
		t.Fatalf("load functions ran %d times; want 1", got)
	}
	for i := range callers {
		if errs[i] != nil || values[i] != 42 {
			t.Fatalf("caller %d got %v, %v; want 42, nil", i, values[i], errs[i])
		}
	}
	if v, err := c.GetOrLoadFunc(context.Background(), "k", mustNotLoad[int](t)); err != nil || v != 42 {
		t.Fatalf("GetOrLoadFunc after the load = %v, %v; want the cached 42", v, err)
	}
	if st := c.Stats(); st.Loads != 1 || st.LoadErrors != 0 {
		t.Fatalf("loads/errors = %d/%d; want 1/0", st.Loads, st.LoadErrors)
	}
}

// TestGetOrLoadFuncJoinsOtherLoads: a function and Options.Loader for the same
// key are two ways to fetch one value, so they must not both run.
func TestGetOrLoadFuncJoinsOtherLoads(t *testing.T) {
	release := make(chan struct{})
	load, calls := blockingLoader(release, 42)
	c := New(Options[string, int]{TTL: time.Minute, Loader: load})
	defer c.Close()

	loaded := make(chan error, 1)
	go func() {
		_, err := c.GetOrLoad(context.Background(), "k")
		loaded <- err
	}()
	waitFor(t, "the loader to start", func() bool { return calls.Load() == 1 })

	joined := make(chan int, 1)
	go func() {
		v, err := c.GetOrLoadFunc(context.Background(), "k", mustNotLoad[int](t))
		if err != nil {
			t.Errorf("GetOrLoadFunc: %v", err)
		}
		joined <- v
	}()
	waitFor(t, "GetOrLoadFunc to join the load", func() bool { return c.Stats().Coalesced == 1 })
	close(release)

	if err := <-loaded; err != nil {
		t.Fatalf("GetOrLoad: %v", err)
	}
	if v := <-joined; v != 42 {
		t.Fatalf("GetOrLoadFunc = %d; want the loader's 42", v)
	}
}

func TestGetManyOrLoadWaitsForAFunctionLoad(t *testing.T) {
	var batches [][]string
	var mu sync.Mutex
	c := New(Options[string, int]{
		TTL: time.Minute,
		BatchLoader: func(_ context.Context, keys []string) (map[string]int, error) {
			mu.Lock()
			batches = append(batches, slices.Clone(keys))
			mu.Unlock()

			return map[string]int{"a": -1, "b": 2}, nil
		},
	})
	defer c.Close()

	release, started := make(chan struct{}), make(chan struct{})
	loaded := make(chan error, 1)
	go func() {
		_, err := c.GetOrLoadFunc(context.Background(), "a", func(context.Context) (int, error) {
			close(started)
			<-release

			return 1, nil
		})
		loaded <- err
	}()
	<-started

	many := make(chan map[string]int, 1)
	go func() {
		got, err := c.GetManyOrLoad(context.Background(), []string{"a", "b"})
		if err != nil {
			t.Errorf("GetManyOrLoad: %v", err)
		}
		many <- got
	}()
	waitFor(t, "the batch to join the load of a", func() bool { return c.Stats().Coalesced == 1 })
	close(release)

	if err := <-loaded; err != nil {
		t.Fatalf("GetOrLoadFunc: %v", err)
	}
	if got := <-many; !maps.Equal(got, map[string]int{"a": 1, "b": 2}) {
		t.Fatalf("GetManyOrLoad = %v; want a from the function and b from the batch", got)
	}
	if !slices.EqualFunc(batches, [][]string{{"b"}}, slices.Equal[[]string]) {
		t.Fatalf("batches = %v; want one batch with b only", batches)
	}
}

func TestGetOrLoadFuncKeepsGetOrLoadPolicies(t *testing.T) {
	c := New(Options[string, string]{
		TTL:         time.Minute,
		NegativeTTL: time.Minute,
		// Room for a negative entry, which is charged about 130 bytes, and not
		// for the large value below.
		MaxBytes: 256,
		Cost:     func(s string) int64 { return int64(len(s)) },
	})
	defer c.Close()
	ctx := context.Background()

	_, err := c.GetOrLoadFunc(ctx, "gone", func(context.Context) (string, error) {
		return "", fmt.Errorf("article gone: %w", ErrNotFound)
	})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v; want ErrNotFound", err)
	}
	if _, err := c.GetOrLoadFunc(ctx, "gone", mustNotLoad[string](t)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second err = %v; want ErrNotFound from the negative entry", err)
	}

	var failures int
	for range 2 {
		_, err := c.GetOrLoadFunc(ctx, "flaky", func(context.Context) (string, error) {
			failures++

			return "", errUpstream
		})
		if !errors.Is(err, errUpstream) {
			t.Fatalf("err = %v; want the function's own error", err)
		}
	}
	if failures != 2 {
		t.Fatalf("failing function ran %d times; want 2, errors are not cached", failures)
	}

	large := strings.Repeat("x", 300)
	v, err := c.GetOrLoadFunc(ctx, "large", func(context.Context) (string, error) { return large, nil })
	if err != nil || v != large {
		t.Fatalf("oversized = %q, %v; want it returned", v, err)
	}
	if _, ok := c.Get("large"); ok {
		t.Fatal("a value over the budget was cached")
	}

	st := c.Stats()
	if st.Loads != 4 || st.LoadNotFound != 1 || st.LoadErrors != 2 || st.Rejections != 1 {
		t.Fatalf("loads/notFound/errors/rejections = %d/%d/%d/%d; want 4/1/2/1",
			st.Loads, st.LoadNotFound, st.LoadErrors, st.Rejections)
	}
}

func TestGetOrLoadFuncNeedsAFunction(t *testing.T) {
	c := New(Options[string, int]{TTL: time.Minute})
	defer c.Close()
	if err := c.Set("k", 1); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if _, err := c.GetOrLoadFunc(context.Background(), "k", nil); !errors.Is(err, ErrNoLoader) {
		t.Fatalf("err = %v; want ErrNoLoader, even for a cached key", err)
	}

	v := NewView(New(Options[string, any]{TTL: time.Minute}), ViewOptions[int]{Name: "v"})
	if _, err := v.GetOrLoadFunc(context.Background(), "k", nil); !errors.Is(err, ErrNoLoader) {
		t.Fatalf("view err = %v; want ErrNoLoader", err)
	}
}

func TestViewGetOrLoadFunc(t *testing.T) {
	for _, disabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("disabled=%v", disabled), func(t *testing.T) {
			var c *Cache[string, any]
			if !disabled {
				c = New(Options[string, any]{TTL: time.Minute, MaxBytes: 1 << 10, Cost: func(any) int64 { return 1 }})
				defer c.Close()
			}
			// No view loader, and a cost of the view's own.
			v := NewView(c, ViewOptions[string]{Name: "text", Cost: func(s string) int64 { return int64(len(s)) }})

			var calls int
			load := func(context.Context) (string, error) {
				calls++

				return "hello", nil
			}
			for range 2 {
				if got, err := v.GetOrLoadFunc(context.Background(), "k", load); got != "hello" || err != nil {
					t.Fatalf("GetOrLoadFunc = %q, %v", got, err)
				}
			}

			if disabled {
				if calls != 2 {
					t.Fatalf("function ran %d times; want 2, a disabled view keeps nothing", calls)
				}

				return
			}
			if calls != 1 {
				t.Fatalf("function ran %d times; want 1, the second call is a hit", calls)
			}
			if raw, ok := c.Get("text:k"); !ok || raw != "hello" {
				t.Fatalf("stored under the view's namespace: %v, %v", raw, ok)
			}
			if c.Bytes() != int64(len("hello")) {
				t.Fatalf("bytes = %d; want the view's cost", c.Bytes())
			}
			if vs := v.Stats(); vs.Loads != 1 || vs.Hits != 1 || vs.Misses != 1 {
				t.Fatalf("view stats = %+v; want the load and the hit on the view", vs)
			}
		})
	}
}

// TestViewGetOrLoadFuncJoinsTheViewLoader is TestGetOrLoadFuncJoinsOtherLoads for a view.
func TestViewGetOrLoadFuncJoinsTheViewLoader(t *testing.T) {
	release := make(chan struct{})
	var calls atomic.Int64
	c := New(Options[string, any]{TTL: time.Minute})
	defer c.Close()
	v := NewView(c, ViewOptions[int]{Name: "n", Loader: func(context.Context, string) (int, error) {
		calls.Add(1)
		<-release

		return 42, nil
	}})

	loaded := make(chan error, 1)
	go func() {
		_, err := v.GetOrLoad(context.Background(), "k")
		loaded <- err
	}()
	waitFor(t, "the view loader to start", func() bool { return calls.Load() == 1 })

	joined := make(chan int, 1)
	go func() {
		got, err := v.GetOrLoadFunc(context.Background(), "k", mustNotLoad[int](t))
		if err != nil {
			t.Errorf("GetOrLoadFunc: %v", err)
		}
		joined <- got
	}()
	waitFor(t, "GetOrLoadFunc to join the load", func() bool { return v.Stats().Coalesced == 1 })
	close(release)

	if err := <-loaded; err != nil {
		t.Fatalf("GetOrLoad: %v", err)
	}
	if got := <-joined; got != 42 {
		t.Fatalf("GetOrLoadFunc = %d; want the view loader's 42", got)
	}
}
