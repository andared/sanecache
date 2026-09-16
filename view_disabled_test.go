package sanecache

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// A view on a nil cache is how an application switches caching off without
// switching code paths: the loader runs every time and nothing is remembered.
func TestDisabledViewLoadsEveryTime(t *testing.T) {
	calls := 0
	v := NewView(nil, ViewOptions[string]{Name: "text", Loader: func(_ context.Context, key string) (string, error) {
		calls++
		return fmt.Sprintf("%s#%d", key, calls), nil
	}})

	for want := 1; want <= 2; want++ {
		got, err := v.GetOrLoad(context.Background(), "k")
		if err != nil || got != fmt.Sprintf("k#%d", want) {
			t.Fatalf("GetOrLoad = %q, %v; want k#%d", got, err, want)
		}
	}
	if st := v.Stats(); st != (ViewStats{Misses: 2, Loads: 2}) {
		t.Fatalf("Stats = %+v; want 2 misses, 2 loads", st)
	}
}

// Switching the cache off must not switch off the protection of the upstream:
// callers who arrive while a load runs still share it.
func TestDisabledViewCoalesces(t *testing.T) {
	release := make(chan struct{})
	load, calls := blockingLoader(release, 42)
	v := NewView(nil, ViewOptions[int]{Name: "numbers", Loader: load})

	const n = 8
	var wg sync.WaitGroup
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if got, err := v.GetOrLoad(context.Background(), "k"); got != 42 || err != nil {
				t.Errorf("GetOrLoad = %v, %v", got, err)
			}
		}()
	}
	waitFor(t, "waiters", func() bool { return v.Stats().Coalesced == n-1 })
	close(release)
	wg.Wait()

	if calls.Load() != 1 {
		t.Fatalf("loads = %d; want 1", calls.Load())
	}

	// The flight is over and nothing was kept, so the next caller loads again.
	if _, err := v.GetOrLoad(context.Background(), "k"); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatalf("loads after the flight = %d; want 2", calls.Load())
	}
}

func TestDisabledViewDoesNotRememberNotFound(t *testing.T) {
	calls := 0
	v := NewView(nil, ViewOptions[int]{Name: "missing", NegativeTTL: time.Hour, Loader: func(context.Context, string) (int, error) {
		calls++
		return 0, fmt.Errorf("no row: %w", ErrNotFound)
	}})

	for range 2 {
		_, err := v.GetOrLoad(context.Background(), "k")
		if !errors.Is(err, ErrNotFound) || !strings.Contains(err.Error(), "no row") {
			t.Fatalf("GetOrLoad err = %v; want the loader's own ErrNotFound", err)
		}
	}
	if calls != 2 {
		t.Fatalf("loads = %d; want 2", calls)
	}
}

func TestDisabledViewRefusesWrites(t *testing.T) {
	v := NewView(nil, ViewOptions[int]{Name: "numbers", TTL: time.Hour, NegativeTTL: time.Hour})

	for name, write := range map[string]func() error{
		"Set":            func() error { return v.Set("k", 1) },
		"SetTTL":         func() error { return v.SetTTL("k", 1, time.Hour) },
		"SetNegative":    func() error { return v.SetNegative("k") },
		"SetNegativeTTL": func() error { return v.SetNegativeTTL("k", time.Hour) },
	} {
		if err := write(); !errors.Is(err, ErrDisabled) {
			t.Errorf("%s err = %v; want ErrDisabled", name, err)
		}
	}

	if _, st := v.Lookup("k"); st != StatusMiss {
		t.Fatalf("Lookup = %v; want miss", st)
	}
	if _, ok := v.Get("k"); ok {
		t.Fatal("Get found a value in a view that stores nothing")
	}
	if v.Delete("k") {
		t.Fatal("Delete reported a present key")
	}
	if v.Name() != "numbers" {
		t.Fatalf("Name = %q", v.Name())
	}
}

func TestDisabledViewWithoutLoader(t *testing.T) {
	v := NewView(nil, ViewOptions[int]{Name: "numbers"})
	if _, err := v.GetOrLoad(context.Background(), "k"); !errors.Is(err, ErrNoLoader) {
		t.Fatalf("GetOrLoad err = %v; want ErrNoLoader", err)
	}
}

func TestDisabledViewCancellationAndPanic(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	load, _ := blockingLoader(release, 1)
	v := NewView(nil, ViewOptions[int]{Name: "slow", Loader: load})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := v.GetOrLoad(ctx, "k"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled caller err = %v", err)
	}

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := v.GetOrLoad(ctx, "k"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timed-out caller err = %v", err)
	}

	boom := NewView(nil, ViewOptions[int]{Name: "boom", Loader: func(context.Context, string) (int, error) {
		panic("disabled boom")
	}})
	defer func() {
		p, ok := recover().(*loaderPanic)
		if !ok || !strings.Contains(p.Error(), "disabled boom") {
			t.Fatalf("panic lost: %v", p)
		}
		if st := boom.Stats(); st.Loads != 1 || st.LoadErrors != 1 {
			t.Fatalf("Stats = %+v; want the panic counted as a failed load", st)
		}
	}()
	_, _ = boom.GetOrLoad(context.Background(), "k")
}
