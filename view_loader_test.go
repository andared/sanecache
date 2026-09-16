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

func TestViewLoaderCoalesces(t *testing.T) {
	for _, shards := range []int{1, 4} {
		t.Run(fmt.Sprint(shards), func(t *testing.T) {
			c := New(Options[string, any]{Shards: shards})
			defer c.Close()
			release := make(chan struct{})
			load, calls := blockingLoader(release, 42)
			v := NewView(c, ViewOptions[int]{Name: "numbers", Loader: load})
			const n = 8
			var wg sync.WaitGroup
			for range n {
				wg.Add(1)
				go func() {
					defer wg.Done()
					got, err := v.GetOrLoad(context.Background(), "k")
					if got != 42 || err != nil {
						t.Errorf("load = %v, %v", got, err)
					}
				}()
			}
			waitFor(t, "view waiters", func() bool { return v.Stats().Coalesced == n-1 })
			close(release)
			wg.Wait()
			if calls.Load() != 1 {
				t.Fatalf("loads = %d", calls.Load())
			}
			if got, ok := c.Get("numbers:k"); !ok || got != 42 {
				t.Fatalf("published = %v, %v", got, ok)
			}
			if vs, cs := v.Stats(), c.Stats(); vs.Loads != 1 || cs.Loads != 1 || vs.Coalesced != cs.Coalesced {
				t.Fatalf("stats: %+v / %+v", vs, cs)
			}
		})
	}
}

func TestViewLoaderPolicies(t *testing.T) {
	for _, disabled := range []bool{false, true} {
		t.Run(fmt.Sprint(disabled), func(t *testing.T) {
			c := New(Options[string, any]{TTL: time.Hour, NegativeTTL: time.Hour, MaxBytes: 1000, Cost: func(any) int64 { return 1 }, DisableStats: disabled, DisableCleanup: true})
			defer c.Close()
			// Drive expiry explicitly so scheduler delays cannot expire recheck fixtures.
			c.core.coarse = new(atomic.Int64)
			c.core.coarse.Store(time.Now().UnixNano())
			var calls int
			v := NewView(c, ViewOptions[string]{Name: "text", TTL: 100 * time.Millisecond, NegativeTTL: 100 * time.Millisecond, Cost: func(s string) int64 { return int64(len(s)) }, Loader: func(_ context.Context, key string) (string, error) {
				calls++
				switch key {
				case "gone":
					return "", fmt.Errorf("missing: %w", ErrNotFound)
				case "error":
					return "", errUpstream
				case "large":
					return strings.Repeat("x", 1001), nil
				}
				return "hello", nil
			}})
			ctx := context.Background()
			if err := c.Set("text:k", 42); err != nil {
				t.Fatal(err)
			}
			if got, err := v.GetOrLoad(ctx, "k"); got != "hello" || err != nil {
				t.Fatalf("typed load: %q, %v", got, err)
			}
			if c.Bytes() != 5 {
				t.Fatalf("view cost: %d", c.Bytes())
			}
			if got, err := v.load(ctx, "k"); got != "hello" || err != nil || calls != 1 {
				t.Fatalf("recheck: %q, %v, calls %d", got, err, calls)
			}
			if _, err := v.GetOrLoad(ctx, "gone"); !errors.Is(err, ErrNotFound) || !strings.Contains(err.Error(), "missing") {
				t.Fatalf("not found: %v", err)
			}
			if _, err := v.load(ctx, "gone"); err != ErrNotFound {
				t.Fatalf("negative recheck: %v", err)
			}
			if _, err := v.GetOrLoad(ctx, "gone"); err != ErrNotFound {
				t.Fatalf("cached negative: %v", err)
			}
			c.core.coarse.Add(int64(150 * time.Millisecond))
			if _, st := v.Lookup("k"); st != StatusMiss {
				t.Fatal("view TTL ignored")
			}
			if _, st := v.Lookup("gone"); st != StatusMiss {
				t.Fatal("view negative TTL ignored")
			}
			for range 2 {
				if _, err := v.GetOrLoad(ctx, "error"); err != errUpstream {
					t.Fatal(err)
				}
			}
			if got, err := v.GetOrLoad(ctx, "large"); len(got) != 1001 || err != nil {
				t.Fatalf("oversized: %q, %v", got, err)
			}
			if _, ok := v.Get("large"); ok {
				t.Fatal("oversized value cached")
			}
			if calls != 5 {
				t.Fatalf("calls = %d", calls)
			}
			if disabled {
				if v.Stats() != (ViewStats{}) || c.Stats().Loads != 0 {
					t.Fatal("disabled stats counted")
				}
			} else {
				vs, cs := v.Stats(), c.Stats()
				if vs.Loads != 5 || vs.LoadErrors != 3 || cs.Loads != vs.Loads || cs.LoadErrors != vs.LoadErrors || cs.Rejections != 1 || vs.TypeMisses != 1 {
					t.Fatalf("stats: %+v / %+v", vs, cs)
				}
			}
		})
	}
}

func TestViewLoaderIsolationAndDefaults(t *testing.T) {
	c := New(Options[string, any]{Loader: func(context.Context, string) (any, error) { t.Error("parent loader called"); return nil, nil }})
	defer c.Close()
	noLoader := NewView(c, ViewOptions[int]{Name: "none"})
	if err := noLoader.Set("k", 7); err != nil {
		t.Fatal(err)
	}
	if _, err := noLoader.GetOrLoad(context.Background(), "k"); err != ErrNoLoader {
		t.Fatal(err)
	}
	var calls atomic.Int64
	release := make(chan struct{})
	load := func(_ context.Context, key string) (int, error) {
		if key != "k" {
			t.Errorf("prefixed loader key %q", key)
		}
		calls.Add(1)
		<-release
		return 1, nil
	}
	// Even identical names do not share loaders; callers reuse a View instance.
	views := []*View[int]{NewView(c, ViewOptions[int]{Name: "a", Loader: load}), NewView(c, ViewOptions[int]{Name: "a", Loader: load}), NewView(c, ViewOptions[int]{Name: "b", Loader: load})}
	var wg sync.WaitGroup
	for _, v := range views {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := v.GetOrLoad(context.Background(), "k"); err != nil {
				t.Error(err)
			}
		}()
	}
	waitFor(t, "independent view loaders", func() bool { return calls.Load() == 3 })
	close(release)
	wg.Wait()
	missing := NewView(c, ViewOptions[int]{Name: "missing", Loader: func(context.Context, string) (int, error) { return 0, ErrNotFound }})
	for range 2 {
		if _, err := missing.GetOrLoad(context.Background(), "k"); err != ErrNotFound {
			t.Fatal(err)
		}
	}
	if missing.Stats().Loads != 2 {
		t.Fatal("negative cached without TTL")
	}
}

func TestViewLoaderCancellation(t *testing.T) {
	c := New(Options[string, any]{})
	defer c.Close()
	entered := make(chan context.Context, 2)
	release := make(chan struct{})
	type contextKey struct{}
	v := NewView(c, ViewOptions[int]{Name: "v", Loader: func(ctx context.Context, _ string) (int, error) {
		entered <- ctx
		select {
		case <-release:
			return 9, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}})
	first, cancel := context.WithCancel(context.WithValue(context.Background(), contextKey{}, "value"))
	defer cancel()
	firstErr := make(chan error, 1)
	go func() { _, err := v.GetOrLoad(first, "k"); firstErr <- err }()
	loadCtx := <-entered
	if loadCtx.Value(contextKey{}) != "value" {
		t.Fatal("context values lost")
	}
	second := make(chan int, 1)
	go func() {
		got, err := v.GetOrLoad(context.Background(), "k")
		if err != nil {
			t.Error(err)
		}
		second <- got
	}()
	waitFor(t, "second view waiter", func() bool { return v.Stats().Coalesced == 1 })
	cancel()
	if err := <-firstErr; err != context.Canceled {
		t.Fatal(err)
	}
	if loadCtx.Err() != nil {
		t.Fatal("load canceled with waiter remaining")
	}
	close(release)
	if <-second != 9 {
		t.Fatal("remaining waiter lost value")
	}
	if got, err := v.GetOrLoad(first, "k"); got != 9 || err != nil {
		t.Fatal("cancelled context did not get cached answer")
	}
	if _, err := v.GetOrLoad(first, "uncached"); err != context.Canceled {
		t.Fatal(err)
	}
}

func TestViewLoaderLastWaiterAndRetry(t *testing.T) {
	c := New(Options[string, any]{})
	defer c.Close()
	entered := make(chan context.Context, 1)
	v := NewView(c, ViewOptions[int]{Name: "v", Loader: func(ctx context.Context, _ string) (int, error) { entered <- ctx; <-ctx.Done(); return 0, ctx.Err() }})
	for range 2 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		done := make(chan error, 1)
		go func() { _, err := v.GetOrLoad(ctx, "k"); done <- err }()
		var loadCtx context.Context
		select {
		case loadCtx = <-entered:
		case <-ctx.Done():
			cancel()
			t.Fatal("retry joined abandoned load")
		}
		cancel()
		if err := <-done; err != context.Canceled {
			t.Fatal(err)
		}
		select {
		case <-loadCtx.Done():
		case <-time.After(time.Second):
			t.Fatal("loader not cancelled")
		}
	}
	waitFor(t, "cancelled loads to finish", func() bool { return v.Stats().Loads == 2 })
}

func TestViewLoaderPanicAndRetry(t *testing.T) {
	c := New(Options[string, any]{})
	defer c.Close()
	calls := 0
	v := NewView(c, ViewOptions[int]{Name: "v", Loader: func(context.Context, string) (int, error) {
		calls++
		if calls == 1 {
			panic("view boom")
		}
		return 4, nil
	}})
	func() {
		defer func() {
			p, ok := recover().(*loaderPanic)
			if !ok || !strings.Contains(p.Error(), "view boom") {
				t.Errorf("panic lost: %v", p)
			}
		}()
		_, _ = v.GetOrLoad(context.Background(), "k")
	}()
	if got, err := v.GetOrLoad(context.Background(), "k"); got != 4 || err != nil {
		t.Fatalf("retry: %d, %v", got, err)
	}
	if vs, cs := v.Stats(), c.Stats(); vs.Loads != 2 || vs.LoadErrors != 1 || cs.LoadErrors != 1 {
		t.Fatalf("stats: %+v / %+v", vs, cs)
	}
}

func TestViewLoadsShareBudgetAndInheritOptions(t *testing.T) {
	for _, policy := range []Policy{LRU, ClearOnFull} {
		t.Run(fmt.Sprint(policy), func(t *testing.T) {
			c := New(Options[string, any]{MaxBytes: 512, Cost: func(any) int64 { return 256 }, TTL: time.Minute, NegativeTTL: time.Minute, DisableCleanup: true, Policy: policy})
			defer c.Close()
			c.core.coarse = new(atomic.Int64)
			c.core.coarse.Store(time.Now().UnixNano())
			ints := NewView(c, ViewOptions[int]{Name: "int", Loader: func(context.Context, string) (int, error) { return 7, nil }})
			texts := NewView(c, ViewOptions[string]{Name: "text", Loader: func(_ context.Context, key string) (string, error) {
				if key == "gone" {
					return "", ErrNotFound
				}
				return key, nil
			}})
			ctx := context.Background()
			if _, err := ints.GetOrLoad(ctx, "k"); err != nil {
				t.Fatal(err)
			}
			if _, err := texts.GetOrLoad(ctx, "k"); err != nil {
				t.Fatal(err)
			}
			if c.Bytes() != 512 {
				t.Fatalf("fallback cost: %d", c.Bytes())
			}
			if _, err := texts.GetOrLoad(ctx, "next"); err != nil {
				t.Fatal(err)
			}
			if _, ok := ints.Get("k"); ok {
				t.Fatal("view load did not evict from shared budget")
			}
			if _, err := texts.GetOrLoad(ctx, "gone"); err != ErrNotFound {
				t.Fatal(err)
			}
			if _, st := texts.Lookup("gone"); st != StatusNegative {
				t.Fatal("negative TTL not inherited")
			}
			c.core.coarse.Add(int64(2 * time.Minute))
			if _, st := texts.Lookup("next"); st != StatusMiss {
				t.Fatal("TTL not inherited")
			}
			if _, st := texts.Lookup("gone"); st != StatusMiss {
				t.Fatal("inherited negative TTL did not expire")
			}
		})
	}
}
