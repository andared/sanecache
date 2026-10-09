package sanecache

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSetWithCost(t *testing.T) {
	c := New(Options[string, string]{MaxBytes: 100, Cost: func(string) int64 { return 1 }, DisableCleanup: true})
	defer c.Close()

	if err := c.SetWithCost("k", "v", 60); err != nil || c.Bytes() != 60 {
		t.Fatalf("err %v, bytes %d", err, c.Bytes())
	}
	if err := c.SetWithCost("big", "v", 101); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("over budget: %v", err)
	}
	if err := c.SetWithCost("k", "v", -5); err != nil || c.Bytes() != 0 {
		t.Fatalf("negative cost: err %v, bytes %d", err, c.Bytes())
	}

	a := New(Options[string, any]{MaxBytes: 100, Cost: func(any) int64 { return 1 }, DisableCleanup: true})
	defer a.Close()
	if err := NewView(a, ViewOptions[int]{Name: "n"}).SetWithCost("k", 1, 7); err != nil || a.Bytes() != 7 {
		t.Fatalf("view: err %v, bytes %d", err, a.Bytes())
	}
	if err := NewView(nil, ViewOptions[int]{Name: "off"}).SetWithCost("k", 1, 7); !errors.Is(err, ErrDisabled) {
		t.Fatalf("disabled view: %v", err)
	}
}

func TestGetOrLoadFuncWithCost(t *testing.T) {
	c := New(Options[string, string]{
		TTL: refreshTTL, NegativeTTL: time.Minute, RefreshAfter: refreshAfter,
		MaxBytes: 1000, Cost: func(string) int64 { return 1 }, DisableCleanup: true,
	})
	defer c.Close()
	advance := manualClock(c)
	ctx := context.Background()

	cost := int64(40)
	load := func(context.Context) (string, int64, error) { return "v", cost, nil }
	if v, err := c.GetOrLoadFuncWithCost(ctx, "k", load); v != "v" || err != nil || c.Bytes() != 40 {
		t.Fatalf("got %q, %v, bytes %d", v, err, c.Bytes())
	}

	// A refresh started by this method charges what its load reports, too.
	cost = 70
	advance(refreshAfter + time.Second)
	_, _ = c.GetOrLoadFuncWithCost(ctx, "k", load)
	waitFor(t, "the refresh", func() bool { return c.Bytes() == 70 })

	if v, err := c.GetOrLoadFuncWithCost(ctx, "big", func(context.Context) (string, int64, error) {
		return "huge", 1001, nil
	}); v != "huge" || err != nil || c.Stats().Rejections != 1 {
		t.Fatalf("too large: %q, %v, %+v", v, err, c.Stats())
	}
	if _, err := c.GetOrLoadFuncWithCost(ctx, "gone", func(context.Context) (string, int64, error) {
		return "", 0, ErrNotFound
	}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("not found: %v", err)
	}
	if _, st := c.Lookup("gone"); st != StatusNegative {
		t.Fatalf("not found was not remembered: %v", st)
	}
	if _, err := c.GetOrLoadFuncWithCost(ctx, "k", nil); !errors.Is(err, ErrNoLoader) {
		t.Fatalf("nil load: %v", err)
	}
}

func TestViewGetOrLoadFuncWithCost(t *testing.T) {
	c := New(Options[string, any]{TTL: time.Minute, MaxBytes: 100, Cost: func(any) int64 { return 1 }, DisableCleanup: true})
	defer c.Close()
	ctx := context.Background()
	load := func(context.Context) (int, int64, error) { return 7, 30, nil }

	v := NewView(c, ViewOptions[int]{Name: "n", Cost: func(int) int64 { return 2 }})
	if got, err := v.GetOrLoadFuncWithCost(ctx, "k", load); got != 7 || err != nil || c.Bytes() != 30 {
		t.Fatalf("got %d, %v, bytes %d", got, err, c.Bytes())
	}
	if _, err := v.GetOrLoadFuncWithCost(ctx, "k", nil); !errors.Is(err, ErrNoLoader) {
		t.Fatalf("nil load: %v", err)
	}

	off := NewView(nil, ViewOptions[int]{Name: "off"})
	if got, err := off.GetOrLoadFuncWithCost(ctx, "k", load); got != 7 || err != nil {
		t.Fatalf("disabled view: %d, %v", got, err)
	}
}
