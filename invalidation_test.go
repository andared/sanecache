package sanecache

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// Exercise the public boundary with a loader paused after reading its source.
// The channels determine ordering; the deadline only prevents a broken test hanging.
func TestLoadInvalidation(t *testing.T) {
	for _, view := range []bool{false, true} {
		for _, negative := range []bool{false, true} {
			for _, mutation := range []string{"delete", "clear", "set", "negative", "rejected"} {
				name := mutation
				if view {
					name += "/view"
				}
				if negative {
					name += "/negative-load"
				}
				t.Run(name, func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					started, release := make(chan struct{}), make(chan struct{})
					loader := func(context.Context, string) (int, error) {
						close(started)
						select {
						case <-release:
						case <-ctx.Done():
							return 0, ctx.Err()
						}
						if negative {
							return 0, ErrNotFound
						}
						return 1, nil
					}
					var load func(context.Context, string) (int, error)
					var lookup func(string) (int, Status)
					var mutate func() error
					if view {
						c := New(Options[string, any]{MaxBytes: 100, Cost: func(any) int64 { return 1 }, NegativeTTL: time.Minute})
						defer c.Close()
						v := NewView(c, ViewOptions[int]{Name: "items", Loader: loader})
						load, lookup = v.GetOrLoad, v.Lookup
						// A separate view shares storage and must invalidate the first's flight.
						writer := NewView(c, ViewOptions[int]{Name: "items", Cost: func(n int) int64 { return int64(n) }})
						mutate = func() error {
							switch mutation {
							case "delete":
								c.Delete("items:k")
							case "clear":
								c.Clear()
							case "set":
								return writer.Set("k", 2)
							case "negative":
								return writer.SetNegative("k")
							case "rejected":
								return writer.Set("k", 101)
							}
							return nil
						}
					} else {
						c := New(Options[string, int]{MaxBytes: 100, Cost: func(n int) int64 { return int64(n) }, NegativeTTL: time.Minute, Loader: loader})
						defer c.Close()
						load, lookup = c.GetOrLoad, c.Lookup
						mutate = func() error {
							switch mutation {
							case "delete":
								c.Delete("k")
							case "clear":
								c.Clear()
							case "set":
								return c.Set("k", 2)
							case "negative":
								return c.SetNegative("k")
							case "rejected":
								return c.Set("k", 101)
							}
							return nil
						}
					}
					done := make(chan error, 1)
					go func() {
						value, err := load(ctx, "k")
						if negative {
							if !errors.Is(err, ErrNotFound) {
								done <- errors.New("old caller lost not-found result")
								return
							}
						} else if err != nil || value != 1 {
							done <- errors.New("old caller lost loaded value")
							return
						}
						done <- nil
					}()
					select {
					case <-started:
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					err := mutate()
					if mutation == "rejected" {
						if !errors.Is(err, ErrTooLarge) {
							t.Fatalf("mutation: %v", err)
						}
					} else if err != nil {
						t.Fatal(err)
					}
					close(release)
					select {
					case err := <-done:
						if err != nil {
							t.Fatal(err)
						}
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					value, status := lookup("k")
					wantValue, wantStatus := 0, StatusMiss
					switch mutation {
					case "set":
						wantValue, wantStatus = 2, StatusHit
					case "negative":
						wantStatus = StatusNegative
					case "rejected":
						if negative {
							wantStatus = StatusNegative
						} else {
							wantValue, wantStatus = 1, StatusHit
						}
					}
					if value != wantValue || status != wantStatus {
						t.Fatalf("lookup = %d, %v; want %d, %v", value, status, wantValue, wantStatus)
					}
				})
			}
		}
	}
}

func TestNewCallerDoesNotJoinInvalidatedLoad(t *testing.T) {
	for _, view := range []bool{false, true} {
		t.Run(map[bool]string{false: "cache", true: "view"}[view], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			entered := make(chan chan struct{}, 2)
			var calls atomic.Int64
			loader := func(context.Context, string) (int, error) {
				number := int(calls.Add(1))
				release := make(chan struct{})
				entered <- release
				select {
				case <-release:
					return number, nil
				case <-ctx.Done():
					return 0, ctx.Err()
				}
			}
			var load func(context.Context, string) (int, error)
			var invalidate func()
			var lookup func(string) (int, Status)
			if view {
				c := New(Options[string, any]{})
				defer c.Close()
				v := NewView(c, ViewOptions[int]{Name: "items", Loader: loader})
				load, invalidate = v.GetOrLoad, func() { v.Delete("k") }
				lookup = v.Lookup
			} else {
				c := New(Options[string, int]{Loader: loader})
				defer c.Close()
				load, invalidate = c.GetOrLoad, func() { c.Delete("k") }
				lookup = c.Lookup
			}
			done := make(chan error, 2)
			start := func() { go func() { _, err := load(ctx, "k"); done <- err }() }
			receive := func() chan struct{} {
				t.Helper()
				select {
				case ch := <-entered:
					return ch
				case <-ctx.Done():
					t.Fatal("new caller joined obsolete load")
					return nil
				}
			}
			start()
			first := receive()
			invalidate()
			start()
			second := receive()
			close(second)
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			close(first)
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if value, status := lookup("k"); value != 2 || status != StatusHit {
				t.Fatalf("old load overwrote replacement: %d, %v", value, status)
			}
		})
	}
}

func TestCompetingViewAndParentLoads(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started, release := make(chan struct{}), make(chan struct{})
	c := New(Options[string, any]{Loader: func(context.Context, string) (any, error) { return 2, nil }})
	defer c.Close()
	v := NewView(c, ViewOptions[int]{Name: "items", Loader: func(context.Context, string) (int, error) {
		close(started)
		select {
		case <-release:
			return 1, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}})
	done := make(chan error, 1)
	go func() { _, err := v.GetOrLoad(ctx, "k"); done <- err }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if value, err := c.GetOrLoad(ctx, "items:k"); err != nil || value != 2 {
		t.Fatalf("parent load: %v, %v", value, err)
	}
	// A mutation to an unrelated key must not disturb the published result.
	c.Delete("items:other")
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if value, ok := v.Get("k"); !ok || value != 2 {
		t.Fatalf("obsolete view load published: %d, %v", value, ok)
	}
}

func TestDeleteDoesNotInvalidateOtherKeys(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started, release := make(chan struct{}), make(chan struct{})
	c := New(Options[string, int]{Loader: func(context.Context, string) (int, error) {
		close(started)
		select {
		case <-release:
			return 1, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}})
	defer c.Close()
	done := make(chan error, 1)
	go func() { _, err := c.GetOrLoad(ctx, "k"); done <- err }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	c.Delete("other")
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if value, ok := c.Get("k"); !ok || value != 1 {
		t.Fatalf("unrelated load invalidated: %d, %v", value, ok)
	}
}
