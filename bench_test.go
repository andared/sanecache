package sanecache

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"
)

const benchKeys = 4096

func benchCache(shards int, policy Policy) *Cache[int, int] {
	return benchCacheClock(shards, policy, 0)
}

func benchCacheClock(shards int, policy Policy, granularity time.Duration) *Cache[int, int] {
	c := New(Options[int, int]{
		TTL:              time.Hour,
		Jitter:           10,
		MaxBytes:         benchKeys,
		Cost:             unitCost[int](),
		Shards:           shards,
		Policy:           policy,
		ClockGranularity: granularity,
	})
	for i := range benchKeys {
		// The budget is sized to fit exactly these keys, so Set cannot fail.
		_ = c.Set(i, i)
	}

	return c
}

// BenchmarkGet is the question that decides whether sharding earns its place:
// under LRU every read takes a write lock to reorder the list, so contention is
// the whole story. Under ClearOnFull reads only take a read lock.
func BenchmarkGet(b *testing.B) {
	for _, policy := range []Policy{LRU, ClearOnFull} {
		for _, shards := range []int{1, 4, 16, 64} {
			b.Run(fmt.Sprintf("%s/shards=%d", policy, shards), func(b *testing.B) {
				c := benchCache(shards, policy)
				defer c.Close()

				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					i := 0
					for pb.Next() {
						c.Get(i % benchKeys)
						i++
					}
				})
			})
		}
	}
}

func BenchmarkSet(b *testing.B) {
	for _, shards := range []int{1, 4, 16, 64} {
		b.Run(fmt.Sprintf("shards=%d", shards), func(b *testing.B) {
			c := benchCache(shards, LRU)
			defer c.Close()

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					_ = c.Set(i%benchKeys, i)
					i++
				}
			})
		})
	}
}

// BenchmarkMixed is the shape most services actually have: reads dominate, and
// writes only happen on a miss.
func BenchmarkMixed(b *testing.B) {
	for _, shards := range []int{1, 4, 16, 64} {
		b.Run(fmt.Sprintf("shards=%d", shards), func(b *testing.B) {
			c := benchCache(shards, LRU)
			defer c.Close()

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					if i%10 == 0 {
						_ = c.Set(i%benchKeys, i)
					} else {
						c.Get(i % benchKeys)
					}
					i++
				}
			})
		})
	}
}

// BenchmarkGetSerial isolates the per-operation cost from the contention story,
// and answers what ClockGranularity is worth: the difference between the two is
// the wall-clock read that every lookup otherwise does.
func BenchmarkGetSerial(b *testing.B) {
	for _, tc := range []struct {
		name        string
		granularity time.Duration
	}{
		{"exact-clock", 0},
		{"coarse-clock", time.Millisecond},
	} {
		b.Run(tc.name, func(b *testing.B) {
			c := benchCacheClock(1, LRU, tc.granularity)
			defer c.Close()

			b.ResetTimer()
			for i := range b.N {
				c.Get(i % benchKeys)
			}
		})
	}
}

// BenchmarkGetOrLoad measures the path a warm cache actually takes: the loader
// is never called, so what is left is the lookup plus the cost of having the
// single-flight machinery available at all.
func BenchmarkGetOrLoad(b *testing.B) {
	c := New(Options[int, int]{
		TTL:      time.Hour,
		MaxBytes: benchKeys,
		Cost:     unitCost[int](),
		Loader: func(_ context.Context, key int) (int, error) {
			return key, nil
		},
	})
	defer c.Close()
	for i := range benchKeys {
		_ = c.Set(i, i)
	}

	ctx := context.Background()
	b.ResetTimer()
	for i := range b.N {
		_, _ = c.GetOrLoad(ctx, i%benchKeys)
	}
}

// BenchmarkView is what a typed view costs over the cache underneath it: one
// concatenation to namespace the key, and one type assertion on the way out.
func BenchmarkView(b *testing.B) {
	c := New(Options[string, any]{TTL: time.Hour})
	defer c.Close()

	view := NewView(c, ViewOptions[int]{Name: "article"})
	keys := make([]string, benchKeys)
	prefixed := make([]string, benchKeys)
	for i := range keys {
		keys[i] = strconv.Itoa(i)
		prefixed[i] = "article:" + keys[i]
		if err := view.Set(keys[i], i); err != nil {
			b.Fatalf("Set: %v", err)
		}
	}

	b.Run("view", func(b *testing.B) {
		b.ReportAllocs()
		for i := range b.N {
			view.Get(keys[i%benchKeys])
		}
	})
	b.Run("cache", func(b *testing.B) {
		b.ReportAllocs()
		for i := range b.N {
			c.Get(prefixed[i%benchKeys])
		}
	})
}

// BenchmarkClockBaseline is the floor under every lookup: each Get reads the
// wall clock once to decide whether the entry has expired. It is here so that
// the numbers above can be read against something.
func BenchmarkClockBaseline(b *testing.B) {
	var sink int64
	for range b.N {
		sink = time.Now().UnixNano()
	}
	_ = sink
}
