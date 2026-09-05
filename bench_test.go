package sanecache

import (
	"fmt"
	"testing"
	"time"
)

const benchKeys = 4096

func benchCache(shards int, policy Policy) *Cache[int, int] {
	c := New(Options[int, int]{
		TTL:      time.Hour,
		Jitter:   10,
		MaxBytes: benchKeys,
		Cost:     unitCost[int](),
		Shards:   shards,
		Policy:   policy,
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

// BenchmarkGetSerial isolates the per-operation cost from the contention story.
func BenchmarkGetSerial(b *testing.B) {
	c := benchCache(1, LRU)
	defer c.Close()

	b.ResetTimer()
	for i := range b.N {
		c.Get(i % benchKeys)
	}
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
