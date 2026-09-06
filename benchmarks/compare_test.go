// Package benchmarks compares sanecache against the caches it would otherwise
// be replacing. It lives in its own module so that sanecache itself keeps its
// promise of no dependencies.
//
// The comparison is deliberately like for like: every cache holds fixed-size
// values, gets a budget sized to the same number of them, and a TTL long enough
// that nothing expires during a run. Where a cache has no notion of cost, it is
// capped at the equivalent entry count instead.
//
// These numbers are not an argument. sanecache does not try to win on
// throughput, and on write-heavy work it will not: a synchronous Set that can
// report failure costs more than a buffered one that cannot. The reason to
// measure is to know what the guarantees cost, and to be able to say so.
package benchmarks

import (
	"math/rand/v2"
	"strconv"
	"testing"
	"time"

	"github.com/andared/sanecache"
	"github.com/dgraph-io/ristretto/v2"
	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/jellydator/ttlcache/v3"
	"github.com/maypok86/otter/v2"
)

const (
	capacity  = 4096             // entries the budget is sized to hold
	valueSize = 256              // bytes per value
	benchTTL  = 30 * time.Minute // long enough that expiry never fires mid-run
)

// cache is the small slice of behaviour all of these libraries have in common.
type cache interface {
	get(key string) bool
	set(key string, value []byte)
	// wait blocks until earlier writes are visible. It is a no-op everywhere
	// except ristretto, whose Set is asynchronous.
	wait()
	close()
}

type factory struct {
	name string
	open func() cache
}

func factories() []factory {
	return []factory{
		// Four rows because each one is a knob, and the point is what each is
		// worth: the defaults, then sharding, then the coarse clock, then the
		// policy that does not reorder a list on every read.
		{"sanecache", func() cache { return openSane(0, 0, sanecache.LRU) }},
		{"sanecache-sharded", func() cache { return openSane(16, 0, sanecache.LRU) }},
		{"sanecache-coarse-clock", func() cache {
			return openSane(16, time.Millisecond, sanecache.LRU)
		}},
		{"sanecache-clear-on-full", func() cache {
			return openSane(16, time.Millisecond, sanecache.ClearOnFull)
		}},
		{"otter", openOtter},
		{"ristretto", openRistretto},
		{"golang-lru-expirable", openExpirable},
		{"ttlcache", openTTLCache},
	}
}

type saneCache struct {
	c *sanecache.Cache[string, []byte]
}

func openSane(shards int, granularity time.Duration, policy sanecache.Policy) cache {
	return saneCache{sanecache.New(sanecache.Options[string, []byte]{
		TTL:              benchTTL,
		MaxBytes:         capacity * valueSize,
		Cost:             func(v []byte) int64 { return int64(len(v)) },
		Shards:           shards,
		Policy:           policy,
		ClockGranularity: granularity,
	})}
}

func (s saneCache) get(key string) bool { _, ok := s.c.Get(key); return ok }

func (s saneCache) set(key string, v []byte) { _ = s.c.Set(key, v) }

func (s saneCache) wait() {}

func (s saneCache) close() { s.c.Close() }

type otterCache struct{ c *otter.Cache[string, []byte] }

func openOtter() cache {
	return otterCache{otter.Must(&otter.Options[string, []byte]{
		MaximumWeight:    capacity * valueSize,
		Weigher:          func(_ string, v []byte) uint32 { return uint32(len(v)) },
		ExpiryCalculator: otter.ExpiryWriting[string, []byte](benchTTL),
	})}
}

func (o otterCache) get(key string) bool { _, ok := o.c.GetIfPresent(key); return ok }

func (o otterCache) set(key string, v []byte) { o.c.Set(key, v) }

func (o otterCache) wait() {}

func (o otterCache) close() {}

type ristrettoCache struct {
	c *ristretto.Cache[string, []byte]
}

func openRistretto() cache {
	c, err := ristretto.NewCache(&ristretto.Config[string, []byte]{
		// The TinyLFU counter count, which the documentation puts at ten times
		// the number of entries the cache is expected to hold. There is no way
		// to derive that from a byte budget without also guessing the average
		// entry size, which is the calculation this library exists to avoid.
		NumCounters: capacity * 10,
		MaxCost:     capacity * valueSize,
		BufferItems: 64,
	})
	if err != nil {
		panic(err)
	}

	return ristrettoCache{c}
}

func (r ristrettoCache) get(key string) bool { _, ok := r.c.Get(key); return ok }

func (r ristrettoCache) set(key string, v []byte) {
	r.c.SetWithTTL(key, v, int64(len(v)), benchTTL)
}

func (r ristrettoCache) wait() { r.c.Wait() }

func (r ristrettoCache) close() { r.c.Close() }

type expirableCache struct {
	c *expirable.LRU[string, []byte]
}

func openExpirable() cache {
	// Entry count rather than bytes: this cache has no notion of cost.
	return expirableCache{expirable.NewLRU[string, []byte](capacity, nil, benchTTL)}
}

func (e expirableCache) get(key string) bool { _, ok := e.c.Get(key); return ok }

func (e expirableCache) set(key string, v []byte) { e.c.Add(key, v) }

func (e expirableCache) wait() {}

func (e expirableCache) close() {}

type ttlCache struct {
	c *ttlcache.Cache[string, []byte]
}

func openTTLCache() cache {
	c := ttlcache.New[string, []byte](
		ttlcache.WithTTL[string, []byte](benchTTL),
		ttlcache.WithCapacity[string, []byte](capacity),
		// Off, so that a read here does not extend a TTL the others leave alone.
		ttlcache.WithDisableTouchOnHit[string, []byte](),
	)
	go c.Start()

	return ttlCache{c}
}

func (t ttlCache) get(key string) bool { return t.c.Get(key) != nil }

func (t ttlCache) set(key string, v []byte) { t.c.Set(key, v, ttlcache.DefaultTTL) }

func (t ttlCache) wait() {}

func (t ttlCache) close() { t.c.Stop() }

// keySet precomputes keys so that the benchmarks measure the cache rather than
// strconv.
func keySet(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = "key:" + strconv.Itoa(i)
	}

	return keys
}

func value() []byte { return make([]byte, valueSize) }

// warm fills a cache with every key. The budget is sized to hold them all, so
// the reads that follow are hits everywhere.
func warm(c cache, keys []string) {
	v := value()
	for _, k := range keys {
		c.set(k, v)
	}
	c.wait()
}

func BenchmarkGet(b *testing.B) {
	keys := keySet(capacity)

	for _, f := range factories() {
		b.Run(f.name, func(b *testing.B) {
			c := f.open()
			defer c.close()
			warm(c, keys)

			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					c.get(keys[i%len(keys)])
					i++
				}
			})
		})
	}
}

func BenchmarkSet(b *testing.B) {
	keys := keySet(capacity)
	v := value()

	for _, f := range factories() {
		b.Run(f.name, func(b *testing.B) {
			c := f.open()
			defer c.close()

			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					c.set(keys[i%len(keys)], v)
					i++
				}
			})
		})
	}
}

// BenchmarkGetSerial takes the contention out of the question. Whatever is left
// between two caches here is the cost of the operation itself: hashing, the map,
// the bookkeeping each one does per read. The difference between this and
// BenchmarkGet is what each cache pays for being used by more than one goroutine.
func BenchmarkGetSerial(b *testing.B) {
	keys := keySet(capacity)

	for _, f := range factories() {
		b.Run(f.name, func(b *testing.B) {
			c := f.open()
			defer c.close()
			warm(c, keys)

			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				c.get(keys[i%len(keys)])
			}
		})
	}
}

// BenchmarkMixed is the shape most services have: reads dominate, and a write
// happens only on a miss.
func BenchmarkMixed(b *testing.B) {
	keys := keySet(capacity)
	v := value()

	for _, f := range factories() {
		b.Run(f.name, func(b *testing.B) {
			c := f.open()
			defer c.close()
			warm(c, keys)

			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					if i%10 == 0 {
						c.set(keys[i%len(keys)], v)
					} else {
						c.get(keys[i%len(keys)])
					}
					i++
				}
			})
		})
	}
}

// BenchmarkHitRate is the comparison that matters more than nanoseconds: given
// the same memory and the same requests, how much of that memory does each
// eviction policy turn into hits. This is where an admission policy earns its
// complexity and where a plain LRU does not.
//
// Writes are drained before the next read, so what is measured is the eviction
// policy rather than the depth of a write buffer. That a buffer exists at all is
// a separate matter, and one a synchronous Set answers directly.
//
// Run this with a fixed count, -benchtime=300000x. On a time-based benchtime
// each cache runs a different number of operations, and a hit rate measured over
// a different number of requests is not comparable with the one next to it.
func BenchmarkHitRate(b *testing.B) {
	keys := keySet(universe)
	v := value()

	for _, w := range workloads() {
		for _, f := range factories() {
			b.Run(w.name+"/"+f.name, func(b *testing.B) {
				c := f.open()
				defer c.close()
				next := w.open()

				var hits, total int
				b.ResetTimer()
				for range b.N {
					key := keys[next()]
					total++
					if c.get(key) {
						hits++

						continue
					}
					c.set(key, v)
					c.wait()
				}
				b.StopTimer()

				if total > 0 {
					b.ReportMetric(100*float64(hits)/float64(total), "%hit")
				}
				// ns/op here mixes hits with misses and their refills, and is
				// not comparable with the benchmarks above. The hit rate is the
				// number to read.
				b.ReportMetric(0, "ns/op")
			})
		}
	}
}

// universe is the key space the hit-rate workloads draw from: 25x what the
// cache can hold, so that what to keep is a real decision.
const universe = 100_000

type workload struct {
	name string
	// open returns a fresh key generator, seeded the same way every time so
	// that every cache sees exactly the same request sequence.
	open func() func() int
}

func workloads() []workload {
	return []workload{
		// A hot head small enough to fit. Any policy that keeps recently used
		// things does well here, which is why it is the easy case.
		{"zipf-1.20", func() func() int { return zipfKeys(1.2, universe) }},
		// A working set larger than the cache: now the policy has to choose.
		{"zipf-1.01", func() func() int { return zipfKeys(1.01, universe) }},
		// The pattern a plain LRU handles worst.
		{"zipf-scan", scanKeys},
	}
}

func zipfKeys(s float64, n int) func() int {
	z := rand.NewZipf(rand.New(rand.NewPCG(1, 2)), s, 1, uint64(n-1))

	return func() int { return int(z.Uint64()) }
}

// scanKeys is a hot set that would fit, interrupted every tenth request by a
// sweep through keys nobody will ask for twice. An LRU admits every one of them
// and evicts part of the hot set to make room; an admission policy declines to.
func scanKeys() func() int {
	hot := zipfKeys(1.2, universe/2)
	var i int

	return func() int {
		i++
		if i%10 != 0 {
			return hot()
		}

		return universe/2 + (i/10)%(universe/2)
	}
}

// TestEveryCacheHoldsWhatItIsGiven is a guard on the comparison itself: a cache
// misconfigured into never storing anything would otherwise look very fast.
func TestEveryCacheHoldsWhatItIsGiven(t *testing.T) {
	keys := keySet(8)
	v := value()

	for _, f := range factories() {
		t.Run(f.name, func(t *testing.T) {
			c := f.open()
			defer c.close()

			c.set(keys[0], v)
			c.wait()
			if !c.get(keys[0]) {
				t.Fatal("a value written to an empty cache was not readable")
			}
			if c.get(keys[1]) {
				t.Fatal("a key that was never written came back")
			}
		})
	}
}
