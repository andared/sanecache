// Package sanecache is a small in-memory cache that aims to be predictable
// before it is fast.
//
// Writes are synchronous, so a value is readable the moment Set returns. A value
// that cannot fit is refused with an error instead of being accepted and dropped
// later. Budgets are expressed in bytes, because a limit on the number of entries
// says nothing about the memory a process will use when entries are documents
// rather than integers. "The upstream says this key does not exist" is a first
// class answer rather than a marker smuggled inside the value type. And TTLs can
// carry jitter, so a batch of keys warmed by one request does not expire in
// lockstep and stampede the upstream.
package sanecache

import (
	"fmt"
	"hash/maphash"
	"math/bits"
	"math/rand/v2"
	"runtime"
	"sync"
	"time"
)

// Status is the outcome of a lookup.
type Status uint8

// The outcomes a lookup can report.
const (
	StatusMiss     Status = iota // the cache knows nothing about this key
	StatusHit                    // a value was cached
	StatusNegative               // the upstream was asked and said the key does not exist
)

func (s Status) String() string {
	switch s {
	case StatusHit:
		return "hit"
	case StatusNegative:
		return "negative"
	default:
		return "miss"
	}
}

// Policy decides what happens when a shard runs over its budget.
type Policy uint8

const (
	// LRU evicts the least recently used entries until the shard fits again.
	// Keeping that order costs a write lock on every read.
	LRU Policy = iota

	// ClearOnFull drops the whole shard except the entry that overflowed it.
	// Reads then need only a read lock, which is worth more than precise
	// eviction when access order is flat and refilling is cheap relative to
	// the bookkeeping.
	ClearOnFull
)

func (p Policy) String() string {
	if p == ClearOnFull {
		return "clear-on-full"
	}

	return "lru"
}

// EvictReason explains why an entry left the cache. Explicit Delete and Clear
// calls do not report a reason.
type EvictReason uint8

// The reasons an entry can leave the cache on its own.
const (
	ReasonEvicted  EvictReason = iota // dropped to stay inside the budget
	ReasonExpired                     // its TTL ran out
	ReasonReplaced                    // a later Set overwrote the key
)

func (r EvictReason) String() string {
	switch r {
	case ReasonExpired:
		return "expired"
	case ReasonReplaced:
		return "replaced"
	default:
		return "evicted"
	}
}

// Options configures a cache. The zero value is a valid, unbounded, never
// expiring cache; every field below is optional.
type Options[K comparable, V any] struct {
	// TTL is how long a value stays valid. Zero means entries never expire on
	// their own, which only makes sense for a cache bounded by MaxBytes or
	// MaxEntries, or one whose entries all get an explicit TTL via SetTTL.
	TTL time.Duration

	// NegativeTTL enables SetNegative and sets how long a cached "does not
	// exist" answer lives. It is usually much shorter than TTL: an object that
	// does not exist yet may appear at any moment.
	NegativeTTL time.Duration

	// Jitter spreads expiry times by up to this percentage in either direction,
	// so keys written together do not expire together. Must be 0..100.
	Jitter int

	// MaxBytes is the total budget in bytes, split evenly across shards. It
	// requires Cost. Zero means unbounded.
	MaxBytes int64

	// MaxEntries caps the number of entries, split evenly across shards. Zero
	// means unbounded. Prefer MaxBytes unless entries are uniform in size.
	MaxEntries int

	// Cost reports the memory a value occupies, in bytes. It is what MaxBytes is
	// measured against, so it should approximate resident size rather than
	// serialized size: a decoded struct commonly costs several times its JSON.
	// Measure it once rather than guessing (see the README).
	Cost func(V) int64

	// Shards splits the cache into independently locked parts, rounded up to a
	// power of two. Zero and one both mean a single lock. More shards reduce
	// contention but make the budget approximate: each shard gets an equal slice
	// of MaxBytes, and an uneven key distribution leaves some of it unused.
	//
	// The per-shard slice is rounded up, so the shards together are never
	// stricter than what was asked for. With small caps that rounding dominates:
	// MaxEntries of 2 across 16 shards is one entry per shard, or sixteen in
	// total. Keep the cap comfortably larger than the shard count.
	Shards int

	// Policy selects the eviction strategy. Defaults to LRU.
	Policy Policy

	// CleanupInterval is how often a background goroutine drops expired entries.
	// Zero picks an interval from the configured TTLs. Expired entries are also
	// dropped lazily on lookup, but until they are swept they still count
	// against the budget.
	CleanupInterval time.Duration

	// DisableCleanup runs the cache without a background goroutine. Expiry then
	// happens only on lookup and on eviction.
	DisableCleanup bool

	// OnEvict, if set, is called for every entry that leaves the cache without
	// being explicitly deleted. Negative entries are reported with the zero
	// value. It runs outside the shard lock, on the goroutine that caused the
	// removal, so it must not block.
	OnEvict func(key K, value V, reason EvictReason)

	// DisableStats skips the counters behind Stats.
	DisableStats bool
}

// Cache is a sharded, TTL-based cache. It is safe for concurrent use. The zero
// value is not usable; call New.
type Cache[K comparable, V any] struct {
	core *core[K, V]
}

// core holds everything the background sweeper touches. It is deliberately
// separate from Cache so that a Cache the caller has dropped can be collected
// while the sweeper goroutine is still shutting down.
type core[K comparable, V any] struct {
	shards []*shard[K, V]
	mask   uint64
	seed   maphash.Seed

	ttl         time.Duration
	negativeTTL time.Duration
	jitter      int64
	cost        func(V) int64
	onEvict     func(K, V, EvictReason)
	countStats  bool

	stats counters
	stop  *stopper
}

// New builds a cache from o. It panics on options that cannot describe a working
// cache, such as a byte budget without a Cost function: those are programming
// mistakes, and failing at construction beats a cache that silently misbehaves.
func New[K comparable, V any](o Options[K, V]) *Cache[K, V] {
	switch {
	case o.Jitter < 0 || o.Jitter > 100:
		panic(fmt.Sprintf("sanecache: Jitter must be 0..100, got %d", o.Jitter))
	case o.MaxBytes < 0:
		panic(fmt.Sprintf("sanecache: MaxBytes must not be negative, got %d", o.MaxBytes))
	case o.MaxEntries < 0:
		panic(fmt.Sprintf("sanecache: MaxEntries must not be negative, got %d", o.MaxEntries))
	case o.MaxBytes > 0 && o.Cost == nil:
		panic("sanecache: MaxBytes requires Cost; without it the budget would count entries, not bytes")
	case o.TTL < 0 || o.NegativeTTL < 0:
		panic("sanecache: TTL and NegativeTTL must not be negative")
	}

	n := shardCount(o.Shards)
	cr := &core[K, V]{
		shards:      make([]*shard[K, V], n),
		mask:        uint64(n - 1),
		seed:        maphash.MakeSeed(),
		ttl:         o.TTL,
		negativeTTL: o.NegativeTTL,
		jitter:      int64(o.Jitter),
		cost:        o.Cost,
		onEvict:     o.OnEvict,
		countStats:  !o.DisableStats,
	}

	perBytes := divideBudget(o.MaxBytes, int64(n))
	perEntries := int(divideBudget(int64(o.MaxEntries), int64(n)))
	for i := range cr.shards {
		cr.shards[i] = newShard[K, V](perBytes, perEntries, o.Policy)
	}

	c := &Cache[K, V]{core: cr}

	if interval := cr.cleanupInterval(o); interval > 0 {
		st := &stopper{ch: make(chan struct{})}
		cr.stop = st
		go cr.sweepLoop(interval, st.ch)
		// The sweeper keeps cr alive by itself, so stopping it has to hang off
		// the handle the caller holds rather than off cr. A caller that drops the
		// cache without calling Close does not leak the goroutine.
		runtime.AddCleanup(c, (*stopper).stop, st)
	}

	return c
}

// Get returns the cached value. A cached "does not exist" answer reports false,
// same as a miss; use Lookup to tell the two apart.
func (c *Cache[K, V]) Get(key K) (V, bool) {
	v, st := c.Lookup(key)

	return v, st == StatusHit
}

// Lookup returns the cached value and how the cache answered.
func (c *Cache[K, V]) Lookup(key K) (V, Status) {
	v, st, wasExpired := c.core.shardFor(key).get(key, time.Now().UnixNano())

	if c.core.countStats {
		switch st {
		case StatusHit:
			c.core.stats.hits.Add(1)
		case StatusNegative:
			c.core.stats.negatives.Add(1)
		default:
			c.core.stats.misses.Add(1)
			if wasExpired {
				c.core.stats.expirations.Add(1)
			}
		}
	}

	return v, st
}

// Set caches value under key for the configured TTL.
func (c *Cache[K, V]) Set(key K, value V) error {
	return c.SetTTL(key, value, c.core.ttl)
}

// SetTTL caches value under key for ttl, overriding Options.TTL. A ttl of zero
// means the entry never expires on its own.
func (c *Cache[K, V]) SetTTL(key K, value V, ttl time.Duration) error {
	var cost int64
	if c.core.cost != nil {
		cost = c.core.cost(value)
	}

	return c.core.store(&entry[K, V]{
		key:       key,
		value:     value,
		cost:      cost,
		expiresAt: c.core.expiryAt(ttl),
	})
}

// SetNegative records that the upstream reports no such key, for the configured
// NegativeTTL. Without it, a template that names a deleted object hits the
// upstream on every single render.
func (c *Cache[K, V]) SetNegative(key K) error {
	return c.SetNegativeTTL(key, c.core.negativeTTL)
}

// SetNegativeTTL is SetNegative with an explicit lifetime.
func (c *Cache[K, V]) SetNegativeTTL(key K, ttl time.Duration) error {
	if ttl <= 0 {
		return ErrNegativeDisabled
	}

	// A negative entry carries no value, but it is not free either: charging it
	// nothing would make it un-evictable under a byte budget.
	var cost int64
	if c.core.cost != nil {
		cost = negativeCost
	}

	return c.core.store(&entry[K, V]{
		key:       key,
		cost:      cost,
		expiresAt: c.core.expiryAt(ttl),
		negative:  true,
	})
}

// Delete removes key and reports whether it was present. OnEvict is not called.
func (c *Cache[K, V]) Delete(key K) bool {
	_, ok := c.core.shardFor(key).delete(key)

	return ok
}

// Clear empties the cache. OnEvict is not called.
func (c *Cache[K, V]) Clear() {
	for _, s := range c.core.shards {
		s.clear()
	}
}

// Len reports how many entries are held, including expired ones not yet swept.
func (c *Cache[K, V]) Len() int {
	n := 0
	for _, s := range c.core.shards {
		entries, _ := s.stats()
		n += entries
	}

	return n
}

// Bytes reports the summed cost of the entries held.
func (c *Cache[K, V]) Bytes() int64 {
	var total int64
	for _, s := range c.core.shards {
		_, bytes := s.stats()
		total += bytes
	}

	return total
}

// Stats returns a snapshot of the counters. It walks every shard, so poll it on
// a metrics interval rather than per request.
func (c *Cache[K, V]) Stats() Stats {
	st := Stats{
		Hits:         c.core.stats.hits.Load(),
		Misses:       c.core.stats.misses.Load(),
		Negatives:    c.core.stats.negatives.Load(),
		Evictions:    c.core.stats.evictions.Load(),
		Expirations:  c.core.stats.expirations.Load(),
		Replacements: c.core.stats.replacements.Load(),
		Rejections:   c.core.stats.rejections.Load(),
	}
	for _, s := range c.core.shards {
		entries, bytes := s.stats()
		st.Entries += entries
		st.Bytes += bytes
	}

	return st
}

// Close stops the background sweeper. The cache stays usable afterwards; entries
// then expire only on lookup. Calling Close more than once is safe, and a cache
// that is simply dropped stops its sweeper too.
func (c *Cache[K, V]) Close() {
	if c.core.stop != nil {
		c.core.stop.stop()
	}
}

// negativeCost is what a "does not exist" marker is charged under a byte budget:
// it holds no value, but it does hold a key and a map slot.
const negativeCost int64 = 64

func (c *core[K, V]) store(e *entry[K, V]) error {
	replaced, victims, err := c.shardFor(e.key).set(e)
	if err != nil {
		if c.countStats {
			c.stats.rejections.Add(1)
		}

		return err
	}

	if replaced != nil {
		if c.countStats {
			c.stats.replacements.Add(1)
		}
		c.notify(replaced, ReasonReplaced)
	}
	for _, v := range victims {
		if c.countStats {
			c.stats.evictions.Add(1)
		}
		c.notify(v, ReasonEvicted)
	}

	return nil
}

func (c *core[K, V]) shardFor(key K) *shard[K, V] {
	if len(c.shards) == 1 {
		return c.shards[0]
	}

	return c.shards[maphash.Comparable(c.seed, key)&c.mask]
}

// expiryAt turns a TTL into an absolute deadline, spreading it by Jitter percent.
func (c *core[K, V]) expiryAt(ttl time.Duration) int64 {
	if ttl <= 0 {
		return 0
	}

	if c.jitter > 0 {
		if span := int64(ttl) * c.jitter / 100; span > 0 {
			ttl += time.Duration(rand.Int64N(2*span+1) - span)
		}
		// Full jitter can land on zero, which would mean "never expires".
		if ttl <= 0 {
			ttl = 1
		}
	}

	return time.Now().Add(ttl).UnixNano()
}

func (c *core[K, V]) notify(e *entry[K, V], r EvictReason) {
	if c.onEvict != nil {
		c.onEvict(e.key, e.value, r)
	}
}

func (c *core[K, V]) sweepLoop(interval time.Duration, stop <-chan struct{}) {
	t := time.NewTicker(interval)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			c.sweep(time.Now().UnixNano())
		case <-stop:
			return
		}
	}
}

func (c *core[K, V]) sweep(now int64) {
	for _, s := range c.shards {
		for _, e := range s.sweep(now) {
			if c.countStats {
				c.stats.expirations.Add(1)
			}
			c.notify(e, ReasonExpired)
		}
	}
}

// cleanupInterval picks how often to sweep: often enough that expired entries do
// not sit on the budget for a large fraction of their own lifetime, and rarely
// enough that an idle cache stays idle.
func (c *core[K, V]) cleanupInterval(o Options[K, V]) time.Duration {
	if o.DisableCleanup {
		return 0
	}
	if o.CleanupInterval > 0 {
		return o.CleanupInterval
	}

	shortest := o.TTL
	if o.NegativeTTL > 0 && (shortest == 0 || o.NegativeTTL < shortest) {
		shortest = o.NegativeTTL
	}
	switch {
	case shortest == 0:
		// Only per call TTLs, if any. Sweeping on a guessed interval would be
		// noise; lookups still expire entries lazily.
		return 0
	case shortest > time.Minute:
		return time.Minute
	case shortest < time.Second:
		return time.Second
	default:
		return shortest
	}
}

// stopper closes a channel exactly once, whether that comes from Close or from
// the cleanup attached to a dropped Cache.
type stopper struct {
	once sync.Once
	ch   chan struct{}
}

func (s *stopper) stop() {
	s.once.Do(func() { close(s.ch) })
}

func shardCount(n int) int {
	if n <= 1 {
		return 1
	}

	return 1 << bits.Len(uint(n-1))
}

// divideBudget splits a total across n shards, rounding up so that the shards
// together are never stricter than the total the caller asked for.
func divideBudget(total, n int64) int64 {
	if total <= 0 {
		return 0
	}

	return (total + n - 1) / n
}
