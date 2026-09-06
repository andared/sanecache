// Package sanecache is a small in-memory cache that aims to be predictable
// before it is fast.
//
// Writes are synchronous, so a value is readable the moment Set returns. A value
// that cannot fit is refused with an error instead of being accepted and dropped
// later. Budgets are expressed in bytes, because a limit on the number of entries
// says nothing about the memory a process will use when entries are documents
// rather than integers. "The upstream says this key does not exist" is a first
// class answer rather than a marker smuggled inside the value type. TTLs can
// carry jitter, so a batch of keys warmed by one request does not expire in
// lockstep and stampede the upstream. And a cold key is fetched once rather than
// once per concurrent caller asking for it.
package sanecache

import (
	"context"
	"fmt"
	"hash/maphash"
	"math/bits"
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
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

	// Loader fetches a value that is not cached. It is what GetOrLoad calls, and
	// callers that ask for the same key while it is running share the one call
	// instead of each starting their own.
	//
	// Returning an error that wraps ErrNotFound means the upstream has no such
	// key: the answer is cached as a negative entry when NegativeTTL is set, and
	// reported to every waiting caller. Any other error is passed through
	// unchanged and is not cached.
	//
	// The context is not any one caller's: it carries the values of the caller
	// that started the load, but it is cancelled only once every caller waiting
	// for the result has given up. A loader must not call GetOrLoad on the same
	// cache and key, which would wait for itself.
	//
	// A load that nobody is waiting for any more is cancelled, but a loader that
	// does not watch its context finishes regardless, and its value is cached
	// even so. That is what keeps a cache warming when callers time out faster
	// than the upstream answers; the price is that such a load can land after a
	// later one and put back a value read before it, with the TTL starting over.
	// A loader that honours cancellation never gets there.
	Loader func(ctx context.Context, key K) (V, error)

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

	// ClockGranularity trades TTL precision for lookup speed. Reading the wall
	// clock is about half the cost of a lookup, so a cache under enough load for
	// that to show up can have a background goroutine hold the time instead,
	// refreshed this often. Expiry is then accurate to within one interval in
	// either direction. Zero, the default, reads the clock on every operation.
	//
	// The goroutine is separate from the sweeper, so this works with
	// DisableCleanup. Close stops it, and lookups go back to the wall clock.
	ClockGranularity time.Duration

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

// core holds everything the background goroutines touch. It is deliberately
// separate from Cache so that a Cache the caller has dropped can be collected
// while those goroutines are still shutting down.
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

	loader  func(context.Context, K) (V, error)
	flights []*flightGroup[K, V]

	// coarse holds the time a background goroutine last read, in unix
	// nanoseconds, or nil when the cache reads the clock itself. Zero means the
	// goroutine has stopped and the wall clock is authoritative again.
	coarse *atomic.Int64

	stop *stopper
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
	case o.ClockGranularity < 0:
		panic("sanecache: ClockGranularity must not be negative")
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
		loader:      o.Loader,
	}

	perBytes := divideBudget(o.MaxBytes, int64(n))
	perEntries := int(divideBudget(int64(o.MaxEntries), int64(n)))
	for i := range cr.shards {
		cr.shards[i] = newShard[K, V](perBytes, perEntries, o.Policy)
	}
	if o.Loader != nil {
		cr.flights = make([]*flightGroup[K, V], n)
		for i := range cr.flights {
			cr.flights[i] = newFlightGroup[K, V]()
		}
	}
	if o.ClockGranularity > 0 {
		// Seeded here rather than on the first tick: a lookup between New and
		// that tick would otherwise see the epoch and expire everything.
		cr.coarse = new(atomic.Int64)
		cr.coarse.Store(time.Now().UnixNano())
	}

	c := &Cache[K, V]{core: cr}

	sweepEvery := cr.cleanupInterval(o)
	if sweepEvery > 0 || cr.coarse != nil {
		st := &stopper{ch: make(chan struct{})}
		cr.stop = st
		if sweepEvery > 0 {
			go cr.sweepLoop(sweepEvery, st.ch)
		}
		if cr.coarse != nil {
			go cr.clockLoop(o.ClockGranularity, st.ch)
		}
		// The goroutines keep cr alive by themselves, so stopping them has to
		// hang off the handle the caller holds rather than off cr. A caller that
		// drops the cache without calling Close does not leak them.
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
	v, st, _ := c.core.lookup(key)

	return v, st
}

// Set caches value under key for the configured TTL.
func (c *Cache[K, V]) Set(key K, value V) error {
	return c.SetTTL(key, value, c.core.ttl)
}

// SetTTL caches value under key for ttl, overriding Options.TTL. A ttl of zero
// means the entry never expires on its own.
func (c *Cache[K, V]) SetTTL(key K, value V, ttl time.Duration) error {
	return c.core.setValue(key, value, c.core.valueCost(value), ttl)
}

// SetNegative records that the upstream reports no such key, for the configured
// NegativeTTL. Without it, a template that names a deleted object hits the
// upstream on every single render.
func (c *Cache[K, V]) SetNegative(key K) error {
	return c.core.setNegative(key, c.core.negativeTTL)
}

// SetNegativeTTL is SetNegative with an explicit lifetime.
func (c *Cache[K, V]) SetNegativeTTL(key K, ttl time.Duration) error {
	return c.core.setNegative(key, ttl)
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
	var st Stats
	for _, s := range c.core.shards {
		entries, bytes := s.stats()
		st.Entries += entries
		st.Bytes += bytes
		s.counters.addTo(&st)
	}

	return st
}

// Close stops the background goroutines. The cache stays usable afterwards:
// entries then expire only on lookup, and a cache configured with
// ClockGranularity goes back to reading the wall clock. Calling Close more than
// once is safe, and a cache that is simply dropped stops its goroutines too.
func (c *Cache[K, V]) Close() {
	if c.core.stop != nil {
		c.core.stop.stop()
	}
}

// negativeCost is what a "does not exist" marker is charged under a byte budget:
// it holds no value, but it does hold a key and a map slot.
const negativeCost int64 = 64

func (c *core[K, V]) valueCost(v V) int64 {
	if c.cost == nil {
		return 0
	}

	return c.cost(v)
}

func (c *core[K, V]) setValue(key K, value V, cost int64, ttl time.Duration) error {
	return c.store(&entry[K, V]{
		key:       key,
		value:     value,
		cost:      cost,
		expiresAt: c.expiryAt(ttl),
	})
}

func (c *core[K, V]) setNegative(key K, ttl time.Duration) error {
	if ttl <= 0 {
		return ErrNegativeDisabled
	}

	// A negative entry carries no value, but it is not free either: charging it
	// nothing would make it un-evictable under a byte budget.
	var cost int64
	if c.cost != nil {
		cost = negativeCost
	}

	return c.store(&entry[K, V]{
		key:       key,
		cost:      cost,
		expiresAt: c.expiryAt(ttl),
		negative:  true,
	})
}

func (c *core[K, V]) store(e *entry[K, V]) error {
	s := c.shardFor(e.key)

	replaced, victims, err := s.set(e)
	if err != nil {
		if c.countStats {
			s.counters.rejections.Add(1)
		}

		return err
	}

	if replaced != nil {
		if c.countStats {
			s.counters.replacements.Add(1)
		}
		c.notify(replaced, ReasonReplaced)
	}
	for _, v := range victims {
		if c.countStats {
			s.counters.evictions.Add(1)
		}
		c.notify(v, ReasonEvicted)
	}

	return nil
}

// lookup is Cache.Lookup with the shard it landed on, which a view needs in
// order to count a type miss where the rest of that key's counters live.
func (c *core[K, V]) lookup(key K) (V, Status, *shard[K, V]) {
	s := c.shardFor(key)
	v, st, wasExpired := s.get(key, c.now())

	if c.countStats {
		switch st {
		case StatusHit:
			s.counters.hits.Add(1)
		case StatusNegative:
			s.counters.negatives.Add(1)
		default:
			s.counters.misses.Add(1)
			if wasExpired {
				s.counters.expirations.Add(1)
			}
		}
	}

	return v, st, s
}

func (c *core[K, V]) shardIndex(key K) uint64 {
	if len(c.shards) == 1 {
		return 0
	}

	return maphash.Comparable(c.seed, key) & c.mask
}

func (c *core[K, V]) shardFor(key K) *shard[K, V] {
	return c.shards[c.shardIndex(key)]
}

// now is the time expiry is judged against: the wall clock, or what the clock
// goroutine last read when ClockGranularity asked for one.
func (c *core[K, V]) now() int64 {
	if c.coarse != nil {
		if t := c.coarse.Load(); t != 0 {
			return t
		}
	}

	return time.Now().UnixNano()
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

	return c.now() + int64(ttl)
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
			c.sweep(c.now())
		case <-stop:
			return
		}
	}
}

// clockLoop keeps core.now cheap. On the way out it publishes a zero, which
// hands lookups back to the wall clock rather than freezing time at whatever
// this goroutine last saw.
func (c *core[K, V]) clockLoop(granularity time.Duration, stop <-chan struct{}) {
	t := time.NewTicker(granularity)
	defer t.Stop()
	defer c.coarse.Store(0)

	for {
		select {
		case <-t.C:
			// The clock is read here rather than taken from the tick. A tick
			// carries the time it fired, which after a pause or a busy scheduler
			// can be several intervals old — and publishing that would make
			// expiry lag by the delay on top of the granularity, which is more
			// than this option promises.
			c.coarse.Store(time.Now().UnixNano())
		case <-stop:
			return
		}
	}
}

func (c *core[K, V]) sweep(now int64) {
	for _, s := range c.shards {
		for _, e := range s.sweep(now) {
			if c.countStats {
				s.counters.expirations.Add(1)
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
