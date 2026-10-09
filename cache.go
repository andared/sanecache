// Package sanecache is a small in-memory cache that is predictable before it is
// fast: writes are synchronous, a value that cannot fit is refused with an error,
// budgets are in bytes, "does not exist" is a first-class answer, TTLs carry
// jitter so that keys warmed together do not expire together, and a cold key is
// loaded once however many callers ask for it.
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

// Options configures a cache. Every field is optional: the zero value is an
// unbounded cache whose entries never expire.
type Options[K comparable, V any] struct {
	// TTL is how long a value stays valid. Zero means it never expires, which
	// suits a bounded cache or one written with SetTTL.
	TTL time.Duration

	// NegativeTTL enables SetNegative and sets how long a "does not exist" answer
	// lives. Keep it short: a missing object may appear at any moment.
	NegativeTTL time.Duration

	// Jitter spreads expiry by up to this percentage either way, so that keys
	// written together do not expire together. Must be 0..100.
	Jitter int

	// RefreshAfter reloads a value in the background once it is this old, so that
	// a key in steady use is replaced before it expires instead of making the next
	// caller wait. Zero turns it off. It must be shorter than TTL, which still
	// holds: a failed refresh is not retried, and the value lives out its TTL.
	//
	// The read that finds a value due returns it and starts its one refresh:
	// GetOrLoad, GetManyOrLoad and GetOrLoadFunc with their loaders, Get and
	// Lookup with Loader or BatchLoader. GetManyOrLoad refreshes the keys it finds
	// due in one BatchLoader call. A refresh is an ordinary load to single flight
	// and invalidation; its context carries the values of the read that started
	// it and is never cancelled. ErrNotFound replaces the value with a negative
	// entry when NegativeTTL is set.
	RefreshAfter time.Duration

	// MaxBytes is the budget in bytes, split evenly across shards. It requires
	// Cost. Zero means unbounded.
	MaxBytes int64

	// MaxEntries caps the number of entries, split evenly across shards. Zero
	// means unbounded. Prefer MaxBytes unless entries are uniform in size.
	MaxEntries int

	// Cost reports the resident size of a value in bytes, which is what MaxBytes
	// counts. A decoded struct commonly costs several times its JSON: measure it
	// rather than guess (see the README).
	Cost func(V) int64

	// Loader fetches a value that is not cached, for GetOrLoad. Callers who ask
	// for a key while its load runs share that one call.
	//
	// An error wrapping ErrNotFound means the upstream has no such key, and is
	// cached as a negative entry when NegativeTTL is set. Other errors are
	// returned unchanged and not cached.
	//
	// The context carries the values of the caller that started the load, but is
	// cancelled only once every waiting caller has given up. A loader that ignores
	// it still caches its result, unless a Delete, a Clear or a successful write of
	// the key came after the load started. A loader must not GetOrLoad its own key.
	Loader func(ctx context.Context, key K) (V, error)

	// BatchLoader fetches several uncached keys in one call: the keys of
	// GetManyOrLoad nobody is loading yet, and the key of GetOrLoad when Loader is
	// not set.
	//
	// A requested key missing from the result does not exist upstream, as if a
	// Loader had returned ErrNotFound; keys not requested are ignored. An error
	// fails every key, and nothing from the call is cached. The context is
	// cancelled once no caller waits for any of its keys.
	BatchLoader func(ctx context.Context, keys []K) (map[K]V, error)

	// Shards splits the cache into independently locked parts, rounded up to a
	// power of two; zero and one both mean a single lock. Each shard gets an equal
	// slice of the budget, rounded up, so the budget becomes approximate. Keep it
	// well above the shard count: MaxEntries of 2 across 16 shards holds 16.
	Shards int

	// Policy selects the eviction strategy. Defaults to LRU.
	Policy Policy

	// CleanupInterval is how often a background goroutine drops expired entries.
	// Zero picks it from the TTLs. Lookups drop expired entries too, but until
	// then they count against the budget.
	CleanupInterval time.Duration

	// DisableCleanup runs without that goroutine: entries then expire only on
	// lookup and on eviction.
	DisableCleanup bool

	// ClockGranularity has a background goroutine read the clock this often,
	// instead of every lookup paying for it, which is about half of what a lookup
	// costs. Expiry is then accurate to one interval either way. Zero reads the
	// clock every time. It works with DisableCleanup; Close returns lookups to the
	// wall clock.
	ClockGranularity time.Duration

	// OnEvict is called for every entry that leaves the cache other than by Delete
	// or Clear, with the zero value for a negative entry. It runs outside the
	// shard lock on the goroutine that caused the removal, so it must not block.
	OnEvict func(key K, value V, reason EvictReason)

	// DisableStats skips the counters behind Stats.
	DisableStats bool
}

// Cache is a sharded, TTL-based cache. It is safe for concurrent use. The zero
// value is not usable; call New.
type Cache[K comparable, V any] struct {
	core *core[K, V]
}

// core holds everything the background goroutines touch, apart from Cache so
// that a dropped Cache can be collected while they shut down.
type core[K comparable, V any] struct {
	shards []*shard[K, V]
	mask   uint64
	seed   maphash.Seed

	ttl          time.Duration
	negativeTTL  time.Duration
	refreshAfter time.Duration
	jitter       int64
	cost         func(V) int64
	onEvict      func(K, V, EvictReason)
	countStats   bool

	loader      func(context.Context, K) (V, error)
	batchLoader func(context.Context, []K) (map[K]V, error)
	flights     []*flightGroup[K, V]

	// getRefresher is refresherWith(loader), worked out once: it is asked on
	// every Get.
	getRefresher refresher

	// views holds the counters of each view name: views of one name share keys,
	// so they share counters too.
	viewsMu sync.Mutex
	views   map[string]*counters

	// coarse is the time the clock goroutine last read, in unix nanoseconds; nil
	// without ClockGranularity, zero once the goroutine has stopped.
	coarse *atomic.Int64

	stop *stopper
}

// New builds a cache from o. It panics on options that cannot describe a working
// cache, such as a byte budget without Cost: a programming mistake is better
// caught at construction than by a cache that silently misbehaves.
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
	case o.RefreshAfter < 0:
		panic("sanecache: RefreshAfter must not be negative")
	case o.RefreshAfter > 0 && (o.TTL == 0 || o.RefreshAfter >= o.TTL):
		panic(fmt.Sprintf("sanecache: RefreshAfter must be shorter than TTL, got %v with TTL %v", o.RefreshAfter, o.TTL))
	}

	n := shardCount(o.Shards)
	cr := &core[K, V]{
		shards:       make([]*shard[K, V], n),
		mask:         uint64(n - 1),
		seed:         maphash.MakeSeed(),
		ttl:          o.TTL,
		negativeTTL:  o.NegativeTTL,
		refreshAfter: o.RefreshAfter,
		jitter:       int64(o.Jitter),
		cost:         o.Cost,
		onEvict:      o.OnEvict,
		countStats:   !o.DisableStats,
		loader:       o.Loader,
		batchLoader:  o.BatchLoader,
	}
	if cr.loader == nil && cr.batchLoader != nil {
		cr.loader = cr.loadAsBatch
	}
	cr.getRefresher = cr.refresherWith(cr.loader)

	perBytes := divideBudget(o.MaxBytes, int64(n))
	perEntries := int(divideBudget(int64(o.MaxEntries), int64(n)))
	for i := range cr.shards {
		cr.shards[i] = newShard[K, V](perBytes, perEntries, o.Policy)
	}
	// Made with or without a loader in Options: GetOrLoadFunc brings its own.
	cr.flights = make([]*flightGroup[K, V], n)
	for i := range cr.flights {
		cr.flights[i] = newFlightGroup[K, V]()
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

// Lookup returns the cached value and how the cache answered. With
// RefreshAfter and a loader in Options, a value due for a refresh starts one.
func (c *Cache[K, V]) Lookup(key K) (V, Status) {
	// One call, so that Get and Lookup still inline into their callers:
	// starting the refresh here would make this too big for that.
	v, st := c.core.read(key)

	return v, st
}

// Set caches value under key for the configured TTL. A successful write prevents
// outstanding loads for the key from publishing over it. Rejected writes do not.
func (c *Cache[K, V]) Set(key K, value V) error {
	return c.SetTTL(key, value, c.core.ttl)
}

// SetTTL caches value under key for ttl, overriding Options.TTL. A ttl of zero
// means the entry never expires on its own.
func (c *Cache[K, V]) SetTTL(key K, value V, ttl time.Duration) error {
	return c.core.setValue(key, value, c.core.valueCost(value), ttl, c.core.refreshAfter, refreshCache)
}

// SetNegative records that the upstream reports no such key, for the configured
// NegativeTTL. A successful write supersedes outstanding loads.
func (c *Cache[K, V]) SetNegative(key K) error {
	return c.core.setNegative(key, c.core.negativeTTL)
}

// SetNegativeTTL is SetNegative with an explicit lifetime.
func (c *Cache[K, V]) SetNegativeTTL(key K, ttl time.Duration) error {
	return c.core.setNegative(key, ttl)
}

// Delete removes key and reports whether it was present. It also invalidates
// outstanding loads for key, even when no entry was present. Existing waiters
// still receive their load result, but it cannot be cached. OnEvict is not called.
func (c *Cache[K, V]) Delete(key K) bool {
	_, ok := c.core.shardFor(key).delete(key)

	return ok
}

// Clear removes entries and invalidates outstanding loads, shard by shard.
// Concurrent new loads and writes can repopulate shards already cleared. Existing
// waiters still receive their load results. OnEvict is not called.
func (c *Cache[K, V]) Clear() {
	for _, s := range c.core.shards {
		s.clear()
	}
}

// Len reports how many entries are held, including expired ones not yet swept.
func (c *Cache[K, V]) Len() int { return c.Stats().Entries }

// Bytes reports the summed cost of the entries held.
func (c *Cache[K, V]) Bytes() int64 { return c.Stats().Bytes }

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

// entryOverhead is an entry and its share of the shard map, apart from the value
// and the bytes of a string key: 115-133 bytes for a Cache[string, any] by
// BenchmarkNegativeEntryMemory, depending on how full the map is.
const entryOverhead int64 = 128

// negativeCost is what a "does not exist" marker is charged under a byte budget:
// no value, but an entry, a map slot and the key, which for a view is a fresh
// string the entry keeps alive.
func negativeCost[K comparable](key K) int64 {
	if s, ok := any(key).(string); ok {
		return entryOverhead + int64(len(s))
	}

	return entryOverhead
}

func (c *core[K, V]) valueCost(v V) int64 {
	if c.cost == nil {
		return 0
	}

	return c.cost(v)
}

// setValue stores a value that expires after ttl. Once it is refreshAfter old, a
// read by owner may refresh it; zero, or anything not shorter than ttl, means
// never.
func (c *core[K, V]) setValue(
	key K, value V, cost int64, ttl, refreshAfter time.Duration, owner refresher, tokens ...*loadToken,
) error {
	e := &entry[K, V]{key: key, value: value, cost: cost, byView: owner == refreshView}
	var refreshAt int64
	e.expiresAt, refreshAt = c.deadlines(ttl, refreshAfter)
	// A fresh entry already holds zero, and an atomic store is not free.
	if refreshAt != 0 {
		e.refreshAt.Store(refreshAt)
	}

	return c.store(e, tokens...)
}

func (c *core[K, V]) setNegative(key K, ttl time.Duration, tokens ...*loadToken) error {
	if ttl <= 0 {
		return ErrNegativeDisabled
	}

	// Charged nothing, it would never be evicted by a byte budget.
	var cost int64
	if c.cost != nil {
		cost = negativeCost(key)
	}

	return c.store(&entry[K, V]{
		key:       key,
		cost:      cost,
		expiresAt: c.expiryAt(ttl),
		negative:  true,
	}, tokens...)
}

func (c *core[K, V]) store(e *entry[K, V], tokens ...*loadToken) error {
	s := c.shardFor(e.key)

	replaced, victims, err := s.set(e, tokens...)
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

// lookup is a read with the shard it landed on, which a view needs in order to
// count a type miss where the rest of that key's counters live, and with whether
// the read, being by, claimed a refresh it now has to start.
func (c *core[K, V]) lookup(key K, by refresher) (V, Status, *shard[K, V], bool) {
	s := c.shardFor(key)
	v, st, wasExpired, refresh := s.get(key, c.now(), by)
	c.countLookup(s, st, wasExpired)

	return v, st, s, refresh
}

// read is lookup for Get and Lookup, starting the refresh it claims with the
// loader in Options. It repeats lookup rather than calling it: neither inlines,
// and the extra call cost 4 ns of a 20 ns hit under a coarse clock.
func (c *core[K, V]) read(key K) (V, Status) {
	s := c.shardFor(key)
	v, st, wasExpired, refresh := s.get(key, c.now(), c.getRefresher)
	c.countLookup(s, st, wasExpired)
	if refresh {
		c.refresh(background, key, c.loader)
	}

	return v, st
}

func (c *core[K, V]) countLookup(s *shard[K, V], st Status, wasExpired bool) {
	if !c.countStats {
		return
	}
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

// refresherWith is who a read is when it would refresh with loader: the cache,
// or nobody when refresh is off or there is nothing to load with.
func (c *core[K, V]) refresherWith(loader func(context.Context, K) (V, error)) refresher {
	if c.refreshAfter > 0 && loader != nil {
		return refreshCache
	}

	return refreshNone
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
	expiresAt, _ := c.deadlines(ttl, 0)

	return expiresAt
}

// deadlines is expiryAt along with the time to refresh. The refresh sits the same
// fraction of the way through the entry's life whatever Jitter made of it, so it
// moves with the expiry and never lands after it.
func (c *core[K, V]) deadlines(ttl, refreshAfter time.Duration) (expiresAt, refreshAt int64) {
	if ttl <= 0 {
		return 0, 0
	}

	life := ttl
	if c.jitter > 0 {
		if span := int64(ttl) * c.jitter / 100; span > 0 {
			life += time.Duration(rand.Int64N(2*span+1) - span)
		}
		// Full jitter can land on zero, which would mean "never expires".
		if life <= 0 {
			life = 1
		}
	}

	now := c.now()
	expiresAt = now + int64(life)
	if refreshAfter > 0 && refreshAfter < ttl {
		// In floating point because the exact product of two durations overflows.
		at := int64(float64(refreshAfter) * (float64(life) / float64(ttl)))
		refreshAt = now + max(at, 1)
	}

	return expiresAt, refreshAt
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
			// Not the tick's own time: after a pause it can be intervals old,
			// and expiry would lag by more than the granularity promises.
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
