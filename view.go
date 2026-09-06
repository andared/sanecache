package sanecache

import (
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)

// viewSeparator joins a view's name to the caller's key. Names may not contain
// it, which is what makes the join unambiguous: "article:1:2" can only be key
// "1:2" in view "article".
const viewSeparator = ":"

// ViewOptions configures a view. Only Name is required.
type ViewOptions[T any] struct {
	// Name identifies the view and namespaces its keys: the view stores under
	// Name + ":" + key. Two views therefore cannot collide, and an OnEvict
	// handler on the cache can tell whose entry it is looking at. It must not be
	// empty and must not contain ":".
	Name string

	// Cost reports the memory a value of this view occupies, in bytes, the way
	// Options.Cost does for the cache as a whole. A view that sets it is spared
	// the type switch that a shared Cost func(any) int64 turns into once a
	// budget holds several types. Unset, the cache's own Cost is used.
	Cost func(T) int64

	// TTL is how long this view's values stay valid. Zero takes the cache's TTL;
	// SetTTL still overrides both.
	//
	// A view with a much shorter TTL than the cache is worth a word of warning:
	// the sweeper's interval is chosen from the cache's TTLs, so those entries
	// may sit on the budget after expiring until a lookup or a sweep finds them.
	// Set Options.CleanupInterval when that matters.
	TTL time.Duration

	// NegativeTTL is how long this view's "does not exist" answers live. Zero
	// takes the cache's NegativeTTL, and if that is unset too, SetNegative on
	// this view reports ErrNegativeDisabled.
	NegativeTTL time.Duration
}

// View is a typed window onto a cache that holds values of several types under
// one byte budget. The cache is declared as Cache[string, any]; each view fixes
// one value type, namespaces its keys, and counts its own hits.
//
// This is the shape that a budget in bytes forces. One cache per type would mean
// one budget per type, and splitting a fixed amount of memory between types up
// front is exactly the guess the byte budget was meant to avoid: the split that
// was right at deploy time is wrong by the next traffic pattern. A view is a
// function rather than a method on Cache because a method cannot introduce a
// type parameter of its own.
//
// A view costs one string join per operation on top of the cache underneath.
// While the name and key together fit in 32 bytes the compiler keeps that on the
// stack; past it, every read allocates.
type View[T any] struct {
	cache       *Cache[string, any]
	name        string
	prefix      string
	cost        func(T) int64
	ttl         time.Duration
	negativeTTL time.Duration
	countStats  bool

	stats viewCounters
}

// NewView opens a view named o.Name onto c. It panics on a name that cannot keep
// views apart, for the same reason New panics on a budget it cannot honour.
func NewView[T any](c *Cache[string, any], o ViewOptions[T]) *View[T] {
	switch {
	case c == nil:
		panic("sanecache: NewView needs a cache")
	case o.Name == "":
		panic("sanecache: ViewOptions.Name must not be empty; the name is what keeps views apart")
	case strings.Contains(o.Name, viewSeparator):
		panic(fmt.Sprintf("sanecache: ViewOptions.Name must not contain %q, got %q", viewSeparator, o.Name))
	case o.TTL < 0 || o.NegativeTTL < 0:
		panic("sanecache: ViewOptions TTL and NegativeTTL must not be negative")
	}

	v := &View[T]{
		cache:       c,
		name:        o.Name,
		prefix:      o.Name + viewSeparator,
		cost:        o.Cost,
		ttl:         o.TTL,
		negativeTTL: o.NegativeTTL,
		countStats:  c.core.countStats,
	}
	if v.ttl == 0 {
		v.ttl = c.core.ttl
	}
	if v.negativeTTL == 0 {
		v.negativeTTL = c.core.negativeTTL
	}

	return v
}

// Name returns the view's name, which is also the prefix its keys carry in the
// underlying cache.
func (v *View[T]) Name() string { return v.name }

// Get returns the cached value. As with Cache.Get, a cached "does not exist"
// answer reports false; use Lookup to tell it from a miss.
func (v *View[T]) Get(key string) (T, bool) {
	val, st := v.Lookup(key)

	return val, st == StatusHit
}

// Lookup returns the cached value and how the view answered. An entry holding
// some other type is reported as a miss and counted as a TypeMiss: the value is
// unusable here, so the caller has to go to the upstream either way.
func (v *View[T]) Lookup(key string) (T, Status) {
	var zero T

	raw, st, shard := v.cache.core.lookup(v.prefix + key)
	switch st {
	case StatusHit:
		val, ok := raw.(T)
		if !ok {
			v.count(&v.stats.typeMisses)
			v.count(&shard.counters.typeMisses)

			return zero, StatusMiss
		}
		v.count(&v.stats.hits)

		return val, StatusHit

	case StatusNegative:
		v.count(&v.stats.negatives)

		return zero, StatusNegative

	default:
		v.count(&v.stats.misses)

		return zero, StatusMiss
	}
}

// Set caches value under key for the view's TTL.
func (v *View[T]) Set(key string, value T) error {
	return v.SetTTL(key, value, v.ttl)
}

// SetTTL caches value under key for ttl, overriding both the view's and the
// cache's TTL. A ttl of zero means the entry never expires on its own.
func (v *View[T]) SetTTL(key string, value T, ttl time.Duration) error {
	return v.cache.core.setValue(v.prefix+key, value, v.valueCost(value), ttl)
}

// SetNegative records that the upstream reports no such key, for the view's
// NegativeTTL.
func (v *View[T]) SetNegative(key string) error {
	return v.cache.core.setNegative(v.prefix+key, v.negativeTTL)
}

// SetNegativeTTL is SetNegative with an explicit lifetime.
func (v *View[T]) SetNegativeTTL(key string, ttl time.Duration) error {
	return v.cache.core.setNegative(v.prefix+key, ttl)
}

// Delete removes key from this view and reports whether it was present.
func (v *View[T]) Delete(key string) bool {
	return v.cache.Delete(v.prefix + key)
}

// Stats returns a snapshot of this view's counters. The cache's own Stats counts
// the same lookups across every view.
func (v *View[T]) Stats() ViewStats {
	return ViewStats{
		Hits:       v.stats.hits.Load(),
		Misses:     v.stats.misses.Load(),
		Negatives:  v.stats.negatives.Load(),
		TypeMisses: v.stats.typeMisses.Load(),
	}
}

func (v *View[T]) valueCost(value T) int64 {
	if v.cost != nil {
		return v.cost(value)
	}

	return v.cache.core.valueCost(value)
}

func (v *View[T]) count(c *atomic.Int64) {
	if v.countStats {
		c.Add(1)
	}
}

// ViewStats is a snapshot of one view's counters, cumulative since the view was
// opened.
type ViewStats struct {
	Hits      int64 // lookups that returned a value of this view's type
	Misses    int64 // lookups that found nothing
	Negatives int64 // lookups that found a cached "does not exist"

	// TypeMisses counts lookups that found an entry holding another type. It is
	// a bug detector rather than a routine metric: with keys namespaced by view
	// name, the only ways to get one are two views sharing a name and writes
	// made straight to the underlying cache.
	TypeMisses int64
}

// HitRate reports hits as a fraction of all lookups, counting a cached negative
// as a hit and a type miss as a miss.
func (s ViewStats) HitRate() float64 {
	total := s.Hits + s.Misses + s.Negatives + s.TypeMisses
	if total == 0 {
		return 0
	}

	return float64(s.Hits+s.Negatives) / float64(total)
}

type viewCounters struct {
	hits       atomic.Int64
	misses     atomic.Int64
	negatives  atomic.Int64
	typeMisses atomic.Int64
}
