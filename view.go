package sanecache

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)

// viewSeparator joins a view's name to the caller's key. Names may not contain
// it, so "article:1:2" can only be key "1:2" in view "article".
const viewSeparator = ":"

// ViewOptions configures a view. Only Name is required.
type ViewOptions[T any] struct {
	// Name namespaces the view's keys: it stores under Name + ":" + key. It must
	// not be empty or contain ":".
	Name string

	// Loader fetches an uncached value by the caller's key, without the prefix,
	// under Options.Loader's rules. Loads coalesce within one View instance, not
	// across views of the same name or with the cache's own loads. A loader must
	// not load its own key through this view.
	Loader func(context.Context, string) (T, error)

	// Cost is Options.Cost for this view's values, sparing the cache's Cost a type
	// switch. Unset, the cache's Cost is used.
	Cost func(T) int64

	// TTL is how long this view's values stay valid; zero takes the cache's. The
	// sweep interval follows the cache's TTLs, so with a much shorter TTL here,
	// set Options.CleanupInterval.
	TTL time.Duration

	// NegativeTTL is how long this view's "does not exist" answers live; zero
	// takes the cache's.
	NegativeTTL time.Duration

	// RefreshAfter is Options.RefreshAfter for this view, with its own loaders.
	// It must be shorter than the view's TTL. Zero means no refresh: the cache's
	// RefreshAfter is not inherited and never applies to a view's values.
	RefreshAfter time.Duration
}

// View is a typed window onto a Cache[string, any] that holds several value types
// under one byte budget: one cache per type would mean splitting the memory
// between types up front. Each view fixes a type, namespaces its keys and counts
// its own hits. It costs a string join per operation, which allocates once name
// and key exceed 32 bytes.
type View[T any] struct {
	cache        *Cache[string, any]
	name         string
	prefix       string
	cost         func(T) int64
	ttl          time.Duration
	negativeTTL  time.Duration
	refreshAfter time.Duration
	countStats   bool

	// getRefresher is refresherWith(loader), worked out once.
	getRefresher refresher

	loader  loadFunc[string, T]
	flights []*flightGroup[string, T]

	// stats is shared by every view of this name on this cache.
	stats *counters
}

// NewView opens a view named o.Name onto c. It panics on invalid options, as New
// does.
//
// A nil c switches caching off, so that code can keep calling GetOrLoad: the
// loader runs on every call, still shared by concurrent callers, and nothing is
// remembered. Lookups miss, Delete reports false, and writes fail with
// ErrDisabled.
func NewView[T any](c *Cache[string, any], o ViewOptions[T]) *View[T] {
	switch {
	case o.Name == "":
		panic("sanecache: ViewOptions.Name must not be empty; the name is what keeps views apart")
	case strings.Contains(o.Name, viewSeparator):
		panic(fmt.Sprintf("sanecache: ViewOptions.Name must not contain %q, got %q", viewSeparator, o.Name))
	case o.TTL < 0 || o.NegativeTTL < 0:
		panic("sanecache: ViewOptions TTL and NegativeTTL must not be negative")
	case o.RefreshAfter < 0:
		panic("sanecache: ViewOptions.RefreshAfter must not be negative")
	}

	if c == nil {
		v := &View[T]{
			loader:     unsized(o.Loader),
			name:       o.Name,
			prefix:     o.Name + viewSeparator,
			countStats: true,
			stats:      new(counters),
			flights:    []*flightGroup[string, T]{newFlightGroup[string, T]()},
		}

		return v
	}

	v := &View[T]{
		cache:       c,
		loader:      unsized(o.Loader),
		name:        o.Name,
		prefix:      o.Name + viewSeparator,
		cost:        o.Cost,
		ttl:         o.TTL,
		negativeTTL: o.NegativeTTL,
		countStats:  c.core.countStats,
		stats:       c.core.viewCounters(o.Name),
	}
	if v.ttl == 0 {
		v.ttl = c.core.ttl
	}
	if v.negativeTTL == 0 {
		v.negativeTTL = c.core.negativeTTL
	}
	if o.RefreshAfter > 0 && (v.ttl == 0 || o.RefreshAfter >= v.ttl) {
		panic(fmt.Sprintf("sanecache: ViewOptions.RefreshAfter must be shorter than the view's TTL, got %v with TTL %v",
			o.RefreshAfter, v.ttl))
	}
	v.refreshAfter = o.RefreshAfter
	v.getRefresher = v.refresherWith(v.loader)

	// Made with or without a view loader: GetOrLoadFunc brings its own.
	v.flights = make([]*flightGroup[string, T], len(c.core.shards))
	for i := range v.flights {
		v.flights[i] = newFlightGroup[string, T]()
	}

	return v
}

// Name returns the view's name, the prefix of its keys in the cache.
func (v *View[T]) Name() string { return v.name }

// Get returns the cached value. As with Cache.Get, a cached "does not exist"
// answer reports false; use Lookup to tell it from a miss.
func (v *View[T]) Get(key string) (T, bool) {
	val, st := v.Lookup(key)

	return val, st == StatusHit
}

// Lookup returns the cached value and how the view answered. An entry of another
// type is a miss, counted as a TypeMiss. With RefreshAfter and a Loader, a value
// due for a refresh starts one.
func (v *View[T]) Lookup(key string) (T, Status) {
	// Not context.Background(): that call would keep this from inlining.
	return v.lookup(background, key, v.getRefresher, v.loader)
}

// background is the context of refreshes that Get and Lookup start.
var background = context.Background()

// lookup is Lookup as reader by, refreshing with loader.
func (v *View[T]) lookup(
	ctx context.Context, key string, by refresher, loader loadFunc[string, T],
) (T, Status) {
	var zero T

	if v.cache == nil {
		v.count(&v.stats.misses)

		return zero, StatusMiss
	}

	raw, st, shard, refresh := v.cache.core.lookup(v.prefix+key, by)
	switch st {
	case StatusHit:
		val, ok := raw.(T)
		if !ok {
			v.count(&v.stats.typeMisses)
			v.count(&shard.counters.typeMisses)

			return zero, StatusMiss
		}
		v.count(&v.stats.hits)
		if refresh {
			v.refresh(ctx, key, loader)
		}

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

// SetTTL caches value under key for ttl; zero means it never expires.
func (v *View[T]) SetTTL(key string, value T, ttl time.Duration) error {
	if v.cache == nil {
		return ErrDisabled
	}

	return v.cache.core.setValue(v.prefix+key, value, v.valueCost(value), ttl, v.refreshAfter, refreshView)
}

// SetWithCost is Cache.SetWithCost for this view, with the view's TTL.
func (v *View[T]) SetWithCost(key string, value T, cost int64) error {
	if v.cache == nil {
		return ErrDisabled
	}

	return v.cache.core.setValue(v.prefix+key, value, max(cost, 0), v.ttl, v.refreshAfter, refreshView)
}

// SetNegative records that the upstream reports no such key, for the view's
// NegativeTTL.
func (v *View[T]) SetNegative(key string) error {
	return v.SetNegativeTTL(key, v.negativeTTL)
}

// SetNegativeTTL is SetNegative with an explicit lifetime.
func (v *View[T]) SetNegativeTTL(key string, ttl time.Duration) error {
	if v.cache == nil {
		return ErrDisabled
	}

	return v.cache.core.setNegative(v.prefix+key, ttl)
}

// Delete is Cache.Delete for this view's key, invalidating its loads in any view
// or the cache.
func (v *View[T]) Delete(key string) bool {
	if v.cache == nil {
		return false
	}

	return v.cache.Delete(v.prefix + key)
}

// Stats returns the counters of this view's name, shared by every view of that
// name on the cache. The cache's Stats counts the same lookups too.
func (v *View[T]) Stats() ViewStats {
	return v.stats.viewStats()
}

// ViewStats returns the counters of every view opened on the cache, by name, so
// that an exporter needs no registry of its own.
func (c *Cache[K, V]) ViewStats() map[string]ViewStats {
	c.core.viewsMu.Lock()
	defer c.core.viewsMu.Unlock()

	stats := make(map[string]ViewStats, len(c.core.views))
	for name, counters := range c.core.views {
		stats[name] = counters.viewStats()
	}

	return stats
}

// viewCounters returns the counters of a view name, creating them on first use.
func (c *core[K, V]) viewCounters(name string) *counters {
	c.viewsMu.Lock()
	defer c.viewsMu.Unlock()

	if c.views == nil {
		c.views = make(map[string]*counters)
	}
	cs, ok := c.views[name]
	if !ok {
		cs = new(counters)
		c.views[name] = cs
	}

	return cs
}

// refresherWith is who a read of this view is when it would refresh with loader.
func (v *View[T]) refresherWith(loader loadFunc[string, T]) refresher {
	if v.refreshAfter > 0 && loader != nil {
		return refreshView
	}

	return refreshNone
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

// ViewStats is a snapshot of the counters of one view name.
type ViewStats struct {
	Hits      int64 // lookups that returned a value of this view's type
	Misses    int64 // lookups that found nothing
	Negatives int64 // lookups that found a cached "does not exist"

	// TypeMisses counts lookups that found another type: a bug detector, since
	// only two views of one name or writes straight to the cache cause them.
	TypeMisses int64

	Loads        int64 // completed loader calls, including errors and panics
	LoadNotFound int64 // completed loader calls that returned ErrNotFound
	LoadErrors   int64 // completed loader calls that failed otherwise, or panicked
	Coalesced    int64 // calls spared a load by another caller

	Refreshes     int64 // completed refreshes started by RefreshAfter
	RefreshErrors int64 // refreshes that failed or panicked
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

func (c *counters) viewStats() ViewStats {
	return ViewStats{
		Hits:          c.hits.Load(),
		Misses:        c.misses.Load(),
		Negatives:     c.negatives.Load(),
		TypeMisses:    c.typeMisses.Load(),
		Loads:         c.loads.Load(),
		LoadNotFound:  c.loadNotFound.Load(),
		LoadErrors:    c.loadErrors.Load(),
		Coalesced:     c.coalesced.Load(),
		Refreshes:     c.refreshes.Load(),
		RefreshErrors: c.refreshErrors.Load(),
	}
}
