package sanecache

import (
	"context"
	"errors"
)

// GetOrLoad returns a typed cached value or calls ViewOptions.Loader. It follows
// Cache.GetOrLoad's error and cancellation policy, storing results with this
// view's Cost, TTL and NegativeTTL under the shared cache's budget. A wrong-type
// entry is a miss and can be replaced by the loaded value.
//
// Without a view loader it returns ErrNoLoader, even for a cached key; the
// underlying cache's loader is never used. Loads coalesce per View instance.
func (v *View[T]) GetOrLoad(ctx context.Context, key string) (T, error) {
	if v.loader == nil {
		var zero T

		return zero, ErrNoLoader
	}

	if val, done, err := v.cached(ctx, key); done {
		return val, err
	}

	return v.loadWith(ctx, key, v.loader)
}

// GetOrLoadFunc is GetOrLoad with the loader passed by the caller instead of
// taken from ViewOptions, as Cache.GetOrLoadFunc is for the cache. Results are
// stored with this view's Cost and TTLs. Loads are shared with this View
// instance's GetOrLoad, so load must produce what the view's loader, or any
// other caller's function, would for that key.
//
// ViewOptions.Loader is not needed. A nil load returns ErrNoLoader. On a view
// with caching switched off, load runs on every call that does not find one
// already running.
func (v *View[T]) GetOrLoadFunc(ctx context.Context, key string, load func(context.Context) (T, error)) (T, error) {
	if load == nil {
		var zero T

		return zero, ErrNoLoader
	}
	if val, done, err := v.cached(ctx, key); done {
		return val, err
	}

	return v.loadWith(ctx, key, func(ctx context.Context, _ string) (T, error) { return load(ctx) })
}

// cached is Cache.cached for a view.
func (v *View[T]) cached(ctx context.Context, key string) (val T, done bool, err error) {
	switch val, st := v.Lookup(key); st {
	case StatusHit:
		return val, true, nil
	case StatusNegative:
		return val, true, ErrNotFound
	}
	if err := ctx.Err(); err != nil {
		return val, true, err
	}

	return val, false, nil
}

func (v *View[T]) loadWith(ctx context.Context, key string, loader func(context.Context, string) (T, error)) (T, error) {
	if v.cache == nil {
		return v.loadUncached(ctx, key, loader)
	}

	return v.load(ctx, key, loader)
}

// loadUncached is load for a view without a cache: callers still share one call
// while it runs, but its result, "does not exist" included, is not kept.
func (v *View[T]) loadUncached(ctx context.Context, key string, loader func(context.Context, string) (T, error)) (T, error) {
	g := v.flights[0]

	g.mu.Lock()
	if cl, ok := g.calls[key]; ok {
		cl.waiters++
		g.mu.Unlock()
		v.count(&v.stats.coalesced)

		return g.wait(ctx, key, cl)
	}
	loadCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	cl := &call[T]{done: make(chan struct{}), waiters: 1, cancel: cancel}
	g.calls[key] = cl
	g.mu.Unlock()

	go g.run(key, cl, func() (T, error) {
		return loader(loadCtx, key)
	}, v.countLoad)

	return g.wait(ctx, key, cl)
}

func (v *View[T]) load(ctx context.Context, key string, loader func(context.Context, string) (T, error)) (T, error) {
	c := v.cache.core
	fullKey := v.prefix + key
	idx := c.shardIndex(fullKey)
	g, s := v.flights[idx], c.shards[idx]
	coalesced := func() {
		v.count(&v.stats.coalesced)
		v.count(&s.counters.coalesced)
	}

	g.mu.Lock()
	if cl, ok := g.calls[key]; ok && cl.token.valid.Load() {
		cl.waiters++
		g.mu.Unlock()
		coalesced()

		return g.wait(ctx, key, cl)
	}
	token := s.beginLoad(fullKey)
	// Recheck without counting another lookup: a load may have published since
	// the caller's miss. A value of a different type still needs a typed load.
	raw, st, _ := s.get(fullKey, c.now())
	val, typed := raw.(T)
	if st == StatusNegative || (st == StatusHit && typed) {
		s.endLoad(fullKey, token)
		g.mu.Unlock()
		coalesced()
		if st == StatusNegative {
			var zero T

			return zero, ErrNotFound
		}

		return val, nil
	}
	loadCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	cl := &call[T]{done: make(chan struct{}), waiters: 1, cancel: cancel, token: token}
	g.calls[key] = cl
	g.mu.Unlock()

	go func() {
		defer s.endLoad(fullKey, token)
		g.run(key, cl, func() (T, error) {
			value, err := loader(loadCtx, key)
			switch {
			case err == nil:
				_ = c.setValue(fullKey, value, v.valueCost(value), v.ttl, token)
			case errors.Is(err, ErrNotFound):
				_ = c.setNegative(fullKey, v.negativeTTL, token)
			}

			return value, err
		}, func(o loadOutcome) {
			v.countLoad(o)
			if v.countStats {
				s.counters.countLoad(o)
			}
		})
	}()

	return g.wait(ctx, key, cl)
}
