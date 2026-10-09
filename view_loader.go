package sanecache

import (
	"context"
	"errors"
)

// GetOrLoad is Cache.GetOrLoad with ViewOptions.Loader, storing with this view's
// Cost and TTLs; an entry of another type is a miss the load replaces. Without
// a view loader it returns ErrNoLoader, even for a cached key.
func (v *View[T]) GetOrLoad(ctx context.Context, key string) (T, error) {
	if v.loader == nil {
		var zero T

		return zero, ErrNoLoader
	}

	if val, done, err := v.cached(ctx, key, v.loader); done {
		return val, err
	}

	return v.loadWith(ctx, key, v.loader)
}

// GetOrLoadFunc is Cache.GetOrLoadFunc for this view, sharing loads with its
// GetOrLoad. A nil load returns ErrNoLoader.
func (v *View[T]) GetOrLoadFunc(ctx context.Context, key string, load func(context.Context) (T, error)) (T, error) {
	if load == nil {
		var zero T

		return zero, ErrNoLoader
	}
	loader := func(ctx context.Context, _ string) (T, error) { return load(ctx) }
	if val, done, err := v.cached(ctx, key, loader); done {
		return val, err
	}

	return v.loadWith(ctx, key, loader)
}

// cached is Cache.cached for a view.
func (v *View[T]) cached(ctx context.Context, key string, loader func(context.Context, string) (T, error)) (T, bool, error) {
	val, st := v.lookup(ctx, key, v.refresherWith(loader), loader)

	return answered(ctx, val, st)
}

func (v *View[T]) loadWith(ctx context.Context, key string, loader func(context.Context, string) (T, error)) (T, error) {
	if v.cache == nil {
		return v.loadUncached(ctx, key, loader)
	}

	return v.load(ctx, key, loader)
}

// loadUncached is load for a view without a cache: shared, but not kept.
func (v *View[T]) loadUncached(ctx context.Context, key string, loader func(context.Context, string) (T, error)) (T, error) {
	g := v.flights[0]

	g.mu.Lock()
	if cl := g.running(key); cl != nil {
		cl.waiters++
		g.mu.Unlock()
		v.count(&v.stats.coalesced)

		return g.wait(ctx, key, cl)
	}
	cl := g.startAndUnlock(ctx, key, nil, func(ctx context.Context, cl *call[T]) {
		g.run(key, cl, func() (T, error) { return loader(ctx, key) }, func(o loadOutcome) { v.stats.countLoad(o, false) })
	})

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
	if cl := g.running(key); cl != nil {
		cl.waiters++
		g.mu.Unlock()
		coalesced()

		return g.wait(ctx, key, cl)
	}
	token := s.beginLoad(fullKey)
	// load's recheck; a value of another type still needs loading.
	raw, st, _, _ := s.get(fullKey, c.now(), refreshNone)
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
	cl := g.startAndUnlock(ctx, key, token, func(ctx context.Context, cl *call[T]) {
		v.run(ctx, g, s, key, cl, loader, false)
	})

	return g.wait(ctx, key, cl)
}

// refresh is Cache's refresh for a view's key, with the view's loader.
func (v *View[T]) refresh(ctx context.Context, key string, loader func(context.Context, string) (T, error)) {
	c := v.cache.core
	fullKey := v.prefix + key
	idx := c.shardIndex(fullKey)
	g, s := v.flights[idx], c.shards[idx]

	g.mu.Lock()
	if g.running(key) != nil {
		g.mu.Unlock()

		return
	}
	g.startAndUnlock(ctx, key, s.beginLoad(fullKey), func(ctx context.Context, cl *call[T]) {
		v.run(ctx, g, s, key, cl, loader, true)
	})
}

// run is Cache's run for a view's key, counted for the view too.
func (v *View[T]) run(
	ctx context.Context, g *flightGroup[string, T], s *shard[string, any], key string, cl *call[T],
	loader func(context.Context, string) (T, error), refresh bool,
) {
	c := v.cache.core
	fullKey := v.prefix + key
	defer s.endLoad(fullKey, cl.token)
	g.run(key, cl, func() (T, error) {
		value, err := loader(ctx, key)
		switch {
		case err == nil:
			_ = c.setValue(fullKey, value, v.valueCost(value), v.ttl, v.refreshAfter, refreshView, cl.token)
		case errors.Is(err, ErrNotFound):
			_ = c.setNegative(fullKey, v.negativeTTL, cl.token)
		}

		return value, err
	}, func(o loadOutcome) {
		if v.countStats {
			v.stats.countLoad(o, refresh)
			s.counters.countLoad(o, refresh)
		}
	})
}
