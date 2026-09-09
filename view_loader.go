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
	var zero T
	if v.loader == nil {
		return zero, ErrNoLoader
	}
	switch val, st := v.Lookup(key); st {
	case StatusHit:
		return val, nil
	case StatusNegative:
		return zero, ErrNotFound
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}

	return v.load(ctx, key)
}

func (v *View[T]) load(ctx context.Context, key string) (T, error) {
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
			value, err := v.loader(loadCtx, key)
			switch {
			case err == nil:
				_ = c.setValue(fullKey, value, v.valueCost(value), v.ttl, token)
			case errors.Is(err, ErrNotFound):
				_ = c.setNegative(fullKey, v.negativeTTL, token)
			}

			return value, err
		}, func(failed bool) {
			v.count(&v.stats.loads)
			v.count(&s.counters.loads)
			if failed {
				v.count(&v.stats.loadErrors)
				v.count(&s.counters.loadErrors)
			}
		})
	}()

	return g.wait(ctx, key, cl)
}
