package sanecache

import (
	"context"
	"errors"
	"runtime/debug"
	"sync"
	"sync/atomic"
)

// GetManyOrLoad returns the cached values of keys, loading the rest in one
// Options.BatchLoader call, for an upstream that answers many keys at once.
//
// Single flight and invalidation work per key, as in GetOrLoad: a key already
// loading elsewhere is waited for, and a GetOrLoad of a key in the batch waits
// for the batch. A key that does not exist upstream is absent from the result
// and is not an error. Duplicate keys are looked up once.
//
// The error is the first failure in key order; the keys that failed are absent
// and the others present. If ctx ends first, the result holds what was at hand
// and the error is ctx.Err(), while the loads carry on for other waiters.
//
// Without a BatchLoader the missing keys are loaded concurrently with
// Options.Loader; with neither, it returns ErrNoLoader. Keys due for a refresh
// are refreshed in a BatchLoader call of their own, not holding up this one.
func (c *Cache[K, V]) GetManyOrLoad(ctx context.Context, keys []K) (map[K]V, error) {
	cr := c.core
	if cr.loader == nil {
		return nil, ErrNoLoader
	}

	// notHit is only built once there is a key that was not a hit, so that a
	// warm batch costs one map rather than two.
	found := make(map[K]V, len(keys))
	var missing, due []K
	var notHit map[K]struct{}
	by := cr.refresherWith(cr.loader)
	for _, key := range keys {
		if _, dup := found[key]; dup {
			continue
		}
		if _, dup := notHit[key]; dup {
			continue
		}

		v, st, _, refresh := cr.lookup(key, by)
		if refresh {
			due = append(due, key)
		}
		if st == StatusHit {
			found[key] = v

			continue
		}
		if notHit == nil {
			notHit = make(map[K]struct{})
		}
		notHit[key] = struct{}{}
		if st == StatusMiss {
			missing = append(missing, key)
		}
	}
	if len(due) > 0 {
		cr.refreshMany(ctx, due)
	}
	if len(missing) == 0 {
		return found, nil
	}
	if err := ctx.Err(); err != nil {
		return found, err
	}

	waits, fresh, batch := cr.acquire(ctx, missing, found)
	switch {
	case batch != nil:
		go cr.runBatch(batch.ctx, batch.cancel, fresh, false)
	default:
		for _, f := range fresh {
			go cr.run(f.ctx, f.g, f.s, f.key, f.cl, cr.loader, false)
		}
	}

	return found, cr.collect(ctx, waits, found)
}

// flight is one key's share in a load. ctx is set only on flights the caller
// starts itself.
type flight[K comparable, V any] struct {
	key K
	g   *flightGroup[K, V]
	s   *shard[K, V]
	cl  *call[V]
	ctx context.Context
}

// batchLoad is the context of one batch, cancelled once every key in it has
// lost its last waiter.
type batchLoad struct {
	ctx    context.Context
	cancel context.CancelFunc
	live   atomic.Int64
}

// join adds a key to the batch and returns the cancel function of its call.
func (b *batchLoad) join() context.CancelFunc {
	b.live.Add(1)

	return sync.OnceFunc(func() {
		if b.live.Add(-1) == 0 {
			b.cancel()
		}
	})
}

// acquire joins or registers a load for each key, serving from the cache into
// found the keys whose load published since the caller's lookup. It returns the
// flights to wait for, those of them to start, and their batch, if any. A
// call's cancel is set before anyone else can see the call.
func (c *core[K, V]) acquire(ctx context.Context, keys []K, found map[K]V) (waits, fresh []flight[K, V], batch *batchLoad) {
	for _, key := range keys {
		idx := c.shardIndex(key)
		g, s := c.flights[idx], c.shards[idx]

		g.mu.Lock()
		if cl := g.running(key); cl != nil {
			cl.waiters++
			g.mu.Unlock()
			c.countCoalesced(s)
			waits = append(waits, flight[K, V]{key: key, g: g, s: s, cl: cl})

			continue
		}

		// The same recheck as load's.
		token := s.beginLoad(key)
		if v, st, _, _ := s.get(key, c.now(), refreshNone); st != StatusMiss {
			s.endLoad(key, token)
			g.mu.Unlock()
			c.countCoalesced(s)
			if st == StatusHit {
				found[key] = v
			}

			continue
		}

		// Values, but not cancellation, as in startAndUnlock.
		cl := &call[V]{done: make(chan struct{}), waiters: 1, token: token}
		var loadCtx context.Context
		if c.batchLoader != nil {
			if batch == nil {
				batch = &batchLoad{}
				batch.ctx, batch.cancel = context.WithCancel(context.WithoutCancel(ctx))
			}
			loadCtx, cl.cancel = batch.ctx, batch.join()
		} else {
			loadCtx, cl.cancel = context.WithCancel(context.WithoutCancel(ctx))
		}
		g.calls[key] = cl
		g.mu.Unlock()

		f := flight[K, V]{key: key, g: g, s: s, cl: cl, ctx: loadCtx}
		waits = append(waits, f)
		fresh = append(fresh, f)
	}

	return waits, fresh, batch
}

// refreshMany is refresh for several keys, in one BatchLoader call when there is
// a batch loader. Keys whose load is already running are left to it.
func (c *core[K, V]) refreshMany(ctx context.Context, keys []K) {
	if c.batchLoader == nil {
		for _, key := range keys {
			c.refresh(ctx, key, c.loader)
		}

		return
	}

	var fresh []flight[K, V]
	batch := &batchLoad{}
	batch.ctx, batch.cancel = context.WithCancel(context.WithoutCancel(ctx))
	for _, key := range keys {
		idx := c.shardIndex(key)
		g, s := c.flights[idx], c.shards[idx]

		g.mu.Lock()
		if g.running(key) != nil {
			g.mu.Unlock()

			continue
		}
		// One waiter that never leaves, as in refresh.
		cl := &call[V]{done: make(chan struct{}), waiters: 1, token: s.beginLoad(key), cancel: batch.join()}
		g.calls[key] = cl
		g.mu.Unlock()

		fresh = append(fresh, flight[K, V]{key: key, g: g, s: s, cl: cl, ctx: batch.ctx})
	}
	if len(fresh) == 0 {
		batch.cancel()

		return
	}

	go c.runBatch(batch.ctx, batch.cancel, fresh, true)
}

// runBatch is run for a batch.
func (c *core[K, V]) runBatch(ctx context.Context, cancel context.CancelFunc, fresh []flight[K, V], refresh bool) {
	defer cancel()
	defer func() {
		for _, f := range fresh {
			f.s.endLoad(f.key, f.cl.token)
		}
	}()

	// Panics are carried and counted as in flightGroup.run.
	defer func() {
		if r := recover(); r != nil {
			pan := &loaderPanic{value: r, stack: debug.Stack()}
			for _, f := range fresh {
				f.cl.pan = pan
			}
		}
		if c.countStats {
			fresh[0].s.counters.batches.Add(1)
			for _, f := range fresh {
				f.s.counters.countLoad(outcomeOf(f.cl.err, f.cl.pan), refresh)
			}
		}
		for _, f := range fresh {
			f.g.finish(f.key, f.cl)
		}
	}()

	keys := make([]K, len(fresh))
	for i, f := range fresh {
		keys[i] = f.key
	}
	values, err := c.batchLoader(ctx, keys)

	for _, f := range fresh {
		v, ok := values[f.key]
		switch {
		case err != nil:
			// Every key fails, even those the map holds: part of an answer
			// from a failed call is not worth caching.
			f.cl.err = err
			if errors.Is(err, ErrNotFound) {
				_ = c.setNegative(f.key, c.negativeTTL, f.cl.token)
			}
		case ok:
			f.cl.val = v
			_ = c.setValue(f.key, v, c.valueCost(v), c.ttl, c.refreshAfter, refreshCache, f.cl.token)
		default:
			f.cl.err = ErrNotFound
			_ = c.setNegative(f.key, c.negativeTTL, f.cl.token)
		}
	}
}

// collect waits for every flight, gathering results into found. If ctx ends
// first it leaves the rest, so that loads nobody waits for can be cancelled.
func (c *core[K, V]) collect(ctx context.Context, waits []flight[K, V], found map[K]V) error {
	var firstErr error
	for i, f := range waits {
		select {
		case <-f.cl.done:
		case <-ctx.Done():
			for _, rest := range waits[i:] {
				rest.g.leave(rest.key, rest.cl)
			}

			return ctx.Err()
		}

		if f.cl.pan != nil {
			for _, rest := range waits[i+1:] {
				rest.g.leave(rest.key, rest.cl)
			}
			panic(f.cl.pan)
		}

		switch {
		case f.cl.err == nil:
			found[f.key] = f.cl.val
		case errors.Is(f.cl.err, ErrNotFound):
		case firstErr == nil:
			firstErr = f.cl.err
		}
	}

	return firstErr
}

// loadAsBatch is GetOrLoad's loader when there is only a BatchLoader.
func (c *core[K, V]) loadAsBatch(ctx context.Context, key K) (V, error) {
	if c.countStats {
		c.shardFor(key).counters.batches.Add(1)
	}

	values, err := c.batchLoader(ctx, []K{key})
	if err != nil {
		var zero V

		return zero, err
	}
	v, ok := values[key]
	if !ok {
		return v, ErrNotFound
	}

	return v, nil
}
