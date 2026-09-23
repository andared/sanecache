package sanecache

import (
	"context"
	"errors"
	"runtime/debug"
	"sync"
	"sync/atomic"
)

// GetManyOrLoad returns the cached values of keys, loading the rest in one
// Options.BatchLoader call. It is GetOrLoad for callers that would otherwise
// fetch keys one at a time from an upstream that answers many at once, such as
// a database query with IN or a pipelined round trip.
//
// Each missing key takes part in single flight on its own: a key that some other
// GetOrLoad or GetManyOrLoad is already loading is waited for rather than loaded
// again, and a GetOrLoad arriving while the batch runs waits for the batch.
// Invalidation works per key, exactly as for GetOrLoad: a Delete or a successful
// write of one key keeps the batch's result for that key out of the cache and
// leaves the others alone.
//
// The result holds every key that has a value. A key the upstream does not have
// is absent from it, whether that answer came from the loader or from a cached
// negative entry; it is not an error. Duplicate keys are looked up once.
//
// The error is the first failure among the keys, in the order they were given,
// and the keys that failed are absent from the result; the others are still
// there. Failures are not cached. If ctx ends first, the result holds what was
// already at hand and the error is ctx.Err(); the loads carry on for any other
// caller waiting for them. A loader panic reaches this caller as it would
// through GetOrLoad.
//
// Without a BatchLoader the missing keys are loaded concurrently with
// Options.Loader, one call per key. With neither, it returns ErrNoLoader.
func (c *Cache[K, V]) GetManyOrLoad(ctx context.Context, keys []K) (map[K]V, error) {
	cr := c.core
	if cr.loader == nil {
		return nil, ErrNoLoader
	}

	// A warm batch is all hits, and found alone is enough to skip their
	// duplicates; the set of keys that were not hits is only built once there is
	// one, so that the common case costs one map rather than two.
	found := make(map[K]V, len(keys))
	var missing []K
	var notHit map[K]struct{}
	for _, key := range keys {
		if _, dup := found[key]; dup {
			continue
		}
		if _, dup := notHit[key]; dup {
			continue
		}

		v, st := c.Lookup(key)
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
	if len(missing) == 0 {
		return found, nil
	}
	if err := ctx.Err(); err != nil {
		return found, err
	}

	waits, fresh, batch := cr.acquire(ctx, missing, found)
	switch {
	case batch != nil:
		go cr.runBatch(batch.ctx, batch.cancel, fresh)
	default:
		for _, f := range fresh {
			go cr.run(f.ctx, f.g, f.s, f.key, f.cl)
		}
	}

	return found, cr.collect(ctx, waits, found)
}

// flight is one key's share in a load: the call its waiters are parked on and
// where that call lives. ctx is the load's context, set only on the flights the
// caller starts itself.
type flight[K comparable, V any] struct {
	key K
	g   *flightGroup[K, V]
	s   *shard[K, V]
	cl  *call[V]
	ctx context.Context
}

// batchLoad is the context shared by every key of one batch. It is cancelled
// once every key in it has lost its last waiter: until then somebody still wants
// part of the answer, and the upstream call is the same call whichever part
// that is.
type batchLoad struct {
	ctx    context.Context
	cancel context.CancelFunc
	live   atomic.Int64
}

// join adds a key to the batch and returns the cancel function for that key's
// call, which leave invokes when the key's last waiter goes.
func (b *batchLoad) join() context.CancelFunc {
	b.live.Add(1)

	return sync.OnceFunc(func() {
		if b.live.Add(-1) == 0 {
			b.cancel()
		}
	})
}

// acquire joins or registers a load for each key. Keys whose load published
// between the caller's lookup and here are served from the cache instead, into
// found. It returns every flight to wait for and, among them, the ones this
// caller has to start, along with their batch when there is a batch loader.
//
// A call's cancel is in place before the call is visible to anyone else, as in
// load: a caller that joins and then gives up may be the one to invoke it. It
// cannot bring a fresh call's waiters to zero on its own, though, since this
// caller is one of them until collect.
func (c *core[K, V]) acquire(ctx context.Context, keys []K, found map[K]V) (waits, fresh []flight[K, V], batch *batchLoad) {
	for _, key := range keys {
		idx := c.shardIndex(key)
		g, s := c.flights[idx], c.shards[idx]

		g.mu.Lock()
		if cl, ok := g.calls[key]; ok && cl.token.valid.Load() {
			cl.waiters++
			g.mu.Unlock()
			c.countCoalesced(s)
			waits = append(waits, flight[K, V]{key: key, g: g, s: s, cl: cl})

			continue
		}

		// The same recheck as load's, for the same reason: without it a caller
		// descheduled after its lookup would load a value already cached.
		token := s.beginLoad(key)
		if v, st, _ := s.get(key, c.now()); st != StatusMiss {
			s.endLoad(key, token)
			g.mu.Unlock()
			c.countCoalesced(s)
			if st == StatusHit {
				found[key] = v
			}

			continue
		}

		// The loads outlive this caller, so they do not inherit its
		// cancellation: values yes, deadline no. See call.waiters.
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

// runBatch performs one batch load and publishes each key's result, the way run
// does for one key.
func (c *core[K, V]) runBatch(ctx context.Context, cancel context.CancelFunc, fresh []flight[K, V]) {
	defer cancel()
	defer func() {
		for _, f := range fresh {
			f.s.endLoad(f.key, f.cl.token)
		}
	}()

	// Recovered and counted on the way out for the same reasons as run: the
	// batch does not run on any caller's goroutine, and a load that panicked is
	// still a load that failed.
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
				f.s.counters.loads.Add(1)
				if f.cl.err != nil || f.cl.pan != nil {
					f.s.counters.loadErrors.Add(1)
				}
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
			// A failed batch fails every key in it, including any the loader
			// managed to fill in: part of an answer from a call that reported an
			// error is not something to cache.
			f.cl.err = err
			if errors.Is(err, ErrNotFound) {
				_ = c.setNegative(f.key, c.negativeTTL, f.cl.token)
			}
		case ok:
			f.cl.val = v
			_ = c.setValue(f.key, v, c.valueCost(v), c.ttl, f.cl.token)
		default:
			f.cl.err = ErrNotFound
			_ = c.setNegative(f.key, c.negativeTTL, f.cl.token)
		}
	}
}

// collect waits for every flight and gathers the results into found. If ctx
// ends first, the flights not yet collected are left, so that a load nobody
// waits for any more can be cancelled.
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

// loadAsBatch is the loader GetOrLoad uses when only a BatchLoader was given: a
// batch of one, so that a cache needs a single loader for both paths.
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
