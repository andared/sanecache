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
	if c.core.loader == nil {
		return nil, ErrNoLoader
	}

	return getMany[K, V](ctx, c.core, keys)
}

// batchHost is what batch loading needs from a cache or a view: the cache
// stores a key as it is, a view under its prefix and as its own type. Loads
// are tracked by the caller's key in the host's flight groups and stored by
// skey, the storage key, in shard idx.
type batchHost[K comparable, V any] interface {
	// batchLookup is a counted read that reports, rather than starts, the
	// refresh it claims.
	batchLookup(key K) (v V, st Status, refresh bool)
	slot(key K) (skey K, idx uint64, g *flightGroup[K, V])
	beginLoad(idx uint64, skey K) *loadToken
	endLoad(idx uint64, skey K, token *loadToken)
	// recheck is load's second look, under the group lock and not counted.
	recheck(idx uint64, skey K) (V, Status)
	// publish caches a loaded value, or a negative entry for ErrNotFound.
	publish(idx uint64, skey K, v V, err error, token *loadToken)
	// countersAt returns where loads in shard idx are counted, nil for none.
	countersAt(idx uint64) [2]*counters
	canBatch() bool
	loadMany(ctx context.Context, keys []K) (map[K]V, error)
	// runOne loads f's key alone, for a host without a batch loader.
	runOne(f flight[K, V])
	refreshOne(ctx context.Context, key K)
}

// getMany is GetManyOrLoad for any host with a loader.
//
// notHit is only built once there is a key that was not a hit, so that a warm
// batch costs one map rather than two.
func getMany[K comparable, V any](ctx context.Context, h batchHost[K, V], keys []K) (map[K]V, error) {
	found := make(map[K]V, len(keys))
	var missing, due []K
	var notHit map[K]struct{}
	for _, key := range keys {
		if _, dup := found[key]; dup {
			continue
		}
		if _, dup := notHit[key]; dup {
			continue
		}

		v, st, refresh := h.batchLookup(key)
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
		refreshMany(ctx, h, due)
	}
	if len(missing) == 0 {
		return found, nil
	}
	if err := ctx.Err(); err != nil {
		return found, err
	}

	waits, fresh, batch := acquire(ctx, h, missing, found)
	switch {
	case batch != nil:
		go runBatch(h, batch.ctx, batch.cancel, fresh, false)
	default:
		for _, f := range fresh {
			go h.runOne(f)
		}
	}

	return found, collect(ctx, waits, found)
}

// flight is one key's share in a load. ctx is set only on flights the caller
// starts itself.
type flight[K comparable, V any] struct {
	key  K
	skey K
	idx  uint64
	g    *flightGroup[K, V]
	cl   *call[V]
	ctx  context.Context
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
func acquire[K comparable, V any](
	ctx context.Context, h batchHost[K, V], keys []K, found map[K]V,
) (waits, fresh []flight[K, V], batch *batchLoad) {
	for _, key := range keys {
		skey, idx, g := h.slot(key)

		g.mu.Lock()
		if cl := g.running(key); cl != nil {
			cl.waiters++
			g.mu.Unlock()
			countCoalesced(h, idx)
			waits = append(waits, flight[K, V]{key: key, skey: skey, idx: idx, g: g, cl: cl})

			continue
		}

		// The same recheck as load's.
		token := h.beginLoad(idx, skey)
		if v, st := h.recheck(idx, skey); st != StatusMiss {
			h.endLoad(idx, skey, token)
			g.mu.Unlock()
			countCoalesced(h, idx)
			if st == StatusHit {
				found[key] = v
			}

			continue
		}

		// Values, but not cancellation, as in startAndUnlock.
		cl := &call[V]{done: make(chan struct{}), waiters: 1, token: token}
		var loadCtx context.Context
		if h.canBatch() {
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

		f := flight[K, V]{key: key, skey: skey, idx: idx, g: g, cl: cl, ctx: loadCtx}
		waits = append(waits, f)
		fresh = append(fresh, f)
	}

	return waits, fresh, batch
}

func countCoalesced[K comparable, V any](h batchHost[K, V], idx uint64) {
	for _, c := range h.countersAt(idx) {
		if c != nil {
			c.coalesced.Add(1)
		}
	}
}

// refreshMany is refresh for several keys, in one batch loader call when there
// is a batch loader. Keys whose load is already running are left to it.
func refreshMany[K comparable, V any](ctx context.Context, h batchHost[K, V], keys []K) {
	if !h.canBatch() {
		for _, key := range keys {
			h.refreshOne(ctx, key)
		}

		return
	}

	var fresh []flight[K, V]
	batch := &batchLoad{}
	batch.ctx, batch.cancel = context.WithCancel(context.WithoutCancel(ctx))
	for _, key := range keys {
		skey, idx, g := h.slot(key)

		g.mu.Lock()
		if g.running(key) != nil {
			g.mu.Unlock()

			continue
		}
		// One waiter that never leaves, as in refresh.
		cl := &call[V]{done: make(chan struct{}), waiters: 1, token: h.beginLoad(idx, skey), cancel: batch.join()}
		g.calls[key] = cl
		g.mu.Unlock()

		fresh = append(fresh, flight[K, V]{key: key, skey: skey, idx: idx, g: g, cl: cl, ctx: batch.ctx})
	}
	if len(fresh) == 0 {
		batch.cancel()

		return
	}

	go runBatch(h, batch.ctx, batch.cancel, fresh, true)
}

// runBatch is run for a batch.
func runBatch[K comparable, V any](
	h batchHost[K, V], ctx context.Context, cancel context.CancelFunc, fresh []flight[K, V], refresh bool,
) {
	defer cancel()
	defer func() {
		for _, f := range fresh {
			h.endLoad(f.idx, f.skey, f.cl.token)
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
		for _, c := range h.countersAt(fresh[0].idx) {
			if c != nil {
				c.batches.Add(1)
			}
		}
		for _, f := range fresh {
			for _, c := range h.countersAt(f.idx) {
				if c != nil {
					c.countLoad(outcomeOf(f.cl.err, f.cl.pan), refresh)
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
	values, err := h.loadMany(ctx, keys)

	for _, f := range fresh {
		v, ok := values[f.key]
		switch {
		case err != nil:
			// Every key fails, even those the map holds: part of an answer
			// from a failed call is not worth caching.
			var zero V
			f.cl.err = err
			h.publish(f.idx, f.skey, zero, err, f.cl.token)
		case ok:
			f.cl.val = v
			h.publish(f.idx, f.skey, v, nil, f.cl.token)
		default:
			f.cl.err = ErrNotFound
			h.publish(f.idx, f.skey, v, ErrNotFound, f.cl.token)
		}
	}
}

// collect waits for every flight, gathering results into found. If ctx ends
// first it leaves the rest, so that loads nobody waits for can be cancelled.
func collect[K comparable, V any](ctx context.Context, waits []flight[K, V], found map[K]V) error {
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

// The cache as a batchHost: keys are stored as they are.

func (c *core[K, V]) batchLookup(key K) (V, Status, bool) {
	v, st, _, refresh := c.lookup(key, c.getRefresher)

	return v, st, refresh
}

func (c *core[K, V]) slot(key K) (K, uint64, *flightGroup[K, V]) {
	idx := c.shardIndex(key)

	return key, idx, c.flights[idx]
}

func (c *core[K, V]) beginLoad(idx uint64, key K) *loadToken { return c.shards[idx].beginLoad(key) }

func (c *core[K, V]) endLoad(idx uint64, key K, token *loadToken) { c.shards[idx].endLoad(key, token) }

func (c *core[K, V]) recheck(idx uint64, key K) (V, Status) {
	v, st, _, _ := c.shards[idx].get(key, c.now(), refreshNone)

	return v, st
}

func (c *core[K, V]) publish(_ uint64, key K, v V, err error, token *loadToken) {
	switch {
	case err == nil:
		_ = c.setValue(key, v, c.valueCost(v), c.ttl, c.refreshAfter, refreshCache, token)
	case errors.Is(err, ErrNotFound):
		_ = c.setNegative(key, c.negativeTTL, token)
	}
}

func (c *core[K, V]) countersAt(idx uint64) [2]*counters {
	if !c.countStats {
		return [2]*counters{}
	}

	return [2]*counters{&c.shards[idx].counters}
}

func (c *core[K, V]) canBatch() bool { return c.batchLoader != nil }

func (c *core[K, V]) loadMany(ctx context.Context, keys []K) (map[K]V, error) {
	return c.batchLoader(ctx, keys)
}

func (c *core[K, V]) runOne(f flight[K, V]) {
	c.run(f.ctx, f.g, c.shards[f.idx], f.key, f.cl, c.loader, false)
}

func (c *core[K, V]) refreshOne(ctx context.Context, key K) { c.refresh(ctx, key, c.loader) }

// loadAsBatch is GetOrLoad's loader when there is only a BatchLoader.
func (c *core[K, V]) loadAsBatch(ctx context.Context, key K) (V, error) {
	if c.countStats {
		c.shardFor(key).counters.batches.Add(1)
	}

	return loadOneAsBatch(ctx, c.batchLoader, key)
}

// loadOneAsBatch asks a batch loader for one key, as a Loader would answer.
func loadOneAsBatch[K comparable, V any](
	ctx context.Context, load func(context.Context, []K) (map[K]V, error), key K,
) (V, error) {
	values, err := load(ctx, []K{key})
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
