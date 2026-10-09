package sanecache

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
)

// GetOrLoad returns the cached value, calling Options.Loader when there is none,
// or Options.BatchLoader with this one key when there is no Loader.
// Callers that ask for the same key while a load is running wait for it instead
// of starting their own, so a cold key costs one upstream call rather than one
// per concurrent caller.
//
// A cached "does not exist" answer is reported as ErrNotFound without calling
// the loader. A loaded value is stored before this returns, so the next caller
// finds it cached; a value too large for the budget is still returned, counted
// as a rejection rather than quietly retried forever. Invalidation or a successful
// write during loading suppresses publication, but the waiting callers still
// receive the loader result. New callers do not join invalidated loads.
//
// Errors other than ErrNotFound are returned as the loader produced them and are
// not cached, so the next call tries again.
func (c *Cache[K, V]) GetOrLoad(ctx context.Context, key K) (V, error) {
	if c.core.loader == nil {
		var zero V

		return zero, ErrNoLoader
	}
	if v, done, err := c.cached(ctx, key, c.core.loader); done {
		return v, err
	}

	return c.core.load(ctx, key, c.core.loader)
}

// GetOrLoadFunc is GetOrLoad with the loader passed by the caller instead of
// taken from Options. It is for upstream calls that need more than the key to
// make — the parameters a key only summarises, a request the caller has already
// built — which a loader would otherwise have to parse back out of the key.
//
// Everything else is GetOrLoad's: single flight, storage with the cache's Cost
// and TTLs, ErrNotFound kept as a negative entry, the error, cancellation and
// panic policies, and the counters. A load for the key already running, whether
// GetOrLoad, GetManyOrLoad or another GetOrLoadFunc started it, is waited for
// rather than repeated. That is the one rule the method adds: callers of a key
// share whichever load started first, so load must produce what any other
// caller's function would for that key.
//
// Options.Loader and Options.BatchLoader are not needed. A nil load returns
// ErrNoLoader.
func (c *Cache[K, V]) GetOrLoadFunc(ctx context.Context, key K, load func(context.Context) (V, error)) (V, error) {
	if load == nil {
		var zero V

		return zero, ErrNoLoader
	}
	loader := func(ctx context.Context, _ K) (V, error) { return load(ctx) }
	if v, done, err := c.cached(ctx, key, loader); done {
		return v, err
	}

	return c.core.load(ctx, key, loader)
}

// cached answers a load from the cache when it can, starting a refresh with
// loader when the value it answers with is due for one. done is false only when
// the key is a miss and the caller still has time to wait for a load.
func (c *Cache[K, V]) cached(ctx context.Context, key K, loader func(context.Context, K) (V, error)) (v V, done bool, err error) {
	v, st, _, refresh := c.core.lookup(key, c.core.refresherWith(loader))
	if refresh {
		c.core.refresh(ctx, key, loader)
	}

	switch st {
	case StatusHit:
		return v, true, nil
	case StatusNegative:
		return v, true, ErrNotFound
	}

	// Checked after the lookup: a cached answer is worth having even to a caller
	// who has already run out of time, and costs nothing to give.
	if err := ctx.Err(); err != nil {
		return v, true, err
	}

	return v, false, nil
}

// call is one load in flight. Waiters read val and err only after done is
// closed, which is what publishes them.
type call[V any] struct {
	done  chan struct{}
	val   V
	err   error
	pan   *loaderPanic
	token *loadToken

	// waiters and cancel are guarded by the group's mutex. The count exists so
	// that one caller giving up does not cancel the load the others are waiting
	// for; the last one out cancels it.
	waiters int
	cancel  context.CancelFunc
}

// flightGroup tracks the loads in flight for one shard's worth of keys. It has
// its own mutex rather than the shard's: a load is slow, and the map lookup that
// joins one should not queue behind reads of the shard it belongs to.
type flightGroup[K comparable, V any] struct {
	mu    sync.Mutex
	calls map[K]*call[V]
}

func newFlightGroup[K comparable, V any]() *flightGroup[K, V] {
	return &flightGroup[K, V]{calls: make(map[K]*call[V])}
}

func (c *core[K, V]) load(ctx context.Context, key K, loader func(context.Context, K) (V, error)) (V, error) {
	idx := c.shardIndex(key)
	g, s := c.flights[idx], c.shards[idx]

	g.mu.Lock()
	if cl, ok := g.calls[key]; ok && cl.token.valid.Load() {
		cl.waiters++
		g.mu.Unlock()
		c.countCoalesced(s)

		return g.wait(ctx, key, cl)
	}

	token := s.beginLoad(key)

	// The caller's own lookup happened before this lock, and a load that
	// finished in between is gone from the map by now — but it publishes its
	// value before it leaves, so looking again here, where no load can slip
	// past, is what makes "one load per key" exact rather than nearly true.
	// Without it, a caller descheduled between the two would start a second load
	// for a value that is already cached. Counted without stats, because the
	// caller's lookup has already been counted as the miss it was.
	if v, st, _, _ := s.get(key, c.now(), refreshNone); st != StatusMiss {
		s.endLoad(key, token)
		g.mu.Unlock()
		c.countCoalesced(s)

		if st == StatusNegative {
			var zero V

			return zero, ErrNotFound
		}

		return v, nil
	}

	// The load outlives the caller that starts it, so it does not inherit that
	// caller's cancellation: values yes, deadline no. See call.waiters.
	loadCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	cl := &call[V]{done: make(chan struct{}), waiters: 1, cancel: cancel, token: token}
	g.calls[key] = cl
	g.mu.Unlock()

	go c.run(loadCtx, g, s, key, cl, loader, false)

	return g.wait(ctx, key, cl)
}

// refresh reloads key in the background ahead of its expiry. Nobody waits for
// it, but it is an ordinary flight: a caller that finds the key expired before
// it finishes joins it, and invalidation keeps its result out. A load for the
// key that is already running is left to publish instead.
func (c *core[K, V]) refresh(ctx context.Context, key K, loader func(context.Context, K) (V, error)) {
	idx := c.shardIndex(key)
	g, s := c.flights[idx], c.shards[idx]

	g.mu.Lock()
	if cl, ok := g.calls[key]; ok && cl.token.valid.Load() {
		g.mu.Unlock()

		return
	}

	// The refresh is a waiter of its own that never leaves, so that a caller
	// who joins it and gives up cannot cancel a load the cache asked for.
	token := s.beginLoad(key)
	loadCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	cl := &call[V]{done: make(chan struct{}), waiters: 1, cancel: cancel, token: token}
	g.calls[key] = cl
	g.mu.Unlock()

	go c.run(loadCtx, g, s, key, cl, loader, true)
}

func (c *core[K, V]) countCoalesced(s *shard[K, V]) {
	if c.countStats {
		s.counters.coalesced.Add(1)
	}
}

// run performs the load and publishes the result. It runs on its own goroutine
// so that a caller can walk away from a load without ending it. refresh says
// the load is a refresh, which is counted apart from loads.
func (c *core[K, V]) run(
	ctx context.Context, g *flightGroup[K, V], s *shard[K, V], key K, cl *call[V],
	loader func(context.Context, K) (V, error), refresh bool,
) {
	defer s.endLoad(key, cl.token)
	g.run(key, cl, func() (V, error) {
		v, err := loader(ctx, key)
		switch {
		case err == nil:
			_ = c.setValue(key, v, c.valueCost(v), c.ttl, c.refreshAfter, refreshCache, cl.token)
		case errors.Is(err, ErrNotFound):
			_ = c.setNegative(key, c.negativeTTL, cl.token)
		}

		return v, err
	}, func(o loadOutcome) {
		if c.countStats {
			s.counters.countLoad(o, refresh)
		}
	})
}

// run shares result publication and panic handling between cache and view loads.
func (g *flightGroup[K, V]) run(key K, cl *call[V], load func() (V, error), record func(loadOutcome)) {
	defer cl.cancel()

	// The loader does not run on a caller's goroutine, so a panic in it would
	// take the process down instead of the request that caused it. Carry it to
	// the callers, who can recover from it as they would from any other call.
	// Counted from the deferred path so that a load that panicked is counted
	// too: a service that recovers panics per request would otherwise see a
	// loader failing every call and a cache reporting no loads at all.
	defer func() {
		if r := recover(); r != nil {
			cl.pan = &loaderPanic{value: r, stack: debug.Stack()}
		}
		record(outcomeOf(cl.err, cl.pan))
		g.finish(key, cl)
	}()

	cl.val, cl.err = load()
}

// wait blocks until the load finishes or ctx is done, whichever comes first.
func (g *flightGroup[K, V]) wait(ctx context.Context, key K, cl *call[V]) (V, error) {
	select {
	case <-cl.done:
		if cl.pan != nil {
			panic(cl.pan)
		}

		return cl.val, cl.err

	case <-ctx.Done():
		g.leave(key, cl)
		var zero V

		return zero, ctx.Err()
	}
}

// leave drops one waiter. The last one to go cancels the load and takes it out
// of the map, so that the next caller starts a fresh one rather than joining a
// load that is being abandoned.
func (g *flightGroup[K, V]) leave(key K, cl *call[V]) {
	g.mu.Lock()
	cl.waiters--
	last := cl.waiters == 0
	if last && g.calls[key] == cl {
		delete(g.calls, key)
	}
	g.mu.Unlock()

	if last {
		cl.cancel()
	}
}

// finish publishes the result. The call leaves the map first so that a caller
// arriving after this point starts a new load instead of waiting for a result
// that has already been handed out.
func (g *flightGroup[K, V]) finish(key K, cl *call[V]) {
	g.mu.Lock()
	if g.calls[key] == cl {
		delete(g.calls, key)
	}
	g.mu.Unlock()

	close(cl.done)
}

// loadOutcome is how a completed load ended, as the counters see it.
type loadOutcome uint8

const (
	loadOK loadOutcome = iota
	// loadNotFound is the upstream saying the key does not exist. It is kept
	// apart from failures: a service asking for ids that are gone is working as
	// designed, and counting it as an error hides the errors that are real.
	loadNotFound
	loadFailed
)

func outcomeOf(err error, pan *loaderPanic) loadOutcome {
	switch {
	case pan != nil:
		return loadFailed
	case err == nil:
		return loadOK
	case errors.Is(err, ErrNotFound):
		return loadNotFound
	default:
		return loadFailed
	}
}

// loaderPanic carries a panic from the loader's goroutine to the callers waiting
// on it, keeping the stack of where it actually happened.
type loaderPanic struct {
	value any
	stack []byte
}

func (p *loaderPanic) Error() string {
	return fmt.Sprintf("sanecache: loader panicked: %v\n\n%s", p.value, p.stack)
}
