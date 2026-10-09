package sanecache

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
)

// GetOrLoad returns the cached value, calling Options.Loader when there is none,
// or Options.BatchLoader with this one key when there is no Loader. Callers of a
// key whose load is running wait for it instead of starting their own.
//
// A cached "does not exist" is reported as ErrNotFound. A loaded value is
// stored before this returns; one too large for the budget is still returned,
// and counted as a rejection. Invalidation or a successful write during the
// load keeps its result out of the cache, though its waiters still get it, and
// new callers start a new load. Other errors are returned as the loader
// produced them and are not cached.
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

// GetOrLoadFunc is GetOrLoad with the loader passed in, for upstream calls that
// need more than the key. A running load of the key, whoever started it, is
// waited for instead, so load must return what any caller's would for that key.
// Options.Loader is not needed; a nil load returns ErrNoLoader.
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

// cached answers a load from the cache when it can, starting the refresh it
// claims with loader. It reports false only for a miss the caller can wait for.
func (c *Cache[K, V]) cached(ctx context.Context, key K, loader func(context.Context, K) (V, error)) (V, bool, error) {
	v, st, _, refresh := c.core.lookup(key, c.core.refresherWith(loader))
	if refresh {
		c.core.refresh(ctx, key, loader)
	}

	return answered(ctx, v, st)
}

// answered is cached's verdict on a lookup. ctx is checked after the lookup: a
// cached answer is worth having even to a caller who has run out of time.
func answered[V any](ctx context.Context, v V, st Status) (V, bool, error) {
	if st == StatusHit {
		return v, true, nil
	}
	if st == StatusNegative {
		return v, true, ErrNotFound
	}
	err := ctx.Err()

	return v, err != nil, err
}

// call is one load in flight. Closing done publishes val, err and pan.
type call[V any] struct {
	done  chan struct{}
	val   V
	err   error
	pan   *loaderPanic
	token *loadToken

	// Guarded by the group's mutex. The last waiter to give up cancels the load,
	// rather than the first.
	waiters int
	cancel  context.CancelFunc
}

// flightGroup tracks the loads in flight for one shard's keys, under a mutex of
// its own so that joining a load does not queue behind the shard's reads.
type flightGroup[K comparable, V any] struct {
	mu    sync.Mutex
	calls map[K]*call[V]
}

func newFlightGroup[K comparable, V any]() *flightGroup[K, V] {
	return &flightGroup[K, V]{calls: make(map[K]*call[V])}
}

// running returns key's load if one is in flight and not invalidated. The caller
// holds g.mu.
func (g *flightGroup[K, V]) running(key K) *call[V] {
	cl := g.calls[key]
	if cl == nil || (cl.token != nil && !cl.token.valid.Load()) {
		return nil
	}

	return cl
}

// startAndUnlock registers a load of key with one waiter, unlocks g.mu and runs
// it on its own goroutine, with the caller's values but not its cancellation.
func (g *flightGroup[K, V]) startAndUnlock(
	ctx context.Context, key K, token *loadToken, run func(context.Context, *call[V]),
) *call[V] {
	loadCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	cl := &call[V]{done: make(chan struct{}), waiters: 1, cancel: cancel, token: token}
	g.calls[key] = cl
	g.mu.Unlock()
	go run(loadCtx, cl)

	return cl
}

// drop takes cl out of the map unless a newer load has replaced it. The caller
// holds g.mu.
func (g *flightGroup[K, V]) drop(key K, cl *call[V]) {
	if g.calls[key] == cl {
		delete(g.calls, key)
	}
}

func (c *core[K, V]) load(ctx context.Context, key K, loader func(context.Context, K) (V, error)) (V, error) {
	idx := c.shardIndex(key)
	g, s := c.flights[idx], c.shards[idx]

	g.mu.Lock()
	if cl := g.running(key); cl != nil {
		cl.waiters++
		g.mu.Unlock()
		c.countCoalesced(s)

		return g.wait(ctx, key, cl)
	}

	token := s.beginLoad(key)

	// A load that finished since the caller's lookup has left the map but
	// published first, so looking again under the lock makes "one load per key"
	// exact. Not counted: the caller's lookup already was.
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

	cl := g.startAndUnlock(ctx, key, token, func(ctx context.Context, cl *call[V]) {
		c.run(ctx, g, s, key, cl, loader, false)
	})

	return g.wait(ctx, key, cl)
}

// refresh reloads key in the background ahead of its expiry, as an ordinary
// flight that callers may join, unless a load of the key is already running.
func (c *core[K, V]) refresh(ctx context.Context, key K, loader func(context.Context, K) (V, error)) {
	idx := c.shardIndex(key)
	g, s := c.flights[idx], c.shards[idx]

	g.mu.Lock()
	if g.running(key) != nil {
		g.mu.Unlock()

		return
	}

	// The refresh is a waiter of its own that never leaves, so that a caller
	// who joins it and gives up cannot cancel a load the cache asked for.
	g.startAndUnlock(ctx, key, s.beginLoad(key), func(ctx context.Context, cl *call[V]) {
		c.run(ctx, g, s, key, cl, loader, true)
	})
}

func (c *core[K, V]) countCoalesced(s *shard[K, V]) {
	if c.countStats {
		s.counters.coalesced.Add(1)
	}
}

// run performs the load and publishes the result; refresh says it is counted as
// a refresh.
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

	// A panic here would take down the process rather than the request, so it
	// is carried to the callers instead, and counted as a failed load.
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

// leave drops one waiter. The last one cancels the load and takes it out of the
// map, so that the next caller does not join an abandoned load.
func (g *flightGroup[K, V]) leave(key K, cl *call[V]) {
	g.mu.Lock()
	cl.waiters--
	last := cl.waiters == 0
	if last {
		g.drop(key, cl)
	}
	g.mu.Unlock()

	if last {
		cl.cancel()
	}
}

// finish publishes the result, taking the call out of the map first so that
// later callers start a new load.
func (g *flightGroup[K, V]) finish(key K, cl *call[V]) {
	g.mu.Lock()
	g.drop(key, cl)
	g.mu.Unlock()

	close(cl.done)
}

// loadOutcome is how a completed load ended, as the counters see it.
type loadOutcome uint8

const (
	loadOK loadOutcome = iota
	// loadNotFound is an answer, not a failure: counted as an error, ids that
	// are gone would hide the errors that are real.
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

// loaderPanic carries a loader's panic, with its stack, to the waiting callers.
type loaderPanic struct {
	value any
	stack []byte
}

func (p *loaderPanic) Error() string {
	return fmt.Sprintf("sanecache: loader panicked: %v\n\n%s", p.value, p.stack)
}
