package sanecache

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
)

// GetOrLoad returns the cached value, calling Options.Loader when there is none.
// Callers that ask for the same key while a load is running wait for it instead
// of starting their own, so a cold key costs one upstream call rather than one
// per concurrent caller.
//
// A cached "does not exist" answer is reported as ErrNotFound without calling
// the loader. A loaded value is stored before this returns, so the next caller
// finds it cached; a value too large for the budget is still returned, counted
// as a rejection rather than quietly retried forever.
//
// Errors other than ErrNotFound are returned as the loader produced them and are
// not cached, so the next call tries again.
func (c *Cache[K, V]) GetOrLoad(ctx context.Context, key K) (V, error) {
	var zero V

	if c.core.loader == nil {
		return zero, ErrNoLoader
	}

	switch v, st := c.Lookup(key); st {
	case StatusHit:
		return v, nil
	case StatusNegative:
		return zero, ErrNotFound
	}

	// Checked after the lookup: a cached answer is worth having even to a caller
	// who has already run out of time, and costs nothing to give.
	if err := ctx.Err(); err != nil {
		return zero, err
	}

	return c.core.load(ctx, key)
}

// call is one load in flight. Waiters read val and err only after done is
// closed, which is what publishes them.
type call[V any] struct {
	done chan struct{}
	val  V
	err  error
	pan  *loaderPanic

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

func (c *core[K, V]) load(ctx context.Context, key K) (V, error) {
	idx := c.shardIndex(key)
	g, s := c.flights[idx], c.shards[idx]

	g.mu.Lock()
	if cl, ok := g.calls[key]; ok {
		cl.waiters++
		g.mu.Unlock()
		c.countCoalesced(s)

		return g.wait(ctx, key, cl)
	}

	// The caller's own lookup happened before this lock, and a load that
	// finished in between is gone from the map by now — but it publishes its
	// value before it leaves, so looking again here, where no load can slip
	// past, is what makes "one load per key" exact rather than nearly true.
	// Without it, a caller descheduled between the two would start a second load
	// for a value that is already cached. Counted without stats, because the
	// caller's lookup has already been counted as the miss it was.
	if v, st, _ := s.get(key, c.now()); st != StatusMiss {
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
	cl := &call[V]{done: make(chan struct{}), waiters: 1, cancel: cancel}
	g.calls[key] = cl
	g.mu.Unlock()

	go c.run(loadCtx, g, s, key, cl)

	return g.wait(ctx, key, cl)
}

func (c *core[K, V]) countCoalesced(s *shard[K, V]) {
	if c.countStats {
		s.counters.coalesced.Add(1)
	}
}

// run performs the load and publishes the result. It runs on its own goroutine
// so that a caller can walk away from a load without ending it.
func (c *core[K, V]) run(ctx context.Context, g *flightGroup[K, V], s *shard[K, V], key K, cl *call[V]) {
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
		if c.countStats {
			s.counters.loads.Add(1)
			if cl.err != nil || cl.pan != nil {
				s.counters.loadErrors.Add(1)
			}
		}
		g.finish(key, cl)
	}()

	v, err := c.loader(ctx, key)
	cl.val, cl.err = v, err

	// Cached before the waiters are released, so that a caller who looks the key
	// up again straight away finds it, and a caller arriving a moment later does
	// not start a second load for a value that is already in hand.
	switch {
	case err == nil:
		_ = c.setValue(key, v, c.valueCost(v), c.ttl)
	case errors.Is(err, ErrNotFound):
		// ErrNegativeDisabled is not a problem here: a cache without NegativeTTL
		// simply does not remember the answer.
		_ = c.setNegative(key, c.negativeTTL)
	}
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

// loaderPanic carries a panic from the loader's goroutine to the callers waiting
// on it, keeping the stack of where it actually happened.
type loaderPanic struct {
	value any
	stack []byte
}

func (p *loaderPanic) Error() string {
	return fmt.Sprintf("sanecache: loader panicked: %v\n\n%s", p.value, p.stack)
}
