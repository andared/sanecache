package sanecache

import (
	"context"
	"errors"
)

// GetManyOrLoad is Cache.GetManyOrLoad for this view, with ViewOptions.BatchLoader,
// or with Loader key by key when there is none. With neither it returns
// ErrNoLoader.
func (v *View[T]) GetManyOrLoad(ctx context.Context, keys []string) (map[string]T, error) {
	if v.loader == nil {
		return nil, ErrNoLoader
	}

	return getMany[string, T](ctx, v, keys)
}

// loadAsBatch is GetOrLoad's loader when there is only a BatchLoader.
func (v *View[T]) loadAsBatch(ctx context.Context, key string) (T, error) {
	if v.countStats {
		v.stats.batches.Add(1)
		if v.cache != nil {
			v.cache.core.shardFor(v.prefix + key).counters.batches.Add(1)
		}
	}

	return loadOneAsBatch(ctx, v.batchLoader, key)
}

// The view as a batchHost: keys are stored under the prefix, and a value of
// another type is a miss. Without a cache nothing is stored and every key is
// tracked in the one flight group.

// batchLookup repeats lookup rather than sharing it: an extra call there is an
// extra call on every Get.
func (v *View[T]) batchLookup(key string) (T, Status, bool) {
	var zero T

	if v.cache == nil {
		v.count(&v.stats.misses)

		return zero, StatusMiss, false
	}

	raw, st, shard, refresh := v.cache.core.lookup(v.prefix+key, v.getRefresher)
	switch st {
	case StatusHit:
		val, ok := raw.(T)
		if !ok {
			v.count(&v.stats.typeMisses)
			v.count(&shard.counters.typeMisses)

			return zero, StatusMiss, false
		}
		v.count(&v.stats.hits)

		return val, StatusHit, refresh

	case StatusNegative:
		v.count(&v.stats.negatives)

		return zero, StatusNegative, false

	default:
		v.count(&v.stats.misses)

		return zero, StatusMiss, false
	}
}

func (v *View[T]) slot(key string) (string, uint64, *flightGroup[string, T]) {
	if v.cache == nil {
		return key, 0, v.flights[0]
	}
	skey := v.prefix + key
	idx := v.cache.core.shardIndex(skey)

	return skey, idx, v.flights[idx]
}

func (v *View[T]) beginLoad(idx uint64, skey string) *loadToken {
	if v.cache == nil {
		return nil
	}

	return v.cache.core.shards[idx].beginLoad(skey)
}

func (v *View[T]) endLoad(idx uint64, skey string, token *loadToken) {
	if v.cache != nil {
		v.cache.core.shards[idx].endLoad(skey, token)
	}
}

func (v *View[T]) recheck(idx uint64, skey string) (T, Status) {
	var zero T

	if v.cache == nil {
		return zero, StatusMiss
	}
	raw, st, _, _ := v.cache.core.shards[idx].get(skey, v.cache.core.now(), refreshNone)
	val, typed := raw.(T)
	if st == StatusHit && !typed {
		return zero, StatusMiss
	}

	return val, st
}

func (v *View[T]) publish(_ uint64, skey string, val T, err error, token *loadToken) {
	if v.cache == nil {
		return
	}
	c := v.cache.core
	switch {
	case err == nil:
		_ = c.setValue(skey, val, v.valueCost(val), v.ttl, v.refreshAfter, refreshView, token)
	case errors.Is(err, ErrNotFound):
		_ = c.setNegative(skey, v.negativeTTL, token)
	}
}

func (v *View[T]) countersAt(idx uint64) [2]*counters {
	switch {
	case !v.countStats:
		return [2]*counters{}
	case v.cache == nil:
		return [2]*counters{v.stats}
	default:
		return [2]*counters{v.stats, &v.cache.core.shards[idx].counters}
	}
}

func (v *View[T]) canBatch() bool { return v.batchLoader != nil }

func (v *View[T]) loadMany(ctx context.Context, keys []string) (map[string]T, error) {
	return v.batchLoader(ctx, keys)
}

func (v *View[T]) runOne(f flight[string, T]) {
	if v.cache == nil {
		f.g.run(f.key, f.cl, func() (T, error) {
			val, _, err := v.loader(f.ctx, f.key)

			return val, err
		}, func(o loadOutcome) { v.stats.countLoad(o, false) })

		return
	}

	v.run(f.ctx, f.g, v.cache.core.shards[f.idx], f.key, f.cl, v.loader, false)
}

// refreshOne is only asked for keys a lookup found due, so there is a cache.
func (v *View[T]) refreshOne(ctx context.Context, key string) { v.refresh(ctx, key, v.loader) }
