# sanecache

A small in-memory cache for Go that aims to be **predictable before it is fast**.

There is no shortage of Go caches, and several of them are excellent. This one exists
because the failures that actually cost time in production were never about throughput:
a write that reports success and is dropped a moment later, a hit rate that quietly
collapses because every value is too big for the budget, a limit expressed in entries
when the thing you are protecting is a memory limit, and a batch of keys that all expire
in the same millisecond and stampede the upstream together.

```go
import "github.com/andared/sanecache"
```

Requires Go 1.24. No dependencies.

## Quick start

```go
c := sanecache.New(sanecache.Options[string, *Article]{
    TTL:         10 * time.Minute,
    NegativeTTL: 30 * time.Second,
    Jitter:      10,
    MaxBytes:    64 << 20,
    Cost:        func(a *Article) int64 { return a.ApproxBytes() },
})
defer c.Close()

if v, status := c.Lookup(id); status != sanecache.StatusMiss {
    if status == sanecache.StatusNegative {
        return nil, ErrNotFound // the upstream already told us, don't ask again
    }
    return v, nil
}

article, err := fetch(ctx, id)
switch {
case errors.Is(err, ErrNotFound):
    c.SetNegative(id)
    return nil, err
case err != nil:
    return nil, err
}

if err := c.Set(id, article); err != nil {
    // ErrTooLarge: this key will never be cached. Worth a metric.
}
```

## What "sane" means here

**Writes are synchronous.** A value is readable the moment `Set` returns. Caches that
buffer writes asynchronously are faster on paper and force `Wait()` calls into your tests
to paper over the gap — and once your tests need it, so does any code that writes a value
and reads it back in the same request.

**A value that does not fit is refused, not swallowed.** `Set` returns `ErrTooLarge` when
the value's cost exceeds its shard's budget. The alternative — accept the write, return
success, drop the entry during eviction — is invisible: the hit rate does not obviously
look wrong, and every request for those keys goes to the upstream forever.

**Budgets are in bytes.** A cap on the number of entries tells you nothing about memory
when entries are documents rather than integers: ten thousand entries is a rounding error
for `int` values and several gigabytes for HTML templates. `MaxBytes` requires a `Cost`
function; asking for a byte budget without one is a construction-time panic rather than a
budget that silently counts entries.

**"It does not exist" is an answer.** `SetNegative` records that the upstream was asked and
said no. Without a first-class form for this, it ends up as a sentinel value smuggled
inside your value type, which does not survive the type parameter and tends to be charged
zero cost — making it the one thing eviction can never reclaim.

**TTLs can be jittered.** `Jitter: 10` spreads each expiry by up to ±10%. Keys warmed
together by one request otherwise expire together and hit the upstream as one wave.

## The `Cost` function is the part worth getting right

`MaxBytes` is only as honest as `Cost`. The number you want is resident size, and it is
usually several times the serialized size — a decoded struct carries headers, pointers,
map overhead and per-field padding that JSON does not.

Measure it once instead of guessing:

```go
runtime.GC()
var before runtime.MemStats
runtime.ReadMemStats(&before)

values := make([]*Article, 0, n)
for range n {
    values = append(values, decode(sample))
}

runtime.GC()
var after runtime.MemStats
runtime.ReadMemStats(&after)
runtime.KeepAlive(values)

ratio := float64(after.HeapAlloc-before.HeapAlloc) / float64(n*len(sample))
```

In one production service this ratio came out at ~2.6× for decoded JSON structs, higher
for `map[string]any`, and ~6.7× for compiled templates. Sizing a cache off raw payload
length understated real memory sevenfold.

## Sharding

`Shards` splits the cache into independently locked parts (rounded up to a power of two).
It is off by default, because it is not free: each shard gets an equal slice of `MaxBytes`,
so an uneven key distribution leaves part of the budget unused, and eviction becomes
per-shard rather than global.

Whether it is worth it depends entirely on contention. On darwin/arm64 with GOMAXPROCS=8,
4096 keys, all readers hitting one cache:

| shards | `Get` (LRU) | `Get` (ClearOnFull) | `Set` | 90/10 mixed |
|-------:|------------:|--------------------:|------:|------------:|
| 1      | 146 ns      | 89 ns               | 205 ns| 146 ns      |
| 4      | 91 ns       | 68 ns               | 129 ns| 85 ns       |
| 16     | 74 ns       | 60 ns               | 86 ns | 62 ns       |
| 64     | 68 ns       | 70 ns               | 81 ns | 95 ns       |

Two things to read out of that. Sharding buys roughly 2× on this machine and stops paying
past ~16 shards — past that you are adding shards faster than you are removing contention.
And the single-threaded cost of `Get` is 58 ns, of which **29 ns is `time.Now()`** — half
of a lookup is reading the clock to check expiry, not touching the map. If your service
does one lookup per request, none of this matters and `Shards: 0` is the right answer.

## Eviction policies

`LRU` (default) evicts least-recently-used entries until the shard fits. Maintaining that
order means every read takes a write lock.

`ClearOnFull` drops the whole shard except the entry that just overflowed it. Reads then
need only a read lock — about 40% cheaper above. This is the right trade when access order
is flat (everything in the working set is used every cycle, so LRU has no information to
offer) and refilling is cheap relative to the bookkeeping.

## Stats

Counters are built in — no callback interface on the hot path. Poll `Stats()` on your
metrics interval and export it however you like:

```go
s := c.Stats()
// s.Hits, s.Misses, s.Negatives, s.Evictions, s.Expirations,
// s.Replacements, s.Rejections, s.Entries, s.Bytes, s.HitRate()
```

`Rejections` is the one to alert on: a steady nonzero rate means keys that can never be
cached. `OnEvict` is available separately when entries hold resources that need releasing.

## Lifecycle

A background goroutine sweeps expired entries so they stop occupying budget before anyone
looks them up. `Close()` stops it. Forgetting to call `Close()` does not leak the
goroutine — dropping the cache stops it too — but calling it is still better than relying
on when the collector gets around to it.

`DisableCleanup: true` runs without the goroutine; entries then expire only on lookup.

## When to use something else

- You want the best possible hit rate for a given memory budget, and admission policies
  and access-frequency estimation are worth the complexity → [otter](https://github.com/maypok86/otter).
- You are caching hundreds of megabytes and GC pressure from millions of live pointers is
  your actual problem → [bigcache](https://github.com/allegro/bigcache) or
  [freecache](https://github.com/coocood/freecache), which keep entries off-heap.
- You just want a bounded LRU with TTL and nothing else →
  [hashicorp/golang-lru](https://github.com/hashicorp/golang-lru)'s `v2/expirable`.

## Status

v0.1. The API above is what exists and is tested; expect it to move before v1.

On the list, in rough order: a loader with single-flight so a cold key is fetched once
rather than once per concurrent caller; typed views over a shared byte budget, for when one
budget holds several value types; and a coarse clock to take that 29 ns off the read path.

MIT licensed.
