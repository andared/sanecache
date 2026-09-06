# sanecache

[![CI](https://github.com/andared/sanecache/actions/workflows/ci.yml/badge.svg)](https://github.com/andared/sanecache/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/andared/sanecache.svg)](https://pkg.go.dev/github.com/andared/sanecache)
[![Go Report Card](https://goreportcard.com/badge/github.com/andared/sanecache)](https://goreportcard.com/report/github.com/andared/sanecache)

A small in-memory cache for Go that aims to be **predictable before it is fast**.

There is no shortage of Go caches, and several of them are excellent. This one exists
because the failures that actually cost time in production were never about throughput:
a write that reports success and is dropped a moment later, a hit rate that quietly
collapses because every value is too big for the budget, a limit expressed in entries
when the thing you are protecting is a memory limit, a batch of keys that all expire in
the same millisecond and stampede the upstream together, and a cold key that a hundred
concurrent requests each fetch for themselves.

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

**A cold key is fetched once.** `GetOrLoad` runs the loader once per key however many
callers arrive while it is running.

## Loading a cold key once

The block in the quick start — look up, go to the upstream on a miss, remember the answer,
remember the absence of one — is the same in every service, and it has a hole in it: on a
cold key, every concurrent request runs it at the same time.

```go
c := sanecache.New(sanecache.Options[string, *Article]{
    TTL:         10 * time.Minute,
    NegativeTTL: 30 * time.Second,
    Loader: func(ctx context.Context, id string) (*Article, error) {
        a, err := db.Article(ctx, id)
        if errors.Is(err, sql.ErrNoRows) {
            return nil, sanecache.ErrNotFound // remember the absence too
        }
        return a, err
    },
})

article, err := c.GetOrLoad(ctx, id)
switch {
case errors.Is(err, sanecache.ErrNotFound):
    return nil, err // from the loader, or from a negative entry it left behind
case err != nil:
    return nil, err
}
```

Three details worth knowing, because they are where implementations differ:

**"Does not exist" is one error, wherever it came from.** The loader returns `ErrNotFound`;
`GetOrLoad` caches that as a negative entry and reports the same `ErrNotFound` to later
callers. Handling of "no such object" does not depend on whether the cache happened to
remember it. Translate the upstream's own error once, in the loader, and it stops mattering
anywhere else.

**Giving up does not cancel the load for everyone else.** The loader's context is not the
first caller's. It carries that caller's values, but it is cancelled only when *every*
caller waiting on the result has gone. `golang.org/x/sync/singleflight` hands the load the
first caller's context, so a request that times out takes down the load that the other
ninety-nine are waiting on. A caller who gives up here gets `ctx.Err()` and leaves the
others alone.

**Failures are not cached.** Anything other than `ErrNotFound` is passed back unchanged and
nothing is stored, so the next call tries again. Single-flight already collapses the retry
storm; caching the error on top of that would turn a blip into an outage that outlives it.

The loader runs on a goroutine of the cache's own, which is what makes the two points above
possible. If it panics, the panic is carried to the callers rather than taking the process
down, and it arrives with the stack of where it actually happened. A load that needs a
deadline of its own should set one inside the loader, where the right number is known.

A load with no caller left is cancelled — but a loader that does not watch its context
finishes anyway, and its value is cached even then. That is deliberate: when callers time
out faster than the upstream answers, throwing those values away means the cache never warms
and every request keeps timing out. The price is the narrow case where such a load lands
after a later one and puts back a value read before it, with the TTL starting again. A
loader that honours cancellation never reaches it.

## Several value types under one budget

A budget in bytes is only worth having if it covers everything competing for the memory.
One cache per type means one budget per type, and dividing a fixed amount of memory between
types up front is exactly the guess the byte budget was meant to avoid: the split that was
right at deploy time is wrong by the next traffic pattern.

```go
c := sanecache.New(sanecache.Options[string, any]{
    TTL:      10 * time.Minute,
    MaxBytes: 64 << 20,
    Cost:     func(any) int64 { return 256 }, // fallback for views without their own
})
defer c.Close()

articles := sanecache.NewView(c, sanecache.ViewOptions[*Article]{
    Name: "article",
    Cost: func(a *Article) int64 { return a.ApproxBytes() },
})
seasons := sanecache.NewView(c, sanecache.ViewOptions[*Season]{
    Name: "season",
    TTL:  time.Hour, // seasons change less often than articles do
})

a, ok := articles.Get(id) // a is a *Article, not an any
```

A view fixes one value type, gives each of its own counters, and prefixes its keys with its
name, so two views cannot collide on the same id. It can bring its own `Cost` — which is
what spares you the type switch that a shared `Cost func(any) int64` otherwise becomes — and
its own TTLs. Eviction stays global: a view that suddenly needs more memory takes it from
whichever entries were used least recently, whatever type they are.

A view is a function rather than a method on `Cache` because a method cannot introduce a
type parameter of its own. Reads cost about 6 ns more than the cache underneath, and no
allocation while the name and key together stay within 32 bytes: up to that length the
compiler keeps the joined key on the stack, and past it every read allocates.

`Stats().TypeMisses` counts lookups that found some other type under a view's key. With
names in the keys that should never happen, which is the point: it is a bug detector for
two views sharing a name, or a write made straight to the underlying cache.

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

Whether it is worth it depends entirely on contention. On an M3 with GOMAXPROCS=8, 4096
keys, all readers hitting one cache:

| shards | `Get` (LRU) | `Get` (ClearOnFull) | `Set` | 90/10 mixed |
|-------:|------------:|--------------------:|------:|------------:|
| 1      | 131 ns      | 108 ns              | 195 ns| 145 ns      |
| 4      | 66 ns       | 37 ns               | 116 ns| 76 ns       |
| 16     | 40 ns       | 24 ns               | 76 ns | 47 ns       |
| 64     | 33 ns       | 19 ns               | 65 ns | 39 ns       |

Sharding is worth about 4x here and is still buying something at 64. If your service does
one lookup per request, none of this matters and `Shards: 0` is the right answer.

## Eviction policies

`LRU` (default) evicts least-recently-used entries until the shard fits. Maintaining that
order means every read takes a write lock.

`ClearOnFull` drops the whole shard except the entry that just overflowed it. Reads then
need only a read lock, which is worth 20% on a single shard and 40% once the cache is
sharded — a read lock only pays off when the cores taking it are not all queued behind the
same one. This is the right trade when access order is flat (everything in the working set
is used every cycle, so LRU has no information to offer) and refilling is cheap relative to
the bookkeeping. It costs hit rate, though: see the table further down.

## The clock

Uncontended, a `Get` that hits costs 47 ns on the machine above, and **27 ns of that is
reading the wall clock** to decide whether the entry has expired. Well over half of a
lookup is `time.Now()`.

`ClockGranularity` hands that job to a background goroutine and lets lookups read an atomic
instead:

```go
sanecache.Options[string, *Article]{
    TTL:              10 * time.Minute,
    ClockGranularity: 100 * time.Millisecond,
}
```

A hit then costs 20 ns rather than 47, and expiry becomes accurate to within one interval
in either direction — which against a ten-minute TTL is nothing, and against a one-second
TTL is a lot. It is off by default because a TTL that quietly means something other than
what it says is exactly the kind of surprise this library is about. `Close` stops the
goroutine and lookups go back to the wall clock, rather than to a clock that has stopped.

## Stats

Counters are built in — no callback interface on the hot path. Poll `Stats()` on your
metrics interval and export it however you like:

```go
s := c.Stats()
// s.Hits, s.Misses, s.Negatives, s.TypeMisses, s.Evictions, s.Expirations,
// s.Replacements, s.Rejections, s.Loads, s.LoadErrors, s.Coalesced,
// s.Entries, s.Bytes, s.HitRate()
```

`Rejections` is the one to alert on: a steady nonzero rate means keys that can never be
cached. `Coalesced` against `Loads` says how much work the single-flight is actually saving
— if they are equal, the cache is cold in a way worth looking at. `OnEvict` is available
separately when entries hold resources that need releasing.

## Lifecycle

A background goroutine sweeps expired entries so they stop occupying budget before anyone
looks them up. `Close()` stops it, along with the clock goroutine if there is one.
Forgetting to call `Close()` does not leak them — dropping the cache stops them too — but
calling it is still better than relying on when the collector gets around to it.

`DisableCleanup: true` runs without the sweeper; entries then expire only on lookup.

## How it compares

The `benchmarks/` module measures this cache against the ones it would otherwise be
replacing. It is a separate module so the root stays dependency-free; `make bench-compare`
runs it. Same machine as above, 256-byte values, a budget sized to 4096 of them. The
sanecache rows are its knobs, each one added to the row above it.

| ns/op | serial `Get` | `Get` ×8 | `Set` ×8 | 90/10 ×8 |
|---|---:|---:|---:|---:|
| sanecache, defaults | 61 | 139 | 226 | 151 |
| ⤷ `Shards: 16` | 64 | 42 | 91 | 49 |
| ⤷ + `ClockGranularity` | 34 | 32 | 84 | 42 |
| ⤷ + `ClearOnFull` | 32 | 20 | 90 | 39 |
| [otter](https://github.com/maypok86/otter) v2 | 77 | 14 | 263 | 30 |
| [ristretto](https://github.com/dgraph-io/ristretto) v2 | 86 | 21 | 324 | 65 |
| [golang-lru](https://github.com/hashicorp/golang-lru) `v2/expirable` | 52 | 142 | 190 | 165 |
| [ttlcache](https://github.com/jellydator/ttlcache) v3 | 63 | 177 | 267 | 192 |

The first column is what one lookup costs; the second is what eight goroutines get out of
the same machine. Read them together, because they say opposite things. Per operation this
cache is *cheaper* than otter and ristretto — they spend their time maintaining a frequency
sketch and a set of ring buffers that only pay off later. What they buy with it is scaling:
otter turns eight cores into 5.4x the throughput, this one into 1.6x, and unsharded into
less than 1x, because reads take a lock and locks are where cores queue.

So the honest shape of it is not "slower". It is: **the same work per operation, and less
of the machine used to do it in parallel.** Whether that matters is a question about the
read rate. At one lookup per request, the gap between 20 ns and 14 ns is six nanoseconds
against a request budget measured in milliseconds. It starts to matter when one request
does thousands of lookups, or when the cache more or less is the service.

Writes are the other way round: 91 ns against 190 to 324. An admission policy has to decide
whether to accept each write and update its sketch, and that costs more than taking a lock
does. A write-heavy cache is the case where this library is simply faster.

The number that usually matters more than any of those is how much of a fixed budget each
policy turns into hits. 4096 entries against a key space of 100,000:

| %hit | zipf s=1.20 | zipf s=1.01 | zipf + scan |
|---|---:|---:|---:|
| sanecache (LRU) | 87.3 | 65.8 | 78.0 |
| sanecache (ClearOnFull) | 83.1 | 58.0 | 73.7 |
| otter v2 | 88.9 | **71.4** | 80.8 |
| ristretto v2 | 87.4 | 68.7 | 78.9 |
| golang-lru `expirable` | 87.3 | 65.8 | 78.0 |
| ttlcache v3 | 87.3 | 65.8 | 78.0 |

When the hot set fits, every policy looks the same and the extra machinery buys 1.6 points.
When it does not, W-TinyLFU is worth 5.6 points of hit rate over LRU — and 5.6 points of
upstream traffic is worth more than every nanosecond in the table above it. That is the real
reason to pick otter, and it is a better one than throughput.

The same table prices the cheap read lock: `ClearOnFull` buys its 40% by giving up 7.8
points of hit rate. It is a trade for caches whose access order is flat, not a free win.

## When to use something else

- You want the best possible hit rate for a given memory budget, and admission policies and
  access-frequency estimation are worth the complexity → [otter](https://github.com/maypok86/otter).
  The hit-rate table above is the size of the prize, and it is the biggest number on this page.
- Your read path is hot enough that how well a cache scales across cores is a real
  difference → otter or [ristretto](https://github.com/dgraph-io/ristretto), and budget for
  `Wait()` in the tests.
- You are caching hundreds of megabytes and GC pressure from millions of live pointers is
  your actual problem → [bigcache](https://github.com/allegro/bigcache) or
  [freecache](https://github.com/coocood/freecache), which keep entries off-heap.
- You just want a bounded LRU with TTL and nothing else →
  [hashicorp/golang-lru](https://github.com/hashicorp/golang-lru)'s `v2/expirable`.

## Status

v0.2. The API above is what exists and is tested; expect it to move before v1.

Contributions are welcome — see [CONTRIBUTING.md](CONTRIBUTING.md) for what this
library optimises for before proposing a change.

MIT licensed.
