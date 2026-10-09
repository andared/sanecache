# sanecache

[![CI](https://github.com/andared/sanecache/actions/workflows/ci.yml/badge.svg)](https://github.com/andared/sanecache/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/andared/sanecache.svg)](https://pkg.go.dev/github.com/andared/sanecache)
[![Go Report Card](https://goreportcard.com/badge/github.com/andared/sanecache)](https://goreportcard.com/report/github.com/andared/sanecache)

A small in-memory cache for Go that aims to be **predictable before it is fast**.

There are excellent Go caches already. This one exists because the failures that cost time
in production were never about throughput: a write that reports success and is dropped a
moment later, a hit rate that collapses because every value is too big for the budget, a
limit in entries when the thing to protect is memory, keys that all expire in the same
millisecond, and a cold key that a hundred concurrent requests each fetch for themselves.

```go
import "github.com/andared/sanecache"
```

Requires Go 1.24. No dependencies.

## Quick start

Save this complete program as `main.go` in a new directory:

```go
package main

import (
    "fmt"
    "time"

    "github.com/andared/sanecache"
)

func main() {
    c := sanecache.New(sanecache.Options[string, string]{
        TTL:        time.Minute,
        MaxEntries: 100,
    })
    defer c.Close()

    if err := c.Set("greeting", "hello"); err != nil {
        panic(err)
    }

    value, ok := c.Get("greeting")
    fmt.Println(value, ok)
}
```

```sh
go mod init example.com/cache-demo
go get github.com/andared/sanecache@v0.7.0
go run .
```

Output: `hello true`. The same program is in [`examples/basic`](examples/basic), and
[a second example](examples/README.md#caching-an-http-dependency) caches JSON responses
from a local HTTP server, 404s included as negative entries. Both run in CI.

## Applying a byte budget

With your own `Article` type and `fetch` function:

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

**Writes are synchronous.** A value is readable the moment `Set` returns, so tests need no
`Wait()`, and neither does code that writes a value and reads it back.

**A value that does not fit is refused.** `Set` returns `ErrTooLarge` when the value costs
more than its shard's budget, instead of accepting it and evicting it unseen, which sends
every request for that key to the upstream forever.

**Budgets are in bytes.** Ten thousand entries is nothing for `int` values and gigabytes for
HTML templates. `MaxBytes` without a `Cost` function panics at construction rather than
quietly counting entries.

**"It does not exist" is an answer.** `SetNegative` records that the upstream said no,
instead of a sentinel smuggled into the value type. Under a byte budget a negative entry is
charged what it holds — entry, map slot and key — so eviction can reclaim it.

**TTLs can be jittered.** `Jitter: 10` spreads each expiry by up to ±10%, so keys warmed
together do not expire together and hit the upstream as one wave.

**A cold key is fetched once**, however many callers ask for it at the same moment.

## Loading a cold key once

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

`GetOrLoad` runs the loader once per key however many callers arrive while it runs. Where
implementations differ:

- **"Does not exist" is one error.** The loader returns `ErrNotFound`, `GetOrLoad` caches it
  as a negative entry and returns the same error to later callers. Translate the upstream's
  own error once, in the loader.
- **Giving up does not cancel the load for everyone else.** The loader's context carries the
  first caller's values but is cancelled only once *every* waiting caller has gone. A bare
  [`singleflight.Group`](https://pkg.go.dev/golang.org/x/sync/singleflight#Group) leaves
  that to you, and a callback capturing the first caller's context lets its timeout cancel
  work others still need.
- **Failures are not cached.** Errors other than `ErrNotFound` are returned unchanged and the
  next call tries again; single flight already collapses the retry storm.

The loader runs on a goroutine of the cache's own. A panic in it reaches the callers, with
the stack of where it happened, rather than taking the process down. Set a deadline inside
the loader, where the right number is known. A loader that ignores cancellation still warms
the cache, unless the key was invalidated or written in the meantime.

### When the loader needs more than the key

When the upstream call needs more than the key — a query string the key only summarises, a
request already built — pass the loader in with the call:

```go
page, err := c.GetOrLoadFunc(ctx, pageKey(path, params), func(ctx context.Context) (*Page, error) {
    return fetchPage(ctx, path, params)
})
```

It shares loads with `GetOrLoad`: callers of a key wait for whichever load started first,
so the function must return what any caller's would for that key. Views have it too.

## Invalidation while loading

`Delete` (even of an absent key), `Clear` and successful writes keep outstanding loads of
the key from putting their result in the cache. So if a loader reads an old record and the
application then updates the source and calls `Delete`, the old record does not come back.
Callers already waiting still get the loader's result; new callers start a new load. Update
the source before invalidating the cache.

This follows the storage key, across views sharing a namespace and the parent cache, and a
load's own publication supersedes other loads of the key. `Clear` works shard by shard, so
it is not an atomic snapshot. `Close` only stops background goroutines.

## Loading many keys at once

`GetManyOrLoad` sends the keys the cache does not hold to a `BatchLoader` in one call — a
query with `IN`, a pipelined round trip:

```go
c := sanecache.New(sanecache.Options[int, *Article]{
    TTL:         10 * time.Minute,
    NegativeTTL: 30 * time.Second,
    BatchLoader: func(ctx context.Context, ids []int) (map[int]*Article, error) {
        return db.ArticlesByIDs(ctx, ids) // an id missing from the map does not exist
    },
})

articles, err := c.GetManyOrLoad(ctx, ids) // cached or loaded; absent ids are not in it
```

Everything works per key: a key another call is already loading is waited for, not loaded
again, and a `Delete` of one key keeps only that key's result out. A key missing from the
answer is cached as a negative entry and is simply absent from the result. An error fails
the whole batch and nothing from it is cached. The upstream call is cancelled only once no
caller waits for any of its keys.

With only a `BatchLoader`, `GetOrLoad` uses it with a batch of one; with only a `Loader`,
`GetManyOrLoad` loads the missing keys concurrently. `Stats().Batches` counts batch calls.
Views have no batch loading yet.

## Refreshing before expiry

On a key in steady use, a TTL means the value expires, the next caller misses and waits, and
every hot key goes cold once per TTL. `Expirations` and `Loads` moving in step is the sign.
`RefreshAfter` moves that load off the request path:

```go
c := sanecache.New(sanecache.Options[int, *Rule]{
    TTL:          time.Minute,
    RefreshAfter: 40 * time.Second,
    Loader:       loadRule,
})
```

A read that finds a value older than `RefreshAfter` returns it at once and starts one load
in the background; a key nobody reads simply expires. The refresh is an ordinary load to
single flight and invalidation. The TTL still holds: a failed refresh is not retried, the
value lives out its TTL, and the next caller then sees the error, so a dead upstream shows
up one TTL later than without refresh, never more.

`GetOrLoad`, `GetManyOrLoad` and `GetOrLoadFunc` refresh with their loaders, `Get` and
`Lookup` with the ones in `Options`. `GetManyOrLoad` refreshes due keys in a batch of their
own. Views take their own `RefreshAfter`. `Stats().Refreshes` and `RefreshErrors` count
refreshes, apart from `Loads`.

## Several value types under one budget

One cache per type means one budget per type, and splitting memory between types up front
is the guess a byte budget was meant to avoid.

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

A view fixes one value type, prefixes its keys with its name and can bring its own `Cost`
and TTLs. Eviction stays global. Reads cost about 6 ns more than the cache underneath, and
allocate once name and key exceed 32 bytes. `Stats().TypeMisses` counts lookups that found
another type under a view's key: a bug detector for two views sharing a name.

Views load too:

```go
articles := sanecache.NewView(c, sanecache.ViewOptions[*Article]{
    Name:        "article",
    Cost:        func(a *Article) int64 { return a.ApproxBytes() },
    NegativeTTL: 30 * time.Second,
    Loader:      fetchArticle, // func(context.Context, string) (*Article, error)
})
a, err := articles.GetOrLoad(ctx, id)
```

The loader gets the key without the prefix and follows the rules of `Cache.GetOrLoad`.
Loads are shared within one `View` instance; the cache's own loader is never a fallback.
Counters belong to the view's name and are included in the cache's.

### Switching caching off

A view opened on a nil cache stores nothing, so that code calling `GetOrLoad` stays the same
when a TTL of zero means "do not cache":

```go
var cache *sanecache.Cache[string, any]
if ttl > 0 {
    cache = shared
}
articles := sanecache.NewView(cache, sanecache.ViewOptions[*Article]{
    Name:   "article",
    TTL:    ttl,
    Loader: fetchArticle,
})
```

It runs the loader on every call, still shared by concurrent callers, and keeps nothing.
Writes return `ErrDisabled`. Its counters are its own.

## The `Cost` function is the part worth getting right

`MaxBytes` is only as honest as `Cost`, and resident size is usually several times the
serialized size. Measure it once:

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

In one production service this came out at ~2.6× for decoded JSON structs, higher for
`map[string]any`, and ~6.7× for compiled templates.

## Sharding

`Shards` splits the cache into independently locked parts, rounded up to a power of two.
It is off by default: each shard gets an equal slice of the budget, and eviction becomes
per shard. On an M3 with GOMAXPROCS=8, 4096 keys, all readers on one cache:

| shards | `Get` (LRU) | `Get` (ClearOnFull) | `Set` | 90/10 mixed |
|-------:|------------:|--------------------:|------:|------------:|
| 1      | 131 ns      | 108 ns              | 195 ns| 145 ns      |
| 4      | 66 ns       | 37 ns               | 116 ns| 76 ns       |
| 16     | 40 ns       | 24 ns               | 76 ns | 47 ns       |
| 64     | 33 ns       | 19 ns               | 65 ns | 39 ns       |

At one lookup per request none of this matters, and `Shards: 0` is the right answer.

## Eviction policies

`LRU`, the default, evicts least recently used entries, so every read takes a write lock.
`ClearOnFull` drops the whole shard except the entry that overflowed it; reads then take a
read lock, worth 20% on one shard and 40% sharded. It suits flat access orders where
refilling is cheap, and costs hit rate: see the table below.

## The clock

An uncontended hit costs 47 ns, and 27 ns of that is `time.Now()`. `ClockGranularity` has a
background goroutine read the clock instead:

```go
sanecache.Options[string, *Article]{
    TTL:              10 * time.Minute,
    ClockGranularity: 100 * time.Millisecond,
}
```

A hit then costs 20 ns, and expiry is accurate to one interval either way: nothing against
ten minutes, a lot against one second, which is why it is off by default. After `Close`,
lookups go back to the wall clock.

## Stats

Counters are built in. Poll `Stats()`, and `ViewStats()` for every view by name:

```go
s := c.Stats()
// s.Hits, s.Misses, s.Negatives, s.TypeMisses, s.Evictions, s.Expirations,
// s.Replacements, s.Rejections, s.Loads, s.LoadNotFound, s.LoadErrors, s.Coalesced, s.Batches,
// s.Refreshes, s.RefreshErrors, s.Entries, s.Bytes, s.HitRate()

for name, vs := range c.ViewStats() {
    // vs.Hits, vs.Misses, vs.Negatives, vs.TypeMisses, vs.Loads, vs.LoadNotFound,
    // vs.LoadErrors, vs.Coalesced, vs.Refreshes, vs.RefreshErrors, vs.HitRate()
}
```

Alert on `Rejections`: keys that can never be cached. `Coalesced` against `Loads` is what
single flight saves. `LoadErrors` leaves out `ErrNotFound`, which is in `LoadNotFound`, so
ids that are gone do not look like failures.

## Lifecycle

A background goroutine sweeps expired entries off the budget. `Close()` stops it and the
clock goroutine; dropping the cache stops them too. With `DisableCleanup: true` entries
expire only on lookup.

## How it compares

The `benchmarks/` module, separate so the root stays dependency-free, measures this cache
against the ones it would replace: `make bench-compare`. Same machine, Go 1.24, 256-byte
values, a budget of 4096 of them, medians of five runs. Each sanecache row adds a knob to the
row above.

| ns/op | serial `Get` | `Get` ×8 | `Set` ×8 | 90/10 ×8 |
|---|---:|---:|---:|---:|
| sanecache, defaults | 62 | 141 | 229 | 156 |
| ⤷ `Shards: 16` | 64 | 42 | 90 | 49 |
| ⤷ + `ClockGranularity` | 33 | 32 | 81 | 38 |
| ⤷ + `ClearOnFull` | 31 | 19 | 86 | 38 |
| [otter](https://github.com/maypok86/otter) v2 | 80 | 14 | 268 | 29 |
| [theine](https://github.com/Yiling-J/theine-go) | 91 | 13 | 174 | 57 |
| [ristretto](https://github.com/dgraph-io/ristretto) v2 | 87 | 21 | 252 | 70 |
| [sturdyc](https://github.com/viccon/sturdyc) | 62 | 32 | 121 | 67 |
| [golang-lru](https://github.com/hashicorp/golang-lru) `v2/expirable` | 54 | 140 | 185 | 168 |
| [ttlcache](https://github.com/jellydator/ttlcache) v3 | 63 | 175 | 264 | 193 |

Per lookup this cache is *cheaper* than otter, theine and ristretto, which maintain a
frequency sketch and ring buffers. What those buy is scaling: otter and theine turn eight
cores into 6–7x the throughput, this one into 1.6x, because reads take a lock. At one lookup
per request, 19 ns against 13 ns is nothing; it matters when a request does thousands of
lookups. Writes are the other way round, 81–90 ns against 121–268: an admission policy costs
more than a lock.

What usually matters more is how much of a fixed budget turns into hits. 4096 entries
against 100,000 keys:

| %hit | zipf s=1.20 | zipf s=1.01 | zipf + scan |
|---|---:|---:|---:|
| sanecache (LRU) | 87.3 | 65.8 | 78.0 |
| sanecache (ClearOnFull) | 83.2 | 57.9 | 73.7 |
| otter v2 | 88.9 | **71.3** | **80.8** |
| theine | **89.0** | **71.3** | 80.7 |
| ristretto v2 | 87.3 | 68.7 | 79.0 |
| sturdyc | 84.9 | 61.6 | 75.3 |
| golang-lru `expirable` | 87.3 | 65.8 | 78.0 |
| ttlcache v3 | 87.3 | 65.8 | 78.0 |

When the hot set fits, policies differ by 1.7 points. When it does not, W-TinyLFU is worth
5.5 points over LRU, and that much upstream traffic outweighs every nanosecond above: the
real reason to pick otter or theine. `ClearOnFull` pays 7.9 points for its cheap reads;
sturdyc, dropping a full shard's oldest tenth at once, pays 4.

### The features are not the difference either

As of otter v2.3.0, [theine](https://github.com/Yiling-J/theine-go) v0.6.2,
[sturdyc](https://github.com/viccon/sturdyc) v1.1.6 and ttlcache v3.4.1:

| | one load per key | batch load | refresh ahead | budget by cost | "not found" remembered | value over budget |
|---|---|---|---|---|---|---|
| sanecache | `GetOrLoad` | `GetManyOrLoad` | `RefreshAfter` | `MaxBytes` + `Cost` | `SetNegative` | `ErrTooLarge` |
| otter v2 | `Get` + `Loader` | `BulkGet` | `RefreshCalculator` | `MaximumWeight` + `Weigher` | no: `ErrNotFound` deletes | accepted, then evicted |
| theine | loading cache | — | — | `Cost` | — | `Set` returns `false` |
| sturdyc | `GetOrFetch` | `GetOrFetchBatch` | `WithEarlyRefreshes` | — | `WithMissingRecordStorage` | — |
| ttlcache v3 | `SuppressedLoader` | — | — | `WithMaxCost` | — | evicts everything, itself last |

Others do things this one does not: otter and theine save the cache to a file and load it
back; sturdyc retries failed refreshes, batches refreshes across requests, sits in front of a
distributed store, and holds several value types under a count of entries. What is left here
is the combination: a byte budget that refuses what does not fit, missing records charged
against it, and value types with costs of their own
[under one budget](#several-value-types-under-one-budget).

## When to use something else

- The best hit rate for a memory budget is worth admission policies and frequency
  estimation → [otter](https://github.com/maypok86/otter). The hit-rate table above is the
  size of the prize.
- How a cache scales across cores is a real difference on your read path → otter or
  [ristretto](https://github.com/dgraph-io/ristretto); with ristretto, budget for `Wait()`
  in the tests.
- You want refreshes retried, refreshes from many requests batched, or a distributed store
  behind the cache → [sturdyc](https://github.com/viccon/sturdyc).
- GC pressure from millions of live pointers is your actual problem →
  [bigcache](https://github.com/allegro/bigcache) or
  [freecache](https://github.com/coocood/freecache), which store entries in byte buffers
  with few pointers, still on the Go heap
  ([BigCache](https://github.com/allegro/bigcache#how-it-works),
  [FreeCache](https://github.com/coocood/freecache#how-it-is-done)).
- A bounded LRU with TTL and nothing else →
  [hashicorp/golang-lru](https://github.com/hashicorp/golang-lru)'s `v2/expirable`.

## Status

v0.7. The API above is what exists and is tested; expect it to move before v1.
Contributions are welcome — see [CONTRIBUTING.md](CONTRIBUTING.md) for what this library
optimises for. MIT licensed.
