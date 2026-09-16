# Changelog

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project follows [semantic versioning](https://semver.org/spec/v2.0.0.html).
While the major version is 0, the public API may change in any release.

## [Unreleased]

### Added

- `NewView` accepts a nil cache and opens a view with caching switched off: `GetOrLoad`
  runs the loader on every call, still coalescing concurrent callers, and keeps nothing.
  Its lookups are misses, `Delete` reports false, and writes return the new `ErrDisabled`.
  `NewView(nil, …)` used to panic.
- `Cache.ViewStats`, a snapshot of the counters of every view opened on the cache, keyed
  by view name. An exporter no longer needs its own registry of views.

### Changed

- View counters belong to the view name rather than the instance: views opened with the
  same name on the same cache share one set of counters, as they already share keys.
  `View.Stats` on either instance now reports both. Code that opened several instances of
  one name and read their counters separately must read them as one.

## [0.4.0] - 2026-09-09

### Changed

- `Delete` (including absent keys), `Clear`, and successful explicit writes now
  prevent outstanding loaders from publishing obsolete results. New callers no
  longer join invalidated flights; existing waiters still receive their result.
- The guarantee applies to cache loaders and typed view loaders across the same
  storage key, including separate views sharing a namespace. Successful loader
  publications also supersede competing outstanding loads. Rejected writes leave
  outstanding loads unchanged.
- A cancelled loader that ignores its context may still warm the cache, but can
  no longer overwrite a successful write or repopulate an explicitly invalidated
  key. No public signatures changed; callers relying on the old late-publication
  behaviour must account for the stronger invalidation guarantee.

### Documentation

- Documented invalidation ordering, existing-waiter results, concurrent `Clear`,
  and the distinction between invalidation and `Close`.

## [0.3.0] - 2026-09-07

### Added

- Runnable basic-cache and HTTP-loader examples, checked by the existing test matrix,
  plus a complete first program and installation commands in the README.
- `ViewOptions.Loader` and `View.GetOrLoad`, with single-flight per view instance,
  typed results, per-view cost and TTLs, negative caching, cancellation and panic
  handling matching the cache loader. View loaders receive unprefixed keys and
  operate independently from the parent cache's loader.
- `ViewStats.Loads`, `ViewStats.LoadErrors` and `ViewStats.Coalesced`, also included
  in the parent cache's counters and controlled by `Options.DisableStats`.

### Changed

- `ViewOptions` and `ViewStats` gained fields. Update positional struct literals
  to use named fields; existing named-field literals continue to work.

### Documentation

- Clarified that `singleflight.Group` leaves context and cancellation policy to its caller,
  and that BigCache and FreeCache reduce pointer scanning with on-heap byte buffers.

## [0.2.0] - 2026-09-06

### Added

- `GetOrLoad` and `Options.Loader`: a cold key is fetched once however many
  callers ask for it at the same moment. `ErrNotFound` from a loader is cached as
  a negative entry and reported back to every caller, including later ones; other
  errors are passed through unchanged and not cached. A caller that gives up does
  not cancel the load the others are waiting on, and a panicking loader reaches
  the callers rather than the process.
- Typed views over a shared byte budget: `NewView`, `ViewOptions`, `View` and
  `ViewStats`, for a cache that holds several value types under one budget. Each
  view fixes a value type, namespaces its keys with its name, and may bring its
  own `Cost` and TTLs.
- `Options.ClockGranularity`: a background goroutine holds the time, taking the
  wall-clock read off the lookup path. Expiry is then accurate to within one
  interval in either direction. A hit costs 20 ns rather than 47.
- `Stats.Loads`, `Stats.LoadErrors`, `Stats.Coalesced` and `Stats.TypeMisses`.
- Comparison benchmarks against otter v2, ristretto v2, `golang-lru/v2/expirable`
  and ttlcache v3, in a separate `benchmarks` module so that the root module
  keeps its promise of no dependencies. `make bench-compare` runs them.

### Changed

- Counters moved from the cache to the shards, on their own cache lines. One
  shared counter meant every core in the process writing to one cache line on
  every lookup, which held reads to a fifth of their throughput however many
  shards they were spread across — sharding could not help, because the lock was
  never what they were queuing for. Concurrent `Get` at 16 shards went from 64 ns
  to 40 ns under `LRU` and from 55 ns to 24 ns under `ClearOnFull`. `Stats` is
  unchanged: it already walked every shard.
- `Stats` gained fields, which moves `Entries` and `Bytes` within the struct.
  Code that builds a `Stats` with positional fields needs updating; code that
  reads fields by name does not.

## [0.1.0] - 2026-09-06

First cut.

### Added

- `Cache[K, V]` with TTL, optional jitter, and lazy plus background expiry.
- Byte budgets via `MaxBytes` and `Cost`, alongside `MaxEntries`.
- `LRU` and `ClearOnFull` eviction policies.
- First-class negative caching: `SetNegative`, `SetNegativeTTL`, `StatusNegative`.
- Optional sharding via `Shards`.
- Built-in counters via `Stats`, and an `OnEvict` callback carrying a reason.
- `ErrTooLarge` from `Set` for values that can never fit.

[Unreleased]: https://github.com/andared/sanecache/compare/v0.4.0...HEAD
[0.4.0]: https://github.com/andared/sanecache/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/andared/sanecache/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/andared/sanecache/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/andared/sanecache/releases/tag/v0.1.0
