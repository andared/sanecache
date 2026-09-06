# Changelog

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project follows [semantic versioning](https://semver.org/spec/v2.0.0.html).
While the major version is 0, the public API may change in any release.

## [Unreleased]

### Added

- Runnable basic-cache and HTTP-loader examples, checked by the existing test matrix,
  plus a complete first program and installation commands in the README.

### Planned

- A loader on views, so a typed view over a shared budget gets the same
  stampede protection the cache has.

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

[Unreleased]: https://github.com/andared/sanecache/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/andared/sanecache/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/andared/sanecache/releases/tag/v0.1.0
