# Changelog

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project follows [semantic versioning](https://semver.org/spec/v2.0.0.html).
While the major version is 0, the public API may change in any release.

## [Unreleased]

### Planned

- A loader with single-flight, so a cold key is fetched once rather than once per
  concurrent caller.
- Typed views over a shared byte budget, for a budget that holds several value types.
- A coarse clock, to take the wall-clock read off the lookup path.
- Comparison benchmarks against other caches, in a separate module.

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

[Unreleased]: https://github.com/andared/sanecache/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/andared/sanecache/releases/tag/v0.1.0
