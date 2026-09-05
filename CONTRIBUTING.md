# Contributing

Thanks for looking. This is a small library with an opinion, so it helps to know
the opinion before proposing changes.

## What this library optimises for

Predictability first, throughput second. Concretely, a change is unlikely to land
if it makes any of these less true:

- a value is readable the moment `Set` returns;
- work that cannot succeed reports an error instead of failing quietly later;
- a limit the caller set is a limit they can reason about;
- there are no dependencies.

That last one is a hard rule, not a preference. It is why stats are counters
rather than a metrics interface, and why the benchmark comparisons against other
caches live in their own module.

Faster is welcome when it does not cost any of the above. If it does, the trade
belongs in the README under "when to use something else" rather than in the code.

## Working on it

```
make test    # race detector, twice, to shake out ordering assumptions
make lint    # golangci-lint plus gofmt
make bench   # the numbers in the README
make cover   # coverage report
```

Requires Go 1.24 or newer, which is also what CI tests against alongside the
current release.

## Pull requests

- Tests for behaviour, not for coverage. A test that would still pass if the
  logic were inverted is worse than no test.
- Performance claims need a benchmark in the same PR. "Should be faster" is a
  hypothesis; `benchstat` output is a result.
- Comments explain why, not what. The code already says what.
- One logical change per PR.

Changes to the public API before v1 are fine, but they need a line in
`CHANGELOG.md` saying what breaks and what to do instead.
