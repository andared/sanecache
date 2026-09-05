# Security

## Reporting a vulnerability

Report privately through GitHub: **Security → Report a vulnerability** on this
repository. Please do not open a public issue for a security problem.

Expect an acknowledgement within a week.

## Scope

This library has no dependencies, does no I/O, and does not parse untrusted
input, so the realistic surface is small. Things that would count:

- a way to make the cache serve one key's value under another key;
- unbounded growth despite a configured `MaxBytes` or `MaxEntries`;
- a data race or memory-safety problem reachable through the public API.

Note that `Jitter` uses `math/rand/v2`. It spreads expiry times to avoid
synchronised upstream load; it is not a security control and expiry times are not
meant to be unpredictable to an attacker.

## Versions

Only the latest release is supported. While the major version is 0, that means
the latest tag.
