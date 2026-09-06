# Runnable examples

Requires Go 1.24 or newer. Run these commands from the repository root:

```sh
go run ./examples/basic
go run ./examples/http_loader
```

## Basic cache

[`basic/main.go`](basic/main.go) creates a bounded cache, writes a value, reads it, and closes
the cache. It prints:

```text
hello true
```

## Caching an HTTP dependency

[`http_loader/main.go`](http_loader/main.go) starts a small HTTP server on a free loopback port
and fetches JSON through `GetOrLoad`. A successful response becomes a cached value; an HTTP 404
becomes `ErrNotFound` and is remembered through `NegativeTTL`. Other errors are returned without
being cached. Requests carry the loader context and have a five-second HTTP timeout.

Each ID is requested twice, but only the first request for each reaches the HTTP server:

```text
Caching HTTP responses
Caching HTTP responses
article not found
article not found
upstream requests: 2
```

The server and cache stop when the program exits. No external service or credentials are needed.
Both examples have output checks and run as part of `go test ./...` and the existing CI matrix.
