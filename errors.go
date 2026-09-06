package sanecache

import "errors"

var (
	// ErrTooLarge is returned by Set when the value's cost exceeds the budget of
	// the shard it hashes to, so caching it could never succeed. It is reported
	// rather than swallowed: a steady trickle of ErrTooLarge means every request
	// for those keys goes to the upstream, which is invisible in the hit rate.
	ErrTooLarge = errors.New("sanecache: value cost exceeds the shard budget")

	// ErrNegativeDisabled is returned by SetNegative when Options.NegativeTTL was
	// not set. Negative entries without a TTL would pin a "does not exist" answer
	// for the lifetime of the process.
	ErrNegativeDisabled = errors.New("sanecache: negative caching is disabled (Options.NegativeTTL is unset)")

	// ErrNotFound is how a loader says the upstream has no such key, and how
	// GetOrLoad reports that answer back — including when it comes from a cached
	// negative entry rather than a fresh call. It is the same error either way,
	// so a caller's handling of "no such object" does not depend on whether the
	// cache happened to remember it.
	//
	// A loader may wrap it: GetOrLoad tests with errors.Is and passes the
	// loader's own error through to the caller that triggered the load.
	ErrNotFound = errors.New("sanecache: the upstream has no such key")

	// ErrNoLoader is returned by GetOrLoad when Options.Loader was not set.
	ErrNoLoader = errors.New("sanecache: GetOrLoad requires Options.Loader")
)
