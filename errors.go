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
)
