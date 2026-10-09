package sanecache

import "errors"

var (
	// ErrTooLarge is returned by Set when the value's cost exceeds the budget of
	// its shard, so that it could never be cached.
	ErrTooLarge = errors.New("sanecache: value cost exceeds the shard budget")

	// ErrNegativeDisabled is returned by SetNegative without a NegativeTTL: a
	// negative entry would otherwise never expire.
	ErrNegativeDisabled = errors.New("sanecache: negative caching is disabled (Options.NegativeTTL is unset)")

	// ErrNotFound is how a loader says the upstream has no such key, and how
	// GetOrLoad reports it, from a load or a cached negative entry alike. A
	// loader may wrap it: it is tested with errors.Is.
	ErrNotFound = errors.New("sanecache: the upstream has no such key")

	// ErrDisabled is returned by the writes of a view opened on a nil cache.
	ErrDisabled = errors.New("sanecache: the view has no cache to write to")

	// ErrNoLoader is returned by GetOrLoad and GetManyOrLoad without a loader,
	// and by GetOrLoadFunc given a nil function.
	ErrNoLoader = errors.New("sanecache: loading requires a loader (Options.Loader, Options.BatchLoader or ViewOptions.Loader)")
)
