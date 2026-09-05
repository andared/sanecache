package sanecache

import "sync/atomic"

// Stats is a snapshot of the cache counters. Counters are cumulative since the
// cache was created; Entries and Bytes are instantaneous.
type Stats struct {
	Hits         int64 // lookups that returned a value
	Misses       int64 // lookups that found nothing
	Negatives    int64 // lookups that found a cached "does not exist"
	Evictions    int64 // entries dropped to stay inside the budget
	Expirations  int64 // entries dropped because their TTL ran out
	Replacements int64 // entries overwritten by a later Set
	Rejections   int64 // Set calls refused with ErrTooLarge

	Entries int   // entries currently held, expired-but-not-yet-swept included
	Bytes   int64 // sum of the costs of those entries
}

// HitRate reports hits as a fraction of all lookups. A cached negative answer
// counts as a hit: it saved the same upstream call a positive one would have.
func (s Stats) HitRate() float64 {
	total := s.Hits + s.Misses + s.Negatives
	if total == 0 {
		return 0
	}

	return float64(s.Hits+s.Negatives) / float64(total)
}

type counters struct {
	hits         atomic.Int64
	misses       atomic.Int64
	negatives    atomic.Int64
	evictions    atomic.Int64
	expirations  atomic.Int64
	replacements atomic.Int64
	rejections   atomic.Int64
}
