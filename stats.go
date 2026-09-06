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

	// TypeMisses counts view lookups that found an entry holding some other
	// type. Hits counts those too, because the cache did have the key; the pair
	// is what tells a namespace collision apart from a plain miss.
	TypeMisses int64

	Loads      int64 // Loader calls that finished, successfully or not
	LoadErrors int64 // of those, the ones that returned an error
	// Coalesced counts the GetOrLoad calls that another caller's load spared
	// from starting one of their own, whether they waited for it or arrived just
	// after it published. Against Loads it says how much the single flight is
	// actually saving.
	Coalesced int64

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

// addTo folds one shard's counters into a snapshot.
func (c *counters) addTo(s *Stats) {
	s.Hits += c.hits.Load()
	s.Misses += c.misses.Load()
	s.Negatives += c.negatives.Load()
	s.TypeMisses += c.typeMisses.Load()
	s.Evictions += c.evictions.Load()
	s.Expirations += c.expirations.Load()
	s.Replacements += c.replacements.Load()
	s.Rejections += c.rejections.Load()
	s.Loads += c.loads.Load()
	s.LoadErrors += c.loadErrors.Load()
	s.Coalesced += c.coalesced.Load()
}

type counters struct {
	hits         atomic.Int64
	misses       atomic.Int64
	negatives    atomic.Int64
	typeMisses   atomic.Int64
	evictions    atomic.Int64
	expirations  atomic.Int64
	replacements atomic.Int64
	rejections   atomic.Int64
	loads        atomic.Int64
	loadErrors   atomic.Int64
	coalesced    atomic.Int64
}
