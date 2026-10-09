package sanecache

import "sync/atomic"

// Stats is a snapshot of the cache counters, cumulative except Entries and Bytes.
type Stats struct {
	Hits         int64 // lookups that returned a value
	Misses       int64 // lookups that found nothing
	Negatives    int64 // lookups that found a cached "does not exist"
	Evictions    int64 // entries dropped to stay inside the budget
	Expirations  int64 // entries dropped because their TTL ran out
	Replacements int64 // entries overwritten by a later Set
	Rejections   int64 // Set calls refused with ErrTooLarge

	// TypeMisses counts view lookups that found another type. Hits counts them
	// too, since the cache had the key.
	TypeMisses int64

	Loads int64 // keys loaded by Loader or BatchLoader, successfully or not
	// LoadNotFound counts loads that ended in ErrNotFound, keys missing from a
	// batch answer included; LoadErrors does not.
	LoadNotFound int64
	LoadErrors   int64 // loads that failed: any other error, or a panic
	Batches      int64 // BatchLoader calls; Loads/Batches is keys per call
	Coalesced    int64 // GetOrLoad calls spared a load by another caller's
	// Refreshes counts finished refreshes, by key. Loads does not include them:
	// it is what misses cost.
	Refreshes int64
	// RefreshErrors counts refreshes that failed or panicked, not ErrNotFound.
	RefreshErrors int64

	Entries int   // entries currently held, expired-but-not-yet-swept included
	Bytes   int64 // sum of the costs of those entries
}

// HitRate reports hits, cached negatives included, as a fraction of lookups.
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
	s.LoadNotFound += c.loadNotFound.Load()
	s.LoadErrors += c.loadErrors.Load()
	s.Batches += c.batches.Load()
	s.Coalesced += c.coalesced.Load()
	s.Refreshes += c.refreshes.Load()
	s.RefreshErrors += c.refreshErrors.Load()
}

// countLoad records one completed load, or refresh when it was one.
func (c *counters) countLoad(o loadOutcome, refresh bool) {
	if refresh {
		c.refreshes.Add(1)
		if o == loadFailed {
			c.refreshErrors.Add(1)
		}

		return
	}

	c.loads.Add(1)
	switch o {
	case loadNotFound:
		c.loadNotFound.Add(1)
	case loadFailed:
		c.loadErrors.Add(1)
	}
}

type counters struct {
	hits          atomic.Int64
	misses        atomic.Int64
	negatives     atomic.Int64
	typeMisses    atomic.Int64
	evictions     atomic.Int64
	expirations   atomic.Int64
	replacements  atomic.Int64
	rejections    atomic.Int64
	loads         atomic.Int64
	loadNotFound  atomic.Int64
	loadErrors    atomic.Int64
	batches       atomic.Int64
	coalesced     atomic.Int64
	refreshes     atomic.Int64
	refreshErrors atomic.Int64
}
