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

	Loads int64 // keys loaded by Loader or BatchLoader, successfully or not
	// LoadNotFound counts the loads that ended in ErrNotFound, a key missing
	// from a batch answer included. It is an answer rather than a failure, so
	// LoadErrors does not count it.
	LoadNotFound int64
	LoadErrors   int64 // loads that failed: any other error, or a panic
	// Batches counts BatchLoader calls. Against Loads it says how many keys an
	// upstream call carries on average.
	Batches int64
	// Coalesced counts the GetOrLoad calls that another caller's load spared
	// from starting one of their own, whether they waited for it or arrived just
	// after it published. Against Loads it says how much the single flight is
	// actually saving.
	Coalesced int64
	// Refreshes counts the reloads RefreshAfter started that finished, however
	// they ended, a key at a time as Loads does. They are not in Loads: Loads is
	// what misses cost the upstream, and Refreshes is what keeping hot keys warm
	// costs it instead.
	Refreshes int64
	// RefreshErrors counts the refreshes that failed or panicked. The value they
	// meant to replace stays until its TTL runs out. A refresh that ended in
	// ErrNotFound is not one of them.
	RefreshErrors int64

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
