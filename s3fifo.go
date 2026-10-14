package sanecache

import "hash/maphash"

// S3FIFO follows Yang et al., "FIFO queues are all you need for cache eviction"
// (SOSP 2023).

const (
	// maxFreq is where an entry's read count stops.
	maxFreq = 3
	// smallShare is the fraction of the budget, one in smallShare, that the
	// small queue holds before it has to give up entries.
	smallShare = 10
)

// admit queues a new entry: into the main queue if it replaces an entry there,
// taking over its read count, or if its key is a ghost; into the small queue
// otherwise.
func (s *shard[K, V]) admit(e, replaced *entry[K, V]) {
	switch {
	case replaced != nil && !replaced.small:
		e.freq.Store(replaced.freq.Load())
		s.pushFront(e)
	case s.ghosts.take(maphash.Comparable(s.ghosts.seed, e.key)):
		s.pushFront(e)
	default:
		e.small = true
		s.smallBytes += e.cost
		s.smallLen++
		e.prev, e.next = nil, s.smallHead
		if s.smallHead != nil {
			s.smallHead.prev = e
		}
		s.smallHead = e
		if s.smallTail == nil {
			s.smallTail = e
		}
	}
}

func (s *shard[K, V]) unlinkSmall(e *entry[K, V]) {
	if e.prev != nil {
		e.prev.next = e.next
	} else if s.smallHead == e {
		s.smallHead = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	} else if s.smallTail == e {
		s.smallTail = e.prev
	}
	e.prev, e.next = nil, nil
	s.smallBytes -= e.cost
	s.smallLen--
}

// smallFull reports whether the small queue holds its share of the budget.
func (s *shard[K, V]) smallFull() bool {
	if s.maxBytes > 0 && s.smallBytes >= max(s.maxBytes/smallShare, 1) {
		return true
	}

	return s.maxEntries > 0 && s.smallLen >= max(s.maxEntries/smallShare, 1)
}

// evictS3FIFO is evict for S3FIFO, never evicting keep. The small queue gives
// up its oldest entry while it holds its share, or when the main queue has
// nothing but keep to give: to the main queue if it was read since it came in,
// out as a ghost otherwise. The main queue's oldest entry goes round again,
// one read the poorer, until it has none left.
func (s *shard[K, V]) evictS3FIFO(keep *entry[K, V]) []*entry[K, V] {
	var victims []*entry[K, V]
	for s.over() {
		mainEmpty := s.tail == nil || (s.tail == keep && keep.prev == nil)
		if t := s.smallTail; t != nil && t != keep && (s.smallFull() || mainEmpty) {
			s.unlinkSmall(t)
			if t.freq.Load() > 0 {
				t.small = false
				t.freq.Store(0)
				s.pushFront(t)

				continue
			}
			delete(s.items, t.key)
			s.bytes -= t.cost
			s.ghosts.add(maphash.Comparable(s.ghosts.seed, t.key), len(s.items))
			victims = append(victims, t)

			continue
		}

		// keep can be the oldest once entries promoted after it went in front.
		t := s.tail
		if t == keep {
			t = t.prev
		}
		if t == nil {
			break
		}
		if f := t.freq.Load(); f > 0 {
			t.freq.Store(f - 1)
			s.touch(t)

			continue
		}
		s.remove(t)
		victims = append(victims, t)
	}

	return victims
}

// ghosts remembers the hashes of the keys the small queue let out, up to as
// many as the shard holds entries. Hashes rather than keys, so that ghosts cost
// no memory outside the budget beyond their own few bytes; a collision only
// lets a key into the main queue early.
type ghosts struct {
	seed maphash.Seed
	// set maps a ghost's hash to the number of the add that made it one, so
	// that a hash added again is not forgotten when its older add leaves.
	set map[uint64]uint64
	// queue holds hashes in the order they were added, queue[i] as add
	// number first+i.
	queue []uint64
	first uint64
}

// add makes h a ghost, forgetting the oldest beyond limit.
func (g *ghosts) add(h uint64, limit int) {
	g.set[h] = g.first + uint64(len(g.queue))
	g.queue = append(g.queue, h)
	for len(g.queue) > max(limit, 1) {
		if old := g.queue[0]; g.set[old] == g.first {
			delete(g.set, old)
		}
		g.queue = g.queue[1:]
		g.first++
	}
}

// take reports whether h is a ghost, and forgets it.
func (g *ghosts) take(h uint64) bool {
	if _, ok := g.set[h]; !ok {
		return false
	}
	delete(g.set, h)

	return true
}

func (g *ghosts) clear() {
	clear(g.set)
	g.queue, g.first = nil, 0
}
