package sanecache

import "sync"

// entry is a single cached item. Entries are linked into an intrusive LRU list
// so that eviction needs no allocation and no side table.
type entry[K comparable, V any] struct {
	key       K
	value     V
	cost      int64
	expiresAt int64 // unix nanoseconds; 0 means "never expires"
	negative  bool  // upstream said this key does not exist

	prev, next *entry[K, V] // head of the list is the most recently used entry
}

func (e *entry[K, V]) expired(now int64) bool {
	return e.expiresAt != 0 && now >= e.expiresAt
}

// shard is an independently locked slice of the cache. A cache with one shard
// is a plain map behind a single lock; more shards trade exact budget accounting
// for less lock contention.
type shard[K comparable, V any] struct {
	mu    sync.RWMutex
	items map[K]*entry[K, V]

	head, tail *entry[K, V]

	bytes      int64
	maxBytes   int64
	maxEntries int
	policy     Policy
}

func newShard[K comparable, V any](maxBytes int64, maxEntries int, policy Policy) *shard[K, V] {
	return &shard[K, V]{
		items:      make(map[K]*entry[K, V]),
		maxBytes:   maxBytes,
		maxEntries: maxEntries,
		policy:     policy,
	}
}

// get returns the entry's value along with its status. The third result reports
// that the lookup landed on an entry that had already expired, so the caller can
// account for it separately from a plain miss.
func (s *shard[K, V]) get(key K, now int64) (V, Status, bool) {
	var zero V

	// Under ClearOnFull nothing is reordered on access, so reads take only the
	// read lock. That is the whole point of the policy: keeping LRU order costs
	// a write lock on every read.
	if s.policy == ClearOnFull {
		s.mu.RLock()
		e, ok := s.items[key]
		if !ok {
			s.mu.RUnlock()
			return zero, StatusMiss, false
		}
		if e.expired(now) {
			s.mu.RUnlock()

			// Drop it under the write lock rather than leaving it for the
			// sweeper: an expired entry that stays in the map would be counted
			// as an expiration again on every lookup, and would go on holding
			// budget in the meantime. Only the goroutine that removes it counts it.
			s.mu.Lock()
			cur, still := s.items[key]
			removed := still && cur == e
			if removed {
				s.remove(cur)
			}
			s.mu.Unlock()

			return zero, StatusMiss, removed
		}

		status, value := StatusHit, e.value
		if e.negative {
			status, value = StatusNegative, zero
		}
		s.mu.RUnlock()

		return value, status, false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.items[key]
	if !ok {
		return zero, StatusMiss, false
	}
	if e.expired(now) {
		s.remove(e)
		return zero, StatusMiss, true
	}

	s.touch(e)
	if e.negative {
		return zero, StatusNegative, false
	}

	return e.value, StatusHit, false
}

// set stores e, returning the entry it replaced (if any) and the entries evicted
// to stay inside the budget. A value that cannot fit on its own is rejected with
// ErrTooLarge rather than stored and silently dropped later.
func (s *shard[K, V]) set(e *entry[K, V]) (replaced *entry[K, V], victims []*entry[K, V], err error) {
	if s.maxBytes > 0 && e.cost > s.maxBytes {
		return nil, nil, ErrTooLarge
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if old, ok := s.items[e.key]; ok {
		s.remove(old)
		replaced = old
	}

	s.items[e.key] = e
	s.bytes += e.cost
	s.pushFront(e)

	return replaced, s.evict(e), nil
}

func (s *shard[K, V]) delete(key K) (*entry[K, V], bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.items[key]
	if !ok {
		return nil, false
	}
	s.remove(e)

	return e, true
}

// sweep drops every entry that has expired by now.
func (s *shard[K, V]) sweep(now int64) []*entry[K, V] {
	s.mu.Lock()
	defer s.mu.Unlock()

	var expired []*entry[K, V]
	for _, e := range s.items {
		if e.expired(now) {
			s.remove(e)
			expired = append(expired, e)
		}
	}

	return expired
}

func (s *shard[K, V]) clear() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.items = make(map[K]*entry[K, V])
	s.head, s.tail, s.bytes = nil, nil, 0
}

func (s *shard[K, V]) stats() (entries int, bytes int64) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.items), s.bytes
}

// evict brings the shard back inside its budget. keep is the entry that was just
// stored: ClearOnFull wipes everything else rather than picking victims, but
// dropping the write that triggered the wipe would only force an immediate refetch.
func (s *shard[K, V]) evict(keep *entry[K, V]) []*entry[K, V] {
	if !s.over() {
		return nil
	}

	if s.policy == ClearOnFull {
		victims := make([]*entry[K, V], 0, len(s.items)-1)
		for _, e := range s.items {
			if e != keep {
				victims = append(victims, e)
			}
		}
		s.items = map[K]*entry[K, V]{keep.key: keep}
		s.head, s.tail = keep, keep
		keep.prev, keep.next = nil, nil
		s.bytes = keep.cost

		return victims
	}

	var victims []*entry[K, V]
	for s.over() && s.tail != nil && s.tail != keep {
		v := s.tail
		s.remove(v)
		victims = append(victims, v)
	}

	return victims
}

func (s *shard[K, V]) over() bool {
	return (s.maxBytes > 0 && s.bytes > s.maxBytes) ||
		(s.maxEntries > 0 && len(s.items) > s.maxEntries)
}

// remove unlinks e from both the map and the LRU list. Callers hold the lock.
func (s *shard[K, V]) remove(e *entry[K, V]) {
	delete(s.items, e.key)
	s.unlink(e)
	s.bytes -= e.cost
}

func (s *shard[K, V]) touch(e *entry[K, V]) {
	if s.head == e {
		return
	}
	s.unlink(e)
	s.pushFront(e)
}

func (s *shard[K, V]) pushFront(e *entry[K, V]) {
	e.prev, e.next = nil, s.head
	if s.head != nil {
		s.head.prev = e
	}
	s.head = e
	if s.tail == nil {
		s.tail = e
	}
}

func (s *shard[K, V]) unlink(e *entry[K, V]) {
	if e.prev != nil {
		e.prev.next = e.next
	} else if s.head == e {
		s.head = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	} else if s.tail == e {
		s.tail = e.prev
	}
	e.prev, e.next = nil, nil
}
