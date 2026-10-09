package sanecache

import (
	"sync"
	"sync/atomic"
)

// entry is a cached item, linked into an intrusive LRU list so that eviction
// needs no allocation.
type entry[K comparable, V any] struct {
	key       K
	value     V
	cost      int64
	expiresAt int64 // unix nanoseconds; 0 means "never expires"

	// refreshAt is when a read should start reloading the entry, in unix
	// nanoseconds; 0 means never. A read claims the refresh by swapping it to 0,
	// under what may be only the read lock, hence atomic.
	refreshAt atomic.Int64

	negative bool // upstream said this key does not exist
	// byView marks an entry a View wrote, which only a view may refresh: the
	// cache's loader would be handed the prefixed key.
	byView bool

	prev, next *entry[K, V] // head of the list is the most recently used entry
}

// refresher is who reads an entry, for claiming its refresh, and who wrote it.
type refresher uint8

const (
	refreshNone refresher = iota
	refreshCache
	refreshView
)

// claimRefresh reports whether a read by by is the one to refresh e. The caller
// holds the shard lock and has ruled out refreshNone, so that most reads skip
// the call.
func (e *entry[K, V]) claimRefresh(by refresher, now int64) bool {
	if e.byView != (by == refreshView) {
		return false
	}
	at := e.refreshAt.Load()

	return at != 0 && now >= at && e.refreshAt.CompareAndSwap(at, 0)
}

func (e *entry[K, V]) expired(now int64) bool {
	return e.expiresAt != 0 && now >= e.expiresAt
}

// shard is an independently locked slice of the cache.
type shard[K comparable, V any] struct {
	mu    sync.RWMutex
	items map[K]*entry[K, V]

	// Only active loads are tracked; invalidation never leaves per-key tombstones.
	loads map[K]map[*loadToken]struct{}

	head, tail *entry[K, V]

	bytes      int64
	maxBytes   int64
	maxEntries int
	policy     Policy

	// Counters per shard, padded off the fields every read reads: one shared
	// counter line held reads to a fifth of their throughput however many shards
	// they used, and without padding a read lock cost more than the write lock it
	// avoids (padding took one ClearOnFull shard from 161 ns to 109).
	_        [cacheLine]byte
	counters counters
	_        [cacheLine]byte
}

// cacheLine is 64 bytes almost everywhere; being wrong costs only padding.
const cacheLine = 64

func newShard[K comparable, V any](maxBytes int64, maxEntries int, policy Policy) *shard[K, V] {
	return &shard[K, V]{
		items:      make(map[K]*entry[K, V]),
		maxBytes:   maxBytes,
		maxEntries: maxEntries,
		policy:     policy,
	}
}

// get returns the entry's value and status. expired reports a miss on an entry
// that had expired; refresh, that the hit was due and the read by by claimed it.
func (s *shard[K, V]) get(key K, now int64, by refresher) (_ V, _ Status, expired, refresh bool) {
	var zero V

	// ClearOnFull reorders nothing on access, so reads take only the read lock:
	// that is the point of the policy.
	if s.policy == ClearOnFull {
		s.mu.RLock()
		e, ok := s.items[key]
		if !ok {
			s.mu.RUnlock()
			return zero, StatusMiss, false, false
		}
		if e.expired(now) {
			s.mu.RUnlock()

			// Dropped now, or every lookup until the sweep would count it as an
			// expiration again. Only the goroutine that removes it counts it.
			s.mu.Lock()
			cur, still := s.items[key]
			removed := still && cur == e
			if removed {
				s.remove(cur)
			}
			s.mu.Unlock()

			return zero, StatusMiss, removed, false
		}

		status, value := StatusHit, e.value
		if e.negative {
			status, value = StatusNegative, zero
		}
		if by != refreshNone {
			refresh = e.claimRefresh(by, now)
		}
		s.mu.RUnlock()

		return value, status, false, refresh
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.items[key]
	if !ok {
		return zero, StatusMiss, false, false
	}
	if e.expired(now) {
		s.remove(e)
		return zero, StatusMiss, true, false
	}

	s.touch(e)
	if e.negative {
		return zero, StatusNegative, false, false
	}

	if by != refreshNone {
		refresh = e.claimRefresh(by, now)
	}

	return e.value, StatusHit, false, refresh
}

// set stores e, returning the entry it replaced and those evicted to make room.
// A value that cannot fit on its own is refused with ErrTooLarge.
func (s *shard[K, V]) set(e *entry[K, V], tokens ...*loadToken) (replaced *entry[K, V], victims []*entry[K, V], err error) {
	if s.maxBytes > 0 && e.cost > s.maxBytes {
		return nil, nil, ErrTooLarge
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if len(tokens) > 0 && !tokens[0].valid.Load() {
		return nil, nil, nil
	}
	s.invalidateLoads(e.key)

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

	s.invalidateLoads(key)
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

	for key := range s.loads {
		s.invalidateLoads(key)
	}
	s.items = make(map[K]*entry[K, V])
	s.head, s.tail, s.bytes = nil, nil, 0
}

func (s *shard[K, V]) stats() (entries int, bytes int64) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.items), s.bytes
}

// evict brings the shard back inside its budget, never evicting keep, the entry
// just stored: ClearOnFull wipes everything else.
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

// loadToken lets a write or delete of a storage key, through any view, stop the
// key's loads from publishing; it is checked under the shard lock.
type loadToken struct{ valid atomic.Bool }

func (s *shard[K, V]) beginLoad(key K) *loadToken {
	s.mu.Lock()
	defer s.mu.Unlock()
	token := &loadToken{}
	token.valid.Store(true)
	if s.loads == nil {
		s.loads = make(map[K]map[*loadToken]struct{})
	}
	if s.loads[key] == nil {
		s.loads[key] = make(map[*loadToken]struct{})
	}
	s.loads[key][token] = struct{}{}
	return token
}

func (s *shard[K, V]) endLoad(key K, token *loadToken) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.loads[key], token)
	if len(s.loads[key]) == 0 {
		delete(s.loads, key)
	}
}

// invalidateLoads is called with s.mu held, atomically with the mutation.
func (s *shard[K, V]) invalidateLoads(key K) {
	for token := range s.loads[key] {
		token.valid.Store(false)
	}
	delete(s.loads, key)
}
