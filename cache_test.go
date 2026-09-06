package sanecache

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func unitCost[V any]() func(V) int64 { return func(V) int64 { return 1 } }

// waitFor polls cond until it holds. It is for the tests whose subject is a
// background goroutine, where the alternative is a sleep long enough to be slow
// and short enough to be flaky.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestGetSet(t *testing.T) {
	c := New(Options[string, int]{})
	defer c.Close()

	if _, ok := c.Get("a"); ok {
		t.Fatal("empty cache returned a value")
	}

	// Writes are synchronous: unlike an asynchronously buffered cache, a value is
	// readable the moment Set returns, with no Wait() in sight.
	if err := c.Set("a", 1); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if v, ok := c.Get("a"); !ok || v != 1 {
		t.Fatalf("Get = %v, %v; want 1, true", v, ok)
	}

	if !c.Delete("a") {
		t.Fatal("Delete reported the key as absent")
	}
	if _, ok := c.Get("a"); ok {
		t.Fatal("deleted key still readable")
	}
}

func TestLookupDistinguishesNegativeFromMiss(t *testing.T) {
	c := New(Options[string, int]{TTL: time.Minute, NegativeTTL: time.Minute})
	defer c.Close()

	if _, st := c.Lookup("gone"); st != StatusMiss {
		t.Fatalf("status = %v; want miss", st)
	}

	if err := c.SetNegative("gone"); err != nil {
		t.Fatalf("SetNegative: %v", err)
	}

	v, st := c.Lookup("gone")
	if st != StatusNegative {
		t.Fatalf("status = %v; want negative", st)
	}
	if v != 0 {
		t.Fatalf("negative entry carried value %v; want the zero value", v)
	}
	// Get cannot tell them apart, and says so by reporting false.
	if _, ok := c.Get("gone"); ok {
		t.Fatal("Get reported a negative entry as a hit")
	}
}

func TestSetNegativeRequiresTTL(t *testing.T) {
	c := New(Options[string, int]{TTL: time.Minute})
	defer c.Close()

	if err := c.SetNegative("gone"); err != ErrNegativeDisabled {
		t.Fatalf("err = %v; want ErrNegativeDisabled", err)
	}
	if c.Len() != 0 {
		t.Fatal("rejected negative entry was stored anyway")
	}
}

// TestSetNegativeTTLDoesNotNeedTheOption: the lifetime is right there in the
// call, so there is nothing left for NegativeTTL to enable.
func TestSetNegativeTTLDoesNotNeedTheOption(t *testing.T) {
	c := New(Options[string, int]{TTL: time.Minute})
	defer c.Close()

	if err := c.SetNegativeTTL("gone", time.Minute); err != nil {
		t.Fatalf("SetNegativeTTL: %v", err)
	}
	if _, st := c.Lookup("gone"); st != StatusNegative {
		t.Fatalf("status = %v; want negative", st)
	}

	// An explicit zero still means "for no time at all", which is not an entry.
	if err := c.SetNegativeTTL("other", 0); err != ErrNegativeDisabled {
		t.Fatalf("err = %v; want ErrNegativeDisabled", err)
	}
}

func TestExpiryIsLazyAndSwept(t *testing.T) {
	c := New(Options[string, int]{DisableCleanup: true})
	defer c.Close()

	if err := c.SetTTL("a", 1, time.Millisecond); err != nil {
		t.Fatalf("SetTTL: %v", err)
	}
	time.Sleep(2 * time.Millisecond)
	if _, ok := c.Get("a"); ok {
		t.Fatal("expired entry returned by Get")
	}

	// An entry nobody looks up still has to leave, or it holds budget forever.
	if err := c.SetTTL("b", 2, time.Hour); err != nil {
		t.Fatalf("SetTTL: %v", err)
	}
	c.core.sweep(time.Now().Add(2 * time.Hour).UnixNano())
	if c.Len() != 0 {
		t.Fatalf("Len = %d after sweep; want 0", c.Len())
	}
	if got := c.Stats().Expirations; got != 2 {
		t.Fatalf("Expirations = %d; want 2", got)
	}
}

func TestZeroTTLNeverExpires(t *testing.T) {
	c := New(Options[string, int]{})
	defer c.Close()

	if err := c.Set("a", 1); err != nil {
		t.Fatalf("Set: %v", err)
	}
	c.core.sweep(time.Now().Add(100 * time.Hour).UnixNano())
	if _, ok := c.Get("a"); !ok {
		t.Fatal("entry without a TTL was swept")
	}
}

func TestJitterStaysInBounds(t *testing.T) {
	const ttl = time.Second
	c := New(Options[string, int]{TTL: ttl, Jitter: 25, DisableCleanup: true})
	defer c.Close()

	spread := make(map[int64]struct{})
	for range 500 {
		start := time.Now()
		got := time.Duration(c.core.expiryAt(ttl) - start.UnixNano())
		if got < ttl*3/4 || got > ttl*5/4 {
			t.Fatalf("expiry %v outside +-25%% of %v", got, ttl)
		}
		spread[int64(got)] = struct{}{}
	}
	if len(spread) < 100 {
		t.Fatalf("only %d distinct expiry times in 500 draws; jitter is not spreading", len(spread))
	}
}

func TestFullJitterNeverProducesImmortalEntries(t *testing.T) {
	// Jitter of 100 can subtract the whole TTL; landing on zero would mean
	// "never expires", which is the opposite of what was asked for.
	c := New(Options[string, int]{TTL: time.Millisecond, Jitter: 100, DisableCleanup: true})
	defer c.Close()

	for range 1000 {
		if c.core.expiryAt(time.Millisecond) == 0 {
			t.Fatal("jitter produced an entry that never expires")
		}
	}
}

func TestByteBudgetEvictsLeastRecentlyUsed(t *testing.T) {
	var evicted []string
	c := New(Options[string, int]{
		MaxBytes: 3,
		Cost:     unitCost[int](),
		OnEvict: func(k string, _ int, r EvictReason) {
			if r == ReasonEvicted {
				evicted = append(evicted, k)
			}
		},
	})
	defer c.Close()

	for _, k := range []string{"a", "b", "c"} {
		if err := c.Set(k, 1); err != nil {
			t.Fatalf("Set %s: %v", k, err)
		}
	}
	if _, ok := c.Get("a"); !ok {
		t.Fatal("a should still be cached")
	}

	// a was just used, so b is now the least recently used.
	if err := c.Set("d", 1); err != nil {
		t.Fatalf("Set d: %v", err)
	}
	if len(evicted) != 1 || evicted[0] != "b" {
		t.Fatalf("evicted = %v; want [b]", evicted)
	}
	if _, ok := c.Get("b"); ok {
		t.Fatal("b was reported evicted but is still readable")
	}
	if c.Bytes() != 3 {
		t.Fatalf("Bytes = %d; want 3", c.Bytes())
	}
}

func TestMaxEntries(t *testing.T) {
	c := New(Options[int, int]{MaxEntries: 2})
	defer c.Close()

	for i := range 5 {
		if err := c.Set(i, i); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
	}
	if c.Len() != 2 {
		t.Fatalf("Len = %d; want 2", c.Len())
	}
}

func TestOversizedValueIsRejectedNotDropped(t *testing.T) {
	c := New(Options[string, int]{
		MaxBytes: 10,
		Cost:     func(int) int64 { return 100 },
	})
	defer c.Close()

	// The failure mode this replaces: a cache that accepts the write, returns
	// success, and drops the entry afterwards without telling anyone.
	if err := c.Set("big", 1); err != ErrTooLarge {
		t.Fatalf("err = %v; want ErrTooLarge", err)
	}
	if _, ok := c.Get("big"); ok {
		t.Fatal("rejected value is readable")
	}
	if got := c.Stats().Rejections; got != 1 {
		t.Fatalf("Rejections = %d; want 1", got)
	}
}

func TestClearOnFullKeepsTheEntryThatOverflowed(t *testing.T) {
	c := New(Options[string, int]{
		Policy:   ClearOnFull,
		MaxBytes: 3,
		Cost:     unitCost[int](),
	})
	defer c.Close()

	for _, k := range []string{"a", "b", "c", "d"} {
		if err := c.Set(k, 1); err != nil {
			t.Fatalf("Set %s: %v", k, err)
		}
	}

	if c.Len() != 1 {
		t.Fatalf("Len = %d; want 1", c.Len())
	}
	// Dropping the write that triggered the wipe would only force an immediate refetch.
	if _, ok := c.Get("d"); !ok {
		t.Fatal("the entry that overflowed the cache was thrown away with the rest")
	}
	if c.Bytes() != 1 {
		t.Fatalf("Bytes = %d; want 1", c.Bytes())
	}
}

func TestOnEvictReasons(t *testing.T) {
	seen := map[EvictReason][]string{}
	c := New(Options[string, int]{
		MaxEntries:     1,
		DisableCleanup: true,
		OnEvict: func(k string, _ int, r EvictReason) {
			seen[r] = append(seen[r], k)
		},
	})
	defer c.Close()

	if err := c.Set("a", 1); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := c.Set("a", 2); err != nil { // replaced
		t.Fatalf("Set: %v", err)
	}
	if err := c.Set("b", 3); err != nil { // evicts a
		t.Fatalf("Set: %v", err)
	}
	if err := c.SetTTL("b", 4, time.Hour); err != nil { // replaces b
		t.Fatalf("SetTTL: %v", err)
	}
	c.core.sweep(time.Now().Add(2 * time.Hour).UnixNano()) // expires b

	want := map[EvictReason][]string{
		ReasonReplaced: {"a", "b"},
		ReasonEvicted:  {"a"},
		ReasonExpired:  {"b"},
	}
	if fmt.Sprint(seen) != fmt.Sprint(want) {
		t.Fatalf("OnEvict calls = %v; want %v", seen, want)
	}

	// Explicit removal is the caller's own doing and is not reported.
	before := len(seen[ReasonEvicted]) + len(seen[ReasonExpired]) + len(seen[ReasonReplaced])
	if err := c.Set("z", 1); err != nil {
		t.Fatalf("Set: %v", err)
	}
	c.Delete("z")
	c.Clear()
	after := len(seen[ReasonEvicted]) + len(seen[ReasonExpired]) + len(seen[ReasonReplaced])
	if after != before {
		t.Fatal("Delete or Clear called OnEvict")
	}
}

func TestStats(t *testing.T) {
	c := New(Options[string, int]{TTL: time.Minute, NegativeTTL: time.Minute})
	defer c.Close()

	if err := c.Set("a", 1); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := c.SetNegative("b"); err != nil {
		t.Fatalf("SetNegative: %v", err)
	}

	c.Get("a")    // hit
	c.Get("a")    // hit
	c.Lookup("b") // negative
	c.Get("nope") // miss

	st := c.Stats()
	if st.Hits != 2 || st.Negatives != 1 || st.Misses != 1 {
		t.Fatalf("hits/negatives/misses = %d/%d/%d; want 2/1/1", st.Hits, st.Negatives, st.Misses)
	}
	if st.Entries != 2 {
		t.Fatalf("Entries = %d; want 2", st.Entries)
	}
	// A cached negative saved the same upstream call a positive one would have.
	if got := st.HitRate(); got != 0.75 {
		t.Fatalf("HitRate = %v; want 0.75", got)
	}
}

func TestShardingSplitsTheBudget(t *testing.T) {
	c := New(Options[int, int]{
		Shards:   4,
		MaxBytes: 8,
		Cost:     unitCost[int](),
	})
	defer c.Close()

	if len(c.core.shards) != 4 {
		t.Fatalf("shards = %d; want 4", len(c.core.shards))
	}
	for i, s := range c.core.shards {
		if s.maxBytes != 2 {
			t.Fatalf("shard %d budget = %d; want 2", i, s.maxBytes)
		}
	}

	// Every key must land somewhere and be readable from there.
	for i := range 200 {
		if err := c.Set(i, i); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
		if v, ok := c.Get(i); !ok || v != i {
			t.Fatalf("Get(%d) = %v, %v", i, v, ok)
		}
	}
	// The budget is per shard, so the total is a ceiling, not a promise of use.
	if c.Bytes() > 8 {
		t.Fatalf("Bytes = %d; want at most 8", c.Bytes())
	}
}

func TestShardCount(t *testing.T) {
	for in, want := range map[int]int{-1: 1, 0: 1, 1: 1, 2: 2, 3: 4, 5: 8, 16: 16, 17: 32} {
		if got := shardCount(in); got != want {
			t.Fatalf("shardCount(%d) = %d; want %d", in, got, want)
		}
	}
}

func TestDivideBudget(t *testing.T) {
	// Rounding up keeps the shards together no stricter than the total asked for.
	for _, tc := range []struct{ total, n, want int64 }{
		{0, 4, 0}, {10, 1, 10}, {10, 4, 3}, {8, 4, 2}, {1, 8, 1},
	} {
		if got := divideBudget(tc.total, tc.n); got != tc.want {
			t.Fatalf("divideBudget(%d, %d) = %d; want %d", tc.total, tc.n, got, tc.want)
		}
	}
}

func TestBackgroundSweeperRuns(t *testing.T) {
	c := New(Options[string, int]{TTL: 10 * time.Millisecond, CleanupInterval: time.Millisecond})
	defer c.Close()

	if err := c.Set("a", 1); err != nil {
		t.Fatalf("Set: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for c.Len() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("sweeper did not drop the expired entry")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestCloseIsIdempotentAndLeavesTheCacheUsable(t *testing.T) {
	c := New(Options[string, int]{TTL: time.Minute})
	c.Close()
	c.Close()

	if err := c.Set("a", 1); err != nil {
		t.Fatalf("Set after Close: %v", err)
	}
	if _, ok := c.Get("a"); !ok {
		t.Fatal("cache unusable after Close")
	}
}

func TestPanicsOnUnworkableOptions(t *testing.T) {
	for name, build := range map[string]func(){
		"budget without cost": func() { New(Options[string, int]{MaxBytes: 1}) },
		"jitter above 100":    func() { New(Options[string, int]{Jitter: 101}) },
		"negative jitter":     func() { New(Options[string, int]{Jitter: -1}) },
		"negative budget":     func() { New(Options[string, int]{MaxBytes: -1}) },
		"negative entries":    func() { New(Options[string, int]{MaxEntries: -1}) },
		"negative ttl":        func() { New(Options[string, int]{TTL: -time.Second}) },
		"negative granularity": func() {
			New(Options[string, int]{ClockGranularity: -time.Second})
		},
	} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("New accepted options that cannot describe a working cache")
				}
			}()
			build()
		})
	}
}

func TestConcurrentUse(t *testing.T) {
	for _, policy := range []Policy{LRU, ClearOnFull} {
		t.Run(policy.String(), func(t *testing.T) {
			c := New(Options[int, int]{
				TTL:             50 * time.Millisecond,
				NegativeTTL:     10 * time.Millisecond,
				Jitter:          20,
				MaxBytes:        512,
				Cost:            unitCost[int](),
				Shards:          8,
				Policy:          policy,
				CleanupInterval: time.Millisecond,
				OnEvict:         func(int, int, EvictReason) {},
			})
			defer c.Close()

			var wg sync.WaitGroup
			for g := range 8 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := range 2000 {
						k := (g*2000 + i) % 900
						// Errors are part of what is being exercised here: a
						// tight budget makes ErrTooLarge an ordinary outcome.
						switch i % 4 {
						case 0:
							_ = c.Set(k, i)
						case 1:
							c.Lookup(k)
						case 2:
							_ = c.SetNegative(k)
						default:
							c.Delete(k)
						}
					}
				}()
			}
			wg.Wait()

			if c.Bytes() < 0 || c.Len() < 0 {
				t.Fatalf("accounting went negative: %d bytes, %d entries", c.Bytes(), c.Len())
			}
		})
	}
}

func TestClearOnFullCountsAnExpirationOnce(t *testing.T) {
	c := New(Options[string, int]{Policy: ClearOnFull, DisableCleanup: true})
	defer c.Close()

	if err := c.SetTTL("a", 1, time.Millisecond); err != nil {
		t.Fatalf("SetTTL: %v", err)
	}
	time.Sleep(2 * time.Millisecond)

	for range 5 {
		if _, ok := c.Get("a"); ok {
			t.Fatal("expired entry returned by Get")
		}
	}

	// The read path takes only a read lock, but an expired entry still has to
	// leave: otherwise it holds budget and is counted again on every lookup.
	if c.Len() != 0 {
		t.Fatalf("Len = %d; want 0", c.Len())
	}
	st := c.Stats()
	if st.Expirations != 1 {
		t.Fatalf("Expirations = %d; want 1", st.Expirations)
	}
	if st.Misses != 5 {
		t.Fatalf("Misses = %d; want 5", st.Misses)
	}
}

func TestCoarseClockExpiresEntries(t *testing.T) {
	c := New(Options[string, int]{
		ClockGranularity: time.Millisecond,
		DisableCleanup:   true,
	})
	defer c.Close()

	if err := c.SetTTL("a", 1, 10*time.Millisecond); err != nil {
		t.Fatalf("SetTTL: %v", err)
	}
	// The clock is seeded at construction: a cache whose coarse time started at
	// the epoch would report everything as expired on the first lookup.
	if _, ok := c.Get("a"); !ok {
		t.Fatal("entry expired immediately under a coarse clock")
	}

	waitFor(t, "the coarse clock to pass the TTL", func() bool {
		_, ok := c.Get("a")

		return !ok
	})
}

// TestCoarseClockStopsWithTheCache: a stopped clock that stayed authoritative
// would freeze time, and nothing would ever expire again.
func TestCoarseClockStopsWithTheCache(t *testing.T) {
	c := New(Options[string, int]{
		ClockGranularity: time.Hour,
		DisableCleanup:   true,
	})

	if err := c.SetTTL("a", 1, time.Millisecond); err != nil {
		t.Fatalf("SetTTL: %v", err)
	}
	time.Sleep(5 * time.Millisecond)
	// An hour of granularity means the clock has not moved, so the entry is
	// still inside its TTL as far as this cache is concerned.
	if _, ok := c.Get("a"); !ok {
		t.Fatal("lookups are reading the wall clock, not the coarse one")
	}

	c.Close()
	waitFor(t, "lookups to fall back to the wall clock", func() bool {
		_, ok := c.Get("a")

		return !ok
	})
}
