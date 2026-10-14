package sanecache

import (
	"fmt"
	"math/rand/v2"
	"testing"
	"time"
)

func newS3FIFO(maxEntries int) *Cache[string, int] {
	return New(Options[string, int]{MaxEntries: maxEntries, Policy: S3FIFO, DisableCleanup: true})
}

// TestS3FIFOKeepsWhatIsReadAgain is the point of the policy: a flood of keys
// read once does not push out the keys in use, which LRU would evict as the
// oldest.
func TestS3FIFOKeepsWhatIsReadAgain(t *testing.T) {
	c := newS3FIFO(10)
	defer c.Close()

	for i := range 10 {
		_ = c.Set(fmt.Sprint("hot", i), i)
	}
	for i := range 5 {
		c.Get(fmt.Sprint("hot", i))
	}
	for i := range 100 {
		_ = c.Set(fmt.Sprint("once", i), i)
	}

	for i := range 5 {
		if _, ok := c.Get(fmt.Sprint("hot", i)); !ok {
			t.Fatalf("hot%d, read again, was evicted by keys read once", i)
		}
	}
	if c.Len() != 10 {
		t.Fatalf("Len = %d; want 10", c.Len())
	}
}

// TestS3FIFOReadmitsGhostsToMain: a key let out of the small queue and soon
// written again has proved itself, and goes straight to the main queue, where
// keys read once cannot displace it.
func TestS3FIFOReadmitsGhostsToMain(t *testing.T) {
	c := newS3FIFO(10)
	defer c.Close()
	s := c.core.shards[0]

	_ = c.Set("back", 0)
	for i := range 10 {
		_ = c.Set(fmt.Sprint("once", i), i)
	}
	if _, ok := c.Get("back"); ok {
		t.Fatal("an unread key outlived ten newer ones")
	}

	_ = c.Set("back", 1)
	if s.items["back"].small {
		t.Fatal("a ghost came back into the small queue")
	}
	for i := range 100 {
		_ = c.Set(fmt.Sprint("more", i), i)
	}
	if v, ok := c.Get("back"); !ok || v != 1 {
		t.Fatal("keys read once evicted an entry of the main queue")
	}

	// A replacement takes over the entry's place and reads: in the small queue
	// too, rather than skipping it for main.
	_ = c.Set("new", 1)
	_ = c.Set("new", 2)
	if !s.items["new"].small {
		t.Fatal("replacing an entry of the small queue moved it to main")
	}

	c.Get("back")
	reads := s.items["back"].freq.Load()
	_ = c.Set("back", 2)
	if e := s.items["back"]; e.small || e.freq.Load() != reads {
		t.Fatalf("replacement: small %v, freq %d; want main, %d", e.small, e.freq.Load(), reads)
	}
}

func TestS3FIFOCapsReadCount(t *testing.T) {
	c := newS3FIFO(10)
	defer c.Close()

	_ = c.Set("k", 1)
	for range 10 {
		c.Get("k")
	}
	if f := c.core.shards[0].items["k"].freq.Load(); f != maxFreq {
		t.Fatalf("freq = %d; want %d", f, maxFreq)
	}
}

// TestS3FIFOAccounting runs random operations under both kinds of budget and
// checks the queues against the map after each.
func TestS3FIFOAccounting(t *testing.T) {
	for name, o := range map[string]Options[string, int]{
		"entries": {MaxEntries: 20},
		"bytes":   {MaxBytes: 200, Cost: func(v int) int64 { return int64(v%20 + 1) }},
	} {
		for seed := range uint64(4) {
			t.Run(fmt.Sprint(name, seed), func(t *testing.T) {
				o.Policy, o.DisableCleanup, o.NegativeTTL = S3FIFO, true, time.Hour
				c := New(o)
				defer c.Close()
				advance := manualClock(c)
				s := c.core.shards[0]
				rng := rand.New(rand.NewPCG(seed, 2))

				for i := range 5000 {
					k := fmt.Sprint(rng.IntN(60))
					switch rng.IntN(10) {
					case 0:
						c.Delete(k)
					case 1:
						_ = c.SetNegative(k)
					case 2:
						_ = c.SetTTL(k, i, time.Second)
					case 3:
						advance(time.Second)
						c.core.sweep(c.core.now())
					case 4, 5, 6:
						c.Get(k)
					default:
						_ = c.Set(k, i)
					}
					if i == 2500 {
						c.Clear()
					}
					checkS3FIFO(t, s)
				}
			})
		}
	}
}

func checkS3FIFO[K comparable, V any](t *testing.T, s *shard[K, V]) {
	t.Helper()

	var smallLen, mainLen int
	var smallBytes, bytes int64
	for e := s.smallHead; e != nil; e = e.next {
		if !e.small || s.items[e.key] != e {
			t.Fatalf("small queue holds an entry it should not: %v", e.key)
		}
		smallLen++
		smallBytes += e.cost
	}
	for e := s.head; e != nil; e = e.next {
		if e.small || s.items[e.key] != e {
			t.Fatalf("main queue holds an entry it should not: %v", e.key)
		}
		mainLen++
	}
	for _, e := range s.items {
		bytes += e.cost
	}

	switch {
	case smallLen+mainLen != len(s.items):
		t.Fatalf("queues hold %d+%d entries, the map %d", smallLen, mainLen, len(s.items))
	case smallLen != s.smallLen || smallBytes != s.smallBytes:
		t.Fatalf("small queue: %d entries, %d bytes; counted %d, %d", smallLen, smallBytes, s.smallLen, s.smallBytes)
	case bytes != s.bytes:
		t.Fatalf("bytes = %d; counted %d", bytes, s.bytes)
	case s.over():
		t.Fatalf("over budget: %d entries, %d bytes", len(s.items), s.bytes)
	case len(s.ghosts.set) > len(s.ghosts.queue):
		t.Fatalf("%d ghosts for a queue of %d", len(s.ghosts.set), len(s.ghosts.queue))
	}
}
