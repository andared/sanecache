package sanecache_test

import (
	"errors"
	"fmt"
	"time"

	"github.com/andared/sanecache"
)

type article struct {
	ID   string
	Body string
}

func Example() {
	c := sanecache.New(sanecache.Options[string, *article]{
		TTL:      10 * time.Minute,
		Jitter:   10,
		MaxBytes: 1 << 20,
		// Resident size, not serialized size: a decoded struct costs several
		// times its payload. Measure the ratio rather than guessing it.
		Cost: func(a *article) int64 { return int64(len(a.Body)) * 3 },
	})
	defer c.Close()

	if err := c.Set("a1", &article{ID: "a1", Body: "hello"}); err != nil {
		fmt.Println("set:", err)
		return
	}

	if a, ok := c.Get("a1"); ok {
		fmt.Println("cached:", a.Body)
	}

	// Output:
	// cached: hello
}

// Telling "nobody asked yet" apart from "the upstream says it does not exist".
func ExampleCache_Lookup() {
	c := sanecache.New(sanecache.Options[string, string]{
		TTL:         time.Minute,
		NegativeTTL: 30 * time.Second,
	})
	defer c.Close()

	_, status := c.Lookup("gone")
	fmt.Println("before asking the upstream:", status)

	// The upstream answered "no such key". Remember that, or every render of a
	// template naming this id will ask again.
	if err := c.SetNegative("gone"); err != nil {
		fmt.Println("set negative:", err)
		return
	}

	_, status = c.Lookup("gone")
	fmt.Println("after:", status)

	// Get collapses the two, because both mean "you have no value".
	_, ok := c.Get("gone")
	fmt.Println("Get reports a hit:", ok)

	// Output:
	// before asking the upstream: miss
	// after: negative
	// Get reports a hit: false
}

// A value that cannot fit is refused rather than accepted and dropped later.
func ExampleCache_Set() {
	c := sanecache.New(sanecache.Options[string, []byte]{
		MaxBytes: 1024,
		Cost:     func(b []byte) int64 { return int64(len(b)) },
	})
	defer c.Close()

	err := c.Set("huge", make([]byte, 4096))
	fmt.Println("rejected:", errors.Is(err, sanecache.ErrTooLarge))

	// The rejection is counted, so keys that can never be cached show up as
	// their own signal instead of an unexplained miss rate.
	_, ok := c.Get("huge")
	fmt.Println("cached:", ok, "rejections:", c.Stats().Rejections)

	// Output:
	// rejected: true
	// cached: false rejections: 1
}

func ExampleCache_Stats() {
	c := sanecache.New(sanecache.Options[int, string]{
		TTL:         time.Minute,
		NegativeTTL: time.Minute,
	})
	defer c.Close()

	_ = c.Set(1, "one")
	_ = c.SetNegative(2)

	c.Get(1)    // hit
	c.Lookup(2) // negative
	c.Get(3)    // miss

	s := c.Stats()
	// A cached negative counts towards the hit rate: it saved the same upstream
	// call a positive answer would have.
	fmt.Printf("hits=%d negatives=%d misses=%d rate=%.2f\n",
		s.Hits, s.Negatives, s.Misses, s.HitRate())

	// Output:
	// hits=1 negatives=1 misses=1 rate=0.67
}
