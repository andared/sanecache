package sanecache_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/andared/sanecache"
)

type article struct {
	ID   string
	Body string
}

type season struct {
	Num int
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

// A cold key is fetched once however many callers want it at the same moment.
func ExampleCache_GetOrLoad() {
	var upstreamCalls atomic.Int64

	c := sanecache.New(sanecache.Options[string, *article]{
		TTL:         time.Minute,
		NegativeTTL: 10 * time.Second,
		Loader: func(_ context.Context, id string) (*article, error) {
			upstreamCalls.Add(1)
			if id == "gone" {
				// The upstream's own "no such row" is translated once, here, so
				// that callers see the same error whether the answer came from
				// the upstream or from the negative entry it left behind.
				return nil, sanecache.ErrNotFound
			}

			return &article{ID: id, Body: "hello"}, nil
		},
	})
	defer c.Close()

	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := c.GetOrLoad(context.Background(), "a1"); err != nil {
				fmt.Println("load:", err)
			}
		}()
	}
	wg.Wait()

	_, err := c.GetOrLoad(context.Background(), "gone")
	fmt.Println("missing:", errors.Is(err, sanecache.ErrNotFound))

	// Ten callers, one upstream call — plus the one that came back empty.
	fmt.Println("upstream calls:", upstreamCalls.Load())

	// Output:
	// missing: true
	// upstream calls: 2
}

// Several value types under one byte budget, which is the only kind of budget
// that does not need dividing up in advance.
func ExampleNewView() {
	c := sanecache.New(sanecache.Options[string, any]{
		TTL:      10 * time.Minute,
		MaxBytes: 64 << 20,
		// The fallback for views that do not bring their own Cost. With several
		// types sharing a budget, this is where the type switch would go.
		Cost: func(any) int64 { return 256 },
	})
	defer c.Close()

	articles := sanecache.NewView(c, sanecache.ViewOptions[*article]{
		Name: "article",
		Cost: func(a *article) int64 { return int64(len(a.Body)) * 3 },
	})
	seasons := sanecache.NewView(c, sanecache.ViewOptions[*season]{
		Name: "season",
		TTL:  time.Hour, // seasons change less often than articles do
	})

	_ = articles.Set("1", &article{ID: "1", Body: "hello"})
	_ = seasons.Set("1", &season{Num: 7})

	a, _ := articles.Get("1")
	s, _ := seasons.Get("1")
	fmt.Println(a.Body, s.Num)

	// The same key in two views is two entries: the view name is part of it.
	fmt.Println("entries:", c.Len(), "bytes:", c.Bytes())

	// Output:
	// hello 7
	// entries: 2 bytes: 271
}
