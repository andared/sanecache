package sanecache

import (
	"testing"
	"time"
)

type article struct {
	id   string
	body string
}

type season struct {
	num int
}

func newViewCache(o Options[string, any]) *Cache[string, any] { return New(o) }

// TestViewsShareOneBudget is the point of the whole thing: one pool of memory,
// several value types, and no guess up front about how to divide it.
func TestViewsShareOneBudget(t *testing.T) {
	c := newViewCache(Options[string, any]{
		MaxBytes: 3,
		Cost:     func(any) int64 { return 1 },
	})
	defer c.Close()

	articles := NewView(c, ViewOptions[*article]{Name: "article"})
	seasons := NewView(c, ViewOptions[*season]{Name: "season"})

	// The same key in two views is two entries, not a collision.
	if err := articles.Set("1", &article{id: "1", body: "hello"}); err != nil {
		t.Fatalf("articles.Set: %v", err)
	}
	if err := seasons.Set("1", &season{num: 7}); err != nil {
		t.Fatalf("seasons.Set: %v", err)
	}

	a, ok := articles.Get("1")
	if !ok || a.body != "hello" {
		t.Fatalf("articles.Get = %v, %v", a, ok)
	}
	s, ok := seasons.Get("1")
	if !ok || s.num != 7 {
		t.Fatalf("seasons.Get = %v, %v", s, ok)
	}
	if c.Len() != 2 {
		t.Fatalf("Len = %d; want 2", c.Len())
	}

	// Eviction is over the whole budget, so a view that suddenly needs more
	// memory takes it from whoever used theirs least recently.
	for i := range 3 {
		if err := seasons.SetTTL(string(rune('a'+i)), &season{num: i}, 0); err != nil {
			t.Fatalf("seasons.Set: %v", err)
		}
	}
	if _, ok := articles.Get("1"); ok {
		t.Fatal("the article survived a budget it shares with a busier view")
	}
	if c.Bytes() > 3 {
		t.Fatalf("Bytes = %d; want at most 3", c.Bytes())
	}
}

func TestViewNamespacesKeysInTheUnderlyingCache(t *testing.T) {
	c := newViewCache(Options[string, any]{})
	defer c.Close()

	articles := NewView(c, ViewOptions[*article]{Name: "article"})
	if err := articles.Set("1", &article{id: "1"}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// Documented, because an OnEvict handler on the cache sees these keys.
	if _, ok := c.Get("article:1"); !ok {
		t.Fatal("view key is not name + \\\":\\\" + key")
	}
	if articles.Name() != "article" {
		t.Fatalf("Name = %q; want article", articles.Name())
	}
}

// TestViewTypeMiss covers the one way a namespaced view can still be handed the
// wrong type: something wrote to the underlying cache directly.
func TestViewTypeMiss(t *testing.T) {
	c := newViewCache(Options[string, any]{})
	defer c.Close()

	articles := NewView(c, ViewOptions[*article]{Name: "article"})
	if err := c.Set("article:1", 42); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// Unusable here, so it is a miss: the caller has to go to the upstream
	// either way. But it is not the same thing as an empty cache, and the
	// counter is what says so.
	if _, st := articles.Lookup("1"); st != StatusMiss {
		t.Fatalf("status = %v; want miss", st)
	}
	if got := articles.Stats().TypeMisses; got != 1 {
		t.Fatalf("view TypeMisses = %d; want 1", got)
	}
	if got := c.Stats().TypeMisses; got != 1 {
		t.Fatalf("cache TypeMisses = %d; want 1", got)
	}
	// The cache did hold the key, and its own counters say so.
	if got := c.Stats().Hits; got != 1 {
		t.Fatalf("cache Hits = %d; want 1", got)
	}
}

func TestViewNegative(t *testing.T) {
	c := newViewCache(Options[string, any]{NegativeTTL: time.Minute})
	defer c.Close()

	articles := NewView(c, ViewOptions[*article]{Name: "article"})
	seasons := NewView(c, ViewOptions[*season]{Name: "season"})

	if err := articles.SetNegative("1"); err != nil {
		t.Fatalf("SetNegative: %v", err)
	}

	if _, st := articles.Lookup("1"); st != StatusNegative {
		t.Fatalf("status = %v; want negative", st)
	}
	// "This article does not exist" says nothing about the season with the same
	// id, which is exactly what the namespace is for.
	if _, st := seasons.Lookup("1"); st != StatusMiss {
		t.Fatalf("status = %v; want miss", st)
	}

	if got := articles.Stats().Negatives; got != 1 {
		t.Fatalf("Negatives = %d; want 1", got)
	}
}

func TestViewNegativeNeedsATTL(t *testing.T) {
	c := newViewCache(Options[string, any]{})
	defer c.Close()

	articles := NewView(c, ViewOptions[*article]{Name: "article"})
	if err := articles.SetNegative("1"); err != ErrNegativeDisabled {
		t.Fatalf("err = %v; want ErrNegativeDisabled", err)
	}

	// A view can enable it on its own, without the whole cache doing so.
	seasons := NewView(c, ViewOptions[*season]{Name: "season", NegativeTTL: time.Minute})
	if err := seasons.SetNegative("1"); err != nil {
		t.Fatalf("SetNegative: %v", err)
	}

	// And an explicit lifetime needs neither.
	if err := articles.SetNegativeTTL("2", time.Minute); err != nil {
		t.Fatalf("SetNegativeTTL: %v", err)
	}
	if _, st := articles.Lookup("2"); st != StatusNegative {
		t.Fatalf("status = %v; want negative", st)
	}
}

// TestViewCost: a per-view Cost is what spares the caller the type switch that a
// shared Cost func(any) int64 turns into once a budget holds several types.
func TestViewCost(t *testing.T) {
	c := newViewCache(Options[string, any]{
		MaxBytes: 1000,
		Cost:     func(any) int64 { return 1 },
	})
	defer c.Close()

	articles := NewView(c, ViewOptions[*article]{
		Name: "article",
		Cost: func(a *article) int64 { return int64(len(a.body)) * 3 },
	})
	seasons := NewView(c, ViewOptions[*season]{Name: "season"})

	if err := articles.Set("1", &article{body: "0123456789"}); err != nil {
		t.Fatalf("articles.Set: %v", err)
	}
	if err := seasons.Set("1", &season{num: 1}); err != nil {
		t.Fatalf("seasons.Set: %v", err)
	}

	// 30 from the view's own Cost, 1 from the cache's for the view without one.
	if got := c.Bytes(); got != 31 {
		t.Fatalf("Bytes = %d; want 31", got)
	}
}

func TestViewTTL(t *testing.T) {
	c := newViewCache(Options[string, any]{TTL: time.Hour, DisableCleanup: true})
	defer c.Close()

	long := NewView(c, ViewOptions[*article]{Name: "article"})
	short := NewView(c, ViewOptions[*season]{Name: "season", TTL: time.Millisecond})

	if err := long.Set("1", &article{id: "1"}); err != nil {
		t.Fatalf("long.Set: %v", err)
	}
	if err := short.Set("1", &season{num: 1}); err != nil {
		t.Fatalf("short.Set: %v", err)
	}
	time.Sleep(5 * time.Millisecond)

	if _, ok := short.Get("1"); ok {
		t.Fatal("the view's own TTL was ignored")
	}
	if _, ok := long.Get("1"); !ok {
		t.Fatal("a view without a TTL of its own did not inherit the cache's")
	}
}

func TestViewDelete(t *testing.T) {
	c := newViewCache(Options[string, any]{})
	defer c.Close()

	articles := NewView(c, ViewOptions[*article]{Name: "article"})
	seasons := NewView(c, ViewOptions[*season]{Name: "season"})
	if err := articles.Set("1", &article{id: "1"}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := seasons.Set("1", &season{num: 1}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if !articles.Delete("1") {
		t.Fatal("Delete reported the key as absent")
	}
	if articles.Delete("1") {
		t.Fatal("Delete reported a key it had just removed as present")
	}
	if _, ok := seasons.Get("1"); !ok {
		t.Fatal("deleting from one view removed another view's entry")
	}
}

func TestViewStats(t *testing.T) {
	c := newViewCache(Options[string, any]{NegativeTTL: time.Minute})
	defer c.Close()

	articles := NewView(c, ViewOptions[*article]{Name: "article"})
	seasons := NewView(c, ViewOptions[*season]{Name: "season"})

	if err := articles.Set("1", &article{id: "1"}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := articles.SetNegative("2"); err != nil {
		t.Fatalf("SetNegative: %v", err)
	}
	articles.Get("1")    // hit
	articles.Lookup("2") // negative
	articles.Get("3")    // miss
	seasons.Get("1")     // miss, and none of the articles' business

	st := articles.Stats()
	if st.Hits != 1 || st.Negatives != 1 || st.Misses != 1 || st.TypeMisses != 0 {
		t.Fatalf("view stats = %+v; want 1 hit, 1 negative, 1 miss", st)
	}
	if got := st.HitRate(); got < 0.66 || got > 0.67 {
		t.Fatalf("HitRate = %v; want ~0.67", got)
	}
	if got := seasons.Stats().Misses; got != 1 {
		t.Fatalf("seasons Misses = %d; want 1", got)
	}
	// The cache counts every view's lookups together.
	if got := c.Stats().Misses; got != 2 {
		t.Fatalf("cache Misses = %d; want 2", got)
	}
}

func TestViewRespectsDisableStats(t *testing.T) {
	c := newViewCache(Options[string, any]{DisableStats: true})
	defer c.Close()

	articles := NewView(c, ViewOptions[*article]{Name: "article"})
	articles.Get("1")

	if got := articles.Stats(); got != (ViewStats{}) {
		t.Fatalf("view stats = %+v; want zero", got)
	}
}

func TestNewViewPanicsOnUnusableNames(t *testing.T) {
	c := newViewCache(Options[string, any]{})
	defer c.Close()

	for name, build := range map[string]func(){
		"nil cache":     func() { NewView(nil, ViewOptions[*article]{Name: "article"}) },
		"empty name":    func() { NewView(c, ViewOptions[*article]{}) },
		"name with sep": func() { NewView(c, ViewOptions[*article]{Name: "a:b"}) },
		"negative ttl": func() {
			NewView(c, ViewOptions[*article]{Name: "a", TTL: -time.Second})
		},
		"negative negative ttl": func() {
			NewView(c, ViewOptions[*article]{Name: "a", NegativeTTL: -time.Second})
		},
	} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("NewView accepted options that cannot keep views apart")
				}
			}()
			build()
		})
	}
}

func TestViewEmptyKey(t *testing.T) {
	c := newViewCache(Options[string, any]{})
	defer c.Close()

	// Nothing special about it: "article:" is still only this view's.
	articles := NewView(c, ViewOptions[*article]{Name: "article"})
	if err := articles.Set("", &article{id: "root"}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if a, ok := articles.Get(""); !ok || a.id != "root" {
		t.Fatalf("Get = %v, %v", a, ok)
	}
	if _, ok := c.Get("article:"); !ok {
		t.Fatal("an empty key did not land under the view's prefix")
	}
}
