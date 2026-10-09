package sanecache

import (
	"reflect"
	"testing"
)

// TestStatsSub covers every counter by reflection, so that a counter added
// later and forgotten in Sub fails here.
func TestStatsSub(t *testing.T) {
	testSub(t, func(cur, prev Stats) Stats { return cur.Sub(prev) })
	testSub(t, func(cur, prev ViewStats) ViewStats { return cur.Sub(prev) })
}

func testSub[S any](t *testing.T, sub func(cur, prev S) S) {
	t.Helper()

	var cur, prev, reset S
	cv, pv, rv := reflect.ValueOf(&cur).Elem(), reflect.ValueOf(&prev).Elem(), reflect.ValueOf(&reset).Elem()
	for i := range cv.NumField() {
		v := int64(i + 1)
		cv.Field(i).SetInt(10 * v)
		pv.Field(i).SetInt(3 * v)
		rv.Field(i).SetInt(100 * v)
	}

	got, wasReset := reflect.ValueOf(sub(cur, prev)), reflect.ValueOf(sub(cur, reset))
	for i := range cv.NumField() {
		name, v := cv.Type().Field(i).Name, int64(i+1)
		wantDelta, wantReset := 7*v, 10*v
		if name == "Entries" || name == "Bytes" {
			wantDelta = 10 * v
		}
		if g := got.Field(i).Int(); g != wantDelta {
			t.Errorf("%T.Sub: %s = %d, want %d", cur, name, g, wantDelta)
		}
		if g := wasReset.Field(i).Int(); g != wantReset {
			t.Errorf("%T.Sub after a reset: %s = %d, want %d", cur, name, g, wantReset)
		}
	}
}
