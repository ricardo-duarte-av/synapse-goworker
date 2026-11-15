package handlers

import "testing"

// get_messages' choice between backfilling before answering and after.
func TestBackfillInForeground(t *testing.T) {
	for _, c := range []struct {
		depths []int64
		limit  int
		want   bool
	}{
		{[]int64{10, 9, 8, 7}, 4, false},
		{[]int64{10, 9, 8}, 4, true},        // short page
		{[]int64{10, 7, 6, 5}, 4, true},     // a gap of three
		{[]int64{10, 8, 6, 4, 2}, 5, true},  // four gaps of two
		{[]int64{10, 8, 6, 4, 3}, 5, false}, // three gaps of two
		{[]int64{10, 10, 9, 9}, 4, false},   // several events at a depth
		{nil, 0, false},
		{nil, 1, true},
	} {
		if got := backfillInForeground(c.depths, c.limit); got != c.want {
			t.Errorf("%v limit %d: got %v", c.depths, c.limit, got)
		}
	}
}
