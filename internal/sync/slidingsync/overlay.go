package slidingsync

import "github.com/ricardo-duarte-av/synapse-goworker/internal/sync/store"

// roomOverlay is Synapse's MutableOverlayMapping over the snapshot rows
// (synapse/util/__init__.py), kept for its iteration order: a list whose one
// range covers it all is served in that order, unsorted.
//
// Entries set after the snapshot come first, in the order they were first
// set (a deleted entry set again goes to the end); then the snapshot's rows
// in the query's order, bar those deleted or overridden.
type roomOverlay struct {
	under      map[string]store.SlidingRoom
	underOrder []string
	mut        map[string]store.SlidingRoom
	mutOrder   []string
	deleted    map[string]bool
}

func newRoomOverlay(under map[string]store.SlidingRoom, order []string) *roomOverlay {
	return &roomOverlay{under: under, underOrder: order,
		mut: map[string]store.SlidingRoom{}, deleted: map[string]bool{}}
}

func (o *roomOverlay) get(roomID string) (store.SlidingRoom, bool) {
	if o.deleted[roomID] {
		return store.SlidingRoom{}, false
	}
	if r, ok := o.mut[roomID]; ok {
		return r, true
	}
	r, ok := o.under[roomID]
	return r, ok
}

func (o *roomOverlay) set(roomID string, r store.SlidingRoom) {
	delete(o.deleted, roomID)
	if _, ok := o.mut[roomID]; !ok {
		o.mutOrder = append(o.mutOrder, roomID)
	}
	o.mut[roomID] = r
}

// pop is MutableMapping.pop(key, None): a missing key is no error.
func (o *roomOverlay) pop(roomID string) {
	if _, ok := o.get(roomID); !ok {
		return
	}
	o.deleted[roomID] = true
	if _, ok := o.mut[roomID]; ok {
		delete(o.mut, roomID)
		for i, id := range o.mutOrder {
			if id == roomID {
				o.mutOrder = append(o.mutOrder[:i], o.mutOrder[i+1:]...)
				break
			}
		}
	}
}

// keys are the entries in iteration order.
func (o *roomOverlay) keys() []string {
	out := make([]string, 0, len(o.mutOrder)+len(o.underOrder))
	for _, id := range o.mutOrder {
		if !o.deleted[id] {
			out = append(out, id)
		}
	}
	for _, id := range o.underOrder {
		if _, overridden := o.mut[id]; !o.deleted[id] && !overridden {
			out = append(out, id)
		}
	}
	return out
}

// toMap is the entries as a map.
func (o *roomOverlay) toMap() map[string]store.SlidingRoom {
	out := make(map[string]store.SlidingRoom, len(o.under)+len(o.mut))
	for _, id := range o.keys() {
		r, _ := o.get(id)
		out[id] = r
	}
	return out
}
