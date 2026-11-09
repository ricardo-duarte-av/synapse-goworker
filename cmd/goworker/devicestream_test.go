package main

import (
	"testing"

	"github.com/ricardo-duarte-av/synapse-goworker/internal/devicelists"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/replserver"
	"github.com/ricardo-duarte-av/synapse-goworker/internal/platform/synapsecfg"
)

// Not the writer: the lowest position of the writers homeserver.yaml names,
// once each has been seen on the bus; before that, main answers.
func TestDeviceStreamFollower(t *testing.T) {
	hs := &synapsecfg.Config{Writers: synapsecfg.Writers{DeviceLists: []string{"master", "w2"}}}
	tr := replserver.NewTracker()
	f := deviceStream((*devicelists.App)(nil), tr, func() *synapsecfg.Config { return hs })
	if _, ok := f(); ok {
		t.Fatal("known before anything was seen")
	}
	tr.Observe("POSITION device_lists master 10 40")
	if _, ok := f(); ok {
		t.Fatal("known with w2 unseen")
	}
	tr.Observe(`RDATA device_lists w2 35 ["@a:x","D",null]`)
	if pos, ok := f(); !ok || pos != 35 {
		t.Fatalf("%d %v", pos, ok)
	}
	if _, ok := deviceStream(nil, nil, func() *synapsecfg.Config { return hs })(); ok {
		t.Fatal("known without a bus")
	}
}
