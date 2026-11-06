package main

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// TestRoleLinesAreWorkerLines: a part's role lines (a stream taken or
// dropped, a role changed, what is left to main or Synapse) go through its
// Worker logger, component=worker, so a part's own level does not hide them.
// On aguiarvieira.pt, device_lists: error hid "writing the device_lists
// stream", which made a running writer look like one that had not started.
// main.go's own log is the worker's, so only internal is checked.
func TestRoleLinesAreWorkerLines(t *testing.T) {
	role := regexp.MustCompile(`Msg\("[^"]*(names this process|no longer writing|restart goworker to take|role changed|` +
		`unavailable|proxied to main|go(es)? to (main|Synapse|/whoami)|left to Synapse|write mode|EDU routing|` +
		`client auth: native|caching immutable|Origins in backoff|sliding sync enabled|to_device enabled|` +
		`starting the federation sender|not named in federation_sender_instances|replication stream lost"\))`)
	call := regexp.MustCompile(`\.(Trace|Debug|Info|Warn|Error)\(\)`)
	worker := regexp.MustCompile(`(Worker|worker|wlog)\.(Trace|Debug|Info|Warn|Error)\(\)`)
	found := 0
	err := filepath.WalkDir("../../internal", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return err
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		lines := strings.Split(string(b), "\n")
		for i, l := range lines {
			if !role.MatchString(l) {
				continue
			}
			// The statement starts at the nearest level call above.
			j := i
			for j >= 0 && i-j < 12 && !call.MatchString(lines[j]) {
				j--
			}
			if j < 0 || i-j >= 12 {
				// Not a log line (an error text, say).
				continue
			}
			found++
			if !worker.MatchString(lines[j]) {
				t.Errorf("%s:%d: a role line logged at its part's level; log it through the part's Worker logger", path, j+1)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if found < 20 {
		t.Fatalf("found %d role lines; the test is not looking where they are", found)
	}
}
