package slidingstore

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// Synapse has tables of the same names in public, so an unqualified name
// would read and write Synapse's own sliding sync state for real clients.
// Nothing pins search_path: behind pgcat a startup parameter does not reach
// the server connection.
func TestEveryTableIsQualified(t *testing.T) {
	unqualified := regexp.MustCompile(`(?:^|[^\w.])(sliding_sync_connection(?:s|_[a-z_]+))\b`)
	// FOR ... OF names a FROM item, and PostgreSQL requires it unqualified.
	lockingClause := regexp.MustCompile(`FOR (?:NO KEY |KEY )?(?:UPDATE|SHARE) OF sliding_sync_[a-z_]+`)
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	found := 0
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		src := lockingClause.ReplaceAllString(string(b), "")
		found += strings.Count(src, "gosync.sliding_sync_")
		for _, m := range unqualified.FindAllStringSubmatch(src, -1) {
			t.Errorf("%s names %s without its schema", f, m[1])
		}
	}
	if found == 0 {
		t.Fatal("found no qualified table names; the test is not looking where the queries are")
	}
}
