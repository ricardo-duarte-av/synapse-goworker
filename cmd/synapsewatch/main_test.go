package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestNewestTag(t *testing.T) {
	for _, tc := range []struct {
		tags []string
		want string
	}{
		{[]string{"v1.164.0", "v1.164.1", "v1.164.0rc3"}, "v1.164.1"},
		{[]string{"v1.164.1", "v1.165.0rc1"}, "v1.165.0rc1"},
		{[]string{"v1.165.0rc2", "v1.165.0", "v1.165.0rc10"}, "v1.165.0"},
		{[]string{"v1.165.0rc2", "v1.165.0rc10"}, "v1.165.0rc10"},
		{[]string{"v1.99.9", "v1.100.0"}, "v1.100.0"},
		// Not release tags.
		{[]string{"v1.164.1", "v2-test", "dinsic_2019-01-01", "v1.170.0-beta"}, "v1.164.1"},
	} {
		if got, _ := newestTag(tc.tags); got != tc.want {
			t.Errorf("%v: %s, want %s", tc.tags, got, tc.want)
		}
	}
}

func TestEscape(t *testing.T) {
	for in, want := range map[string]string{
		"Fix thing (#20314)":           "Fix thing (element-hq/synapse#20314)",
		"Fix #18793 (#20081)":          "Fix element-hq/synapse#18793 (element-hq/synapse#20081)",
		"Thanks @someone for <script>": "Thanks &#64;someone for &lt;script&gt;",
		"Use `null` in C#5 notes":      "Use `null` in C#5 notes",
	} {
		if got := escape(in); got != want {
			t.Errorf("%q: %q, want %q", in, got, want)
		}
	}
}

// A throwaway repository with two tags: what changed under the manifest's
// paths is listed by component, what changed elsewhere is not, and a path
// deleted since reviewed is reported.
func TestWatch(t *testing.T) {
	dir := t.TempDir()
	git := func(args ...string) {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
		cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@example.org",
			"GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@example.org")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	write := func(path, body string) {
		t.Helper()
		p := filepath.Join(dir, path)
		_ = os.MkdirAll(filepath.Dir(p), 0o755)
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	git("init", "-q", "-b", "develop")
	write("synapse/handlers/message.py", "a\n")
	write("synapse/federation/sender/queue.py", "a\n")
	write("synapse/other.py", "a\n")
	write("synapse/gone.py", "a\n")
	git("add", ".")
	git("commit", "-qm", "start")
	git("tag", "v1.0.0")
	write("synapse/handlers/message.py", "b\n")
	git("commit", "-qam", "Change message (#42)")
	write("synapse/other.py", "b\n")
	git("commit", "-qam", "Change something we do not mirror")
	git("rm", "-q", "synapse/gone.py")
	git("commit", "-qm", "Remove gone")
	git("tag", "v1.1.0rc1")

	m := &Manifest{Reviewed: "v1.0.0", Components: map[string]Component{
		"creator": {Code: "internal/creator", Synapse: []string{"synapse/handlers/message.py", "synapse/gone.py"}},
		"sender":  {Code: "internal/sender", Synapse: []string{"synapse/federation/sender/"}},
	}}
	r, err := watch(gitRepo(dir), m)
	if err != nil {
		t.Fatal(err)
	}
	if r.Newest != "v1.1.0rc1" || !r.Changed() {
		t.Fatalf("newest %s, changed %v", r.Newest, r.Changed())
	}
	c := r.Components["creator"]
	if len(c.Commits) != 2 || c.Commits[0].Subject != "Remove gone" || c.Commits[1].Subject != "Change message (#42)" {
		t.Errorf("creator commits %+v", c.Commits)
	}
	if len(c.Gone) != 1 || c.Gone[0] != "synapse/gone.py" {
		t.Errorf("gone %v", c.Gone)
	}
	if s := r.Components["sender"]; len(s.Commits) != 0 || len(s.Gone) != 0 {
		t.Errorf("sender %+v", s)
	}
	md := r.markdown(m)
	for _, want := range []string{"## creator (`internal/creator`)", "element-hq/synapse#42", "`synapse/gone.py` no longer exists",
		"compare/v1.0.0...v1.1.0rc1", "reviewed: v1.1.0rc1"} {
		if !strings.Contains(md, want) {
			t.Errorf("report lacks %q:\n%s", want, md)
		}
	}
	if strings.Contains(md, "## sender") || strings.Contains(md, "do not mirror") {
		t.Errorf("report lists what did not change:\n%s", md)
	}

	// Reviewed caught up: nothing to report.
	m.Reviewed = "v1.1.0rc1"
	m.Components["creator"] = Component{Code: "internal/creator", Synapse: []string{"synapse/handlers/message.py"}}
	if r, err := watch(gitRepo(dir), m); err != nil || r.Changed() {
		t.Errorf("caught up: %v, changed %v", err, r != nil && r.Changed())
	}

	// A manifest path that does not exist at reviewed is the manifest's
	// mistake.
	m.Components["creator"] = Component{Synapse: []string{"synapse/handlers/messages.py"}}
	if _, err := watch(gitRepo(dir), m); err == nil || !strings.Contains(err.Error(), "fix the manifest") {
		t.Errorf("typo not caught: %v", err)
	}
}

// The manifest in the repository parses and names a release tag.
func TestManifestParses(t *testing.T) {
	m, err := loadManifest("../../synapse-watch.yaml")
	if err != nil {
		t.Fatal(err)
	}
	for name, c := range m.Components {
		if c.Code == "" || len(c.Synapse) == 0 {
			t.Errorf("component %s: code %q, %d paths", name, c.Code, len(c.Synapse))
		}
		for _, p := range c.Synapse {
			if !strings.HasPrefix(p, "synapse/") && !strings.HasPrefix(p, "rust/") {
				t.Errorf("component %s: %s is not a Synapse source path", name, p)
			}
		}
	}
}
