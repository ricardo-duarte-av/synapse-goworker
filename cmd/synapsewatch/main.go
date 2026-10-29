// Command synapsewatch reports what Synapse changed, since the release we
// last reviewed, in the code goworker mirrors (synapse-watch.yaml).
//
// It reads a clone of element-hq/synapse with its tags, finds the newest
// release tag (never an rc: we port against the release we deploy, and an
// rc's changes reach the release anyway), and for each component lists the
// commits and files between its reviewed tag (its own, or the manifest's) and that tag under the
// component's paths, less the Synapse PRs listed as handled. The report is
// Markdown, for the nightly workflow to put in an issue; it judges nothing.
//
//	synapsewatch -manifest synapse-watch.yaml -synapse ../synapse -out report.md
//
// Under GitHub Actions it also writes newest=<tag> and changed=true|false to
// $GITHUB_OUTPUT. A manifest path missing at `reviewed` is an error: the
// manifest is wrong. One missing at the newest tag is a change: Synapse moved
// or deleted code we mirror.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// Manifest is synapse-watch.yaml.
type Manifest struct {
	// Reviewed is the tag a component was last reviewed against, unless it
	// names its own.
	Reviewed   string               `yaml:"reviewed"`
	Components map[string]Component `yaml:"components"`
	// Handled are Synapse PR numbers ported or dismissed although they are
	// past a component's reviewed tag.
	Handled []int `yaml:"handled"`
}

// Component is one part of goworker and the Synapse paths it mirrors.
type Component struct {
	Code     string   `yaml:"code"`
	Reviewed string   `yaml:"reviewed"`
	Synapse  []string `yaml:"synapse"`
}

// reviewedOf is a component's reviewed tag.
func (m *Manifest) reviewedOf(c Component) string {
	if c.Reviewed != "" {
		return c.Reviewed
	}
	return m.Reviewed
}

// handled reports whether a commit's subject names a handled PR: Synapse's
// squash merges end in "(#1234)", and a backport may carry two.
func (m *Manifest) handled(subject string) bool {
	for _, n := range prRE.FindAllStringSubmatch(subject, -1) {
		v, _ := strconv.Atoi(n[1])
		if slices.Contains(m.Handled, v) {
			return true
		}
	}
	return false
}

var prRE = regexp.MustCompile(`\(#(\d+)\)`)

func main() {
	manifest := flag.String("manifest", "synapse-watch.yaml", "the manifest")
	synapse := flag.String("synapse", "synapse", "a clone of element-hq/synapse, with tags")
	out := flag.String("out", "", "write the report here (default stdout)")
	flag.Parse()

	m, err := loadManifest(*manifest)
	if err != nil {
		fail(err)
	}
	g := gitRepo(*synapse)
	r, err := watch(g, m)
	if err != nil {
		fail(err)
	}
	report := r.markdown(m)
	if *out == "" {
		fmt.Print(report)
	} else if err := os.WriteFile(*out, []byte(report), 0o644); err != nil {
		fail(err)
	}
	if path := os.Getenv("GITHUB_OUTPUT"); path != "" {
		f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o644)
		if err != nil {
			fail(err)
		}
		fmt.Fprintf(f, "newest=%s\nchanged=%t\n", r.Newest, r.Changed())
		_ = f.Close()
	}
	fmt.Fprintf(os.Stderr, "reviewed %s, newest %s, changed %t\n", m.Reviewed, r.Newest, r.Changed())
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "synapsewatch:", err)
	os.Exit(1)
}

func loadManifest(path string) (*Manifest, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var m Manifest
	if err := yaml.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if _, ok := parseVersion(m.Reviewed); !ok {
		return nil, fmt.Errorf("%s: reviewed %q is not a release tag", path, m.Reviewed)
	}
	if len(m.Components) == 0 {
		return nil, fmt.Errorf("%s: no components", path)
	}
	for name, c := range m.Components {
		if c.Reviewed != "" {
			if _, ok := parseVersion(c.Reviewed); !ok {
				return nil, fmt.Errorf("%s: component %s: reviewed %q is not a release tag", path, name, c.Reviewed)
			}
		}
	}
	return &m, nil
}

// version is a Synapse release tag, vX.Y.Z. Release candidates (vX.Y.ZrcN)
// do not match: porting from one would put goworker ahead of the Synapse it
// runs beside.
type version struct{ major, minor, patch int }

var tagRE = regexp.MustCompile(`^v(\d+)\.(\d+)\.(\d+)$`)

func parseVersion(tag string) (version, bool) {
	m := tagRE.FindStringSubmatch(tag)
	if m == nil {
		return version{}, false
	}
	n := func(s string) int { v, _ := strconv.Atoi(s); return v }
	return version{n(m[1]), n(m[2]), n(m[3])}, true
}

func (v version) compare(w version) int {
	for _, d := range []int{v.major - w.major, v.minor - w.minor, v.patch - w.patch} {
		if d != 0 {
			return d
		}
	}
	return 0
}

// newestTag is the newest release tag.
func newestTag(tags []string) (string, bool) {
	best, bestV, found := "", version{}, false
	for _, t := range tags {
		v, ok := parseVersion(t)
		if ok && (!found || v.compare(bestV) > 0) {
			best, bestV, found = t, v, true
		}
	}
	return best, found
}

// git runs git in a repository.
type gitRepo string

func (g gitRepo) run(args ...string) (string, error) {
	cmd := exec.Command("git", append([]string{"-C", string(g)}, args...)...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("git %s: %v: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return string(out), nil
}

func (g gitRepo) lines(args ...string) ([]string, error) {
	out, err := g.run(args...)
	if err != nil {
		return nil, err
	}
	var ls []string
	for _, l := range strings.Split(out, "\n") {
		if l = strings.TrimSpace(l); l != "" {
			ls = append(ls, l)
		}
	}
	return ls, nil
}

// exists reports whether path, a file or a directory, is in the tree at ref.
func (g gitRepo) exists(ref, path string) (bool, error) {
	ls, err := g.lines("ls-tree", "--name-only", ref, "--", strings.TrimSuffix(path, "/"))
	return len(ls) > 0, err
}

// Result is what changed between each component's reviewed tag and the
// newest tag.
type Result struct {
	Newest string
	// By component name.
	Components map[string]*ComponentResult
}

// ComponentResult is one component's share of the changes.
type ComponentResult struct {
	// Reviewed is the tag the component's changes are counted from.
	Reviewed string
	Commits  []Commit
	// Stat is git diff --stat over the component's paths.
	Stat string
	// Gone are manifest paths that no longer exist at the newest tag.
	Gone []string
}

// Commit is one commit touching a component's paths.
type Commit struct {
	Hash, Subject string
}

// Changed reports whether anything mirrored changed.
func (r *Result) Changed() bool {
	for _, c := range r.Components {
		if len(c.Commits) > 0 || len(c.Gone) > 0 {
			return true
		}
	}
	return false
}

func watch(g gitRepo, m *Manifest) (*Result, error) {
	tags, err := g.lines("tag", "--list", "v*")
	if err != nil {
		return nil, err
	}
	newest, ok := newestTag(tags)
	if !ok {
		return nil, fmt.Errorf("no release tags in %s: was it cloned with tags?", g)
	}
	nv, _ := parseVersion(newest)
	r := &Result{Newest: newest, Components: map[string]*ComponentResult{}}

	for name, c := range m.Components {
		reviewed := m.reviewedOf(c)
		if !slices.Contains(tags, reviewed) {
			return nil, fmt.Errorf("component %s: reviewed tag %s is not in %s", name, reviewed, g)
		}
		// A path the component mirrors must exist at its reviewed tag, or at
		// the newest if it is new since (added to the manifest as ported).
		for _, p := range c.Synapse {
			ok, err := g.exists(reviewed, p)
			if err == nil && !ok {
				ok, err = g.exists(newest, p)
			}
			if err != nil {
				return nil, err
			}
			if !ok {
				return nil, fmt.Errorf("component %s: %s is not in Synapse at %s; fix the manifest", name, p, reviewed)
			}
		}
		cr := &ComponentResult{Reviewed: reviewed}
		r.Components[name] = cr
		if rv, _ := parseVersion(reviewed); nv.compare(rv) <= 0 {
			continue
		}
		span := reviewed + ".." + newest
		args := append([]string{"log", "--no-merges", "--format=%H%x09%s", span, "--"}, c.Synapse...)
		ls, err := g.lines(args...)
		if err != nil {
			return nil, err
		}
		for _, l := range ls {
			hash, subject, _ := strings.Cut(l, "\t")
			if m.handled(subject) {
				continue
			}
			cr.Commits = append(cr.Commits, Commit{hash, subject})
		}
		if len(cr.Commits) > 0 {
			stat, err := g.run(append([]string{"diff", "--stat=100", span, "--"}, c.Synapse...)...)
			if err != nil {
				return nil, err
			}
			cr.Stat = strings.TrimRight(stat, "\n")
		}
		for _, p := range c.Synapse {
			if was, err := g.exists(reviewed, p); err != nil || !was {
				// New since reviewed, so not gone.
				continue
			}
			ok, err := g.exists(newest, p)
			if err != nil {
				return nil, err
			}
			if !ok {
				cr.Gone = append(cr.Gone, p)
			}
		}
	}
	return r, nil
}

// maxBody keeps the issue under GitHub's 65,536-character limit.
const maxBody = 60000

const repoURL = "https://github.com/element-hq/synapse"

func (r *Result) markdown(m *Manifest) string {
	var b strings.Builder
	if !r.Changed() {
		fmt.Fprintf(&b, "Nothing goworker mirrors changed up to Synapse %s that is not reviewed or handled.\n", r.Newest)
		return b.String()
	}
	fmt.Fprintf(&b, "Synapse up to **%s** changed code goworker mirrors since each component's reviewed tag.\n\n", r.Newest)
	b.WriteString("For each change, port it (see the component's notes) or decide it does not affect us, " +
		"and add its PR number to `handled:` in `synapse-watch.yaml`; once a component is clear, " +
		"set its `reviewed:` to " + r.Newest + " and drop the numbers it covers. " +
		"This issue closes on the first nightly run with nothing left.\n")

	names := make([]string, 0, len(r.Components))
	for n := range r.Components {
		names = append(names, n)
	}
	slices.Sort(names)
	for _, n := range names {
		c := r.Components[n]
		if len(c.Commits) == 0 && len(c.Gone) == 0 {
			continue
		}
		fmt.Fprintf(&b, "\n## %s (`%s`), since %s\n\n[compare](%s/compare/%s...%s)\n\n",
			n, m.Components[n].Code, c.Reviewed, repoURL, c.Reviewed, r.Newest)
		for _, p := range c.Gone {
			fmt.Fprintf(&b, "- **`%s` no longer exists at %s**: moved or deleted.\n", p, r.Newest)
		}
		for _, cm := range c.Commits {
			fmt.Fprintf(&b, "- [ ] [`%s`](%s/commit/%s) %s\n", cm.Hash[:10], repoURL, cm.Hash, escape(cm.Subject))
		}
		if c.Stat != "" {
			fmt.Fprintf(&b, "\n<details><summary>Files</summary>\n\n```\n%s\n```\n</details>\n", c.Stat)
		}
	}
	s := b.String()
	if len(s) > maxBody {
		s = s[:maxBody] + "\n\n…truncated: see the compare link above.\n"
	}
	return s
}

// escape makes a Synapse commit subject safe in a goworker issue: "#1234"
// would link goworker's own issue 1234, so it names Synapse's; "@name"
// would notify that GitHub user, so it is broken up; and "<" would open
// HTML.
func escape(s string) string {
	s = issueRE.ReplaceAllString(s, "element-hq/synapse#$1")
	return strings.NewReplacer("<", "&lt;", ">", "&gt;", "@", "&#64;").Replace(s)
}

var issueRE = regexp.MustCompile(`(?:^|\B)#(\d+)\b`)
