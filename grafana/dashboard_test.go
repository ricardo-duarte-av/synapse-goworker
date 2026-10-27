// Package grafana holds the dashboard. goworker.json is the source: edit it in
// Grafana, export it, and save it over the file. These tests are what keeps an
// edited export honest.
package grafana

import (
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

var metricName = regexp.MustCompile(`\b((?:gosync|goinbound|gofed|gopro|goworker)_[a-z0-9_]+)`)

type panel struct {
	ID         int             `json:"id"`
	Type       string          `json:"type"`
	Title      string          `json:"title"`
	Datasource json.RawMessage `json:"datasource"`
	Panels     []panel         `json:"panels"`
	Targets    []struct {
		Expr       string          `json:"expr"`
		Datasource json.RawMessage `json:"datasource"`
	} `json:"targets"`
}

type dashboard struct {
	Panels      []panel `json:"panels"`
	Annotations struct {
		List []struct {
			Expr string `json:"expr"`
		} `json:"list"`
	} `json:"annotations"`
	Templating struct {
		List []struct {
			Name string `json:"name"`
		} `json:"list"`
	} `json:"templating"`
}

func load(t *testing.T) (dashboard, []panel) {
	t.Helper()
	b, err := os.ReadFile("goworker.json")
	if err != nil {
		t.Fatal(err)
	}
	var d dashboard
	if err := json.Unmarshal(b, &d); err != nil {
		t.Fatalf("goworker.json: %v", err)
	}
	var all []panel
	var walk func([]panel)
	walk = func(ps []panel) {
		for _, p := range ps {
			all = append(all, p)
			walk(p.Panels)
		}
	}
	walk(d.Panels)
	return d, all
}

func exprs(d dashboard, all []panel) []string {
	var out []string
	for _, p := range all {
		for _, tg := range p.Targets {
			out = append(out, tg.Expr)
		}
	}
	for _, a := range d.Annotations.List {
		out = append(out, a.Expr)
	}
	return out
}

func base(m string) string {
	for _, s := range []string{"_bucket", "_count", "_sum"} {
		if strings.HasSuffix(m, s) {
			return strings.TrimSuffix(m, s)
		}
	}
	return m
}

// registered finds every metric name the code declares, by reading it. The
// three ways names are spelled: a whole quoted name; gopro's Namespace "gopro"
// plus Name; and a quoted prefix joined to desc("suffix", ...) calls.
func registered(t *testing.T) map[string]bool {
	t.Helper()
	quoted := regexp.MustCompile(`"((?:gosync|goinbound|gofed|gopro|goworker)_[a-z0-9_]*[a-z0-9])"`)
	namespace := regexp.MustCompile(`Namespace:\s*"gopro"`)
	name := regexp.MustCompile(`Name:\s*"([a-z0-9_]+)"`)
	prefix := regexp.MustCompile(`"((?:gosync|goinbound|gofed|gopro|goworker)_[a-z0-9_]*_)"\s*\+`)
	desc := regexp.MustCompile(`desc\("([a-z0-9_]+)"`)
	names := map[string]bool{}
	for _, dir := range []string{"../internal", "../cmd"} {
		err := filepath.WalkDir(dir, func(path string, e fs.DirEntry, err error) error {
			if err != nil || e.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return err
			}
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			src := string(b)
			for _, m := range quoted.FindAllStringSubmatch(src, -1) {
				names[m[1]] = true
			}
			if namespace.MatchString(src) {
				for _, m := range name.FindAllStringSubmatch(src, -1) {
					if !strings.HasPrefix(m[1], "gopro_") {
						names["gopro_"+m[1]] = true
					}
				}
			}
			for _, p := range prefix.FindAllStringSubmatch(src, -1) {
				for _, m := range desc.FindAllStringSubmatch(src, -1) {
					names[p[1]+m[1]] = true
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	return names
}

// TestEveryQueriedMetricExists: a renamed or removed metric leaves its panel
// silently empty, which looks exactly like a part nginx sends nothing to.
func TestEveryQueriedMetricExists(t *testing.T) {
	d, all := load(t)
	reg := registered(t)
	var missing []string
	seen := map[string]bool{}
	for _, e := range exprs(d, all) {
		for _, m := range metricName.FindAllString(e, -1) {
			if b := base(m); !reg[b] && !reg[m] && !seen[b] {
				seen[b] = true
				missing = append(missing, b)
			}
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Errorf("the dashboard queries metrics the code does not register: %s", strings.Join(missing, ", "))
	}
}

// TestEveryMetricHasAPanel: the dashboard shows everything the worker exports,
// so a new metric gets a panel when it is added, not when someone goes looking.
func TestEveryMetricHasAPanel(t *testing.T) {
	d, all := load(t)
	used := map[string]bool{}
	for _, e := range exprs(d, all) {
		for _, m := range metricName.FindAllString(e, -1) {
			used[base(m)] = true
		}
	}
	var missing []string
	for n := range registered(t) {
		if !used[n] {
			missing = append(missing, n)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Errorf("metrics with no panel on goworker.json: %s", strings.Join(missing, ", "))
	}
}

// TestPanelsAreWellFormed: an export from Grafana can carry a hard-coded data
// source uid, a variable that no longer exists, or duplicate ids after a
// copy-paste; each breaks a panel on someone else's Grafana.
func TestPanelsAreWellFormed(t *testing.T) {
	d, all := load(t)
	vars := map[string]bool{"__range": true, "__rate_interval": true, "__interval": true}
	for _, v := range d.Templating.List {
		vars[v.Name] = true
	}
	ref := regexp.MustCompile(`\$\{?(\w+)`)
	ids := map[int]string{}
	for _, p := range all {
		if prev, ok := ids[p.ID]; ok {
			t.Errorf("panel id %d used by %q and %q", p.ID, prev, p.Title)
		}
		ids[p.ID] = p.Title
		dss := []json.RawMessage{p.Datasource}
		for _, tg := range p.Targets {
			dss = append(dss, tg.Datasource)
			for _, m := range ref.FindAllStringSubmatch(tg.Expr, -1) {
				if !vars[m[1]] {
					t.Errorf("panel %q uses $%s, which is not a dashboard variable", p.Title, m[1])
				}
			}
		}
		if p.Type == "row" {
			continue
		}
		for _, ds := range dss {
			if len(ds) == 0 || string(ds) == "null" {
				continue
			}
			if !strings.Contains(string(ds), `"${datasource}"`) {
				t.Errorf("panel %q has data source %s, want ${datasource}", p.Title, ds)
			}
		}
	}
}
