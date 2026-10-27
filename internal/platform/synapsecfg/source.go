package synapsecfg

import "sync/atomic"

// Source is the worker's one copy of homeserver.yaml.
//
// Every part of the worker reads it from here rather than parsing the file
// itself. Reload re-reads the file, for the two moments that call for it: a
// Synapse process restarting (when Synapse itself applies an edit), and a
// relay to a writer failing in a way a moved writer or rotated secret would
// explain. Reload only replaces the copy; who acts on a change, and when, is
// up to the caller.
type Source struct {
	path string
	cur  atomic.Pointer[Config]
}

// Open reads homeserver.yaml.
func Open(path string) (*Source, error) {
	cfg, err := Load(path)
	if err != nil {
		return nil, err
	}
	s := &Source{path: path}
	s.cur.Store(cfg)
	return s, nil
}

// Current is the last good copy.
func (s *Source) Current() *Config { return s.cur.Load() }

// Path is the file this was read from.
func (s *Source) Path() string { return s.path }

// Reload re-reads the file. On a failure the last good copy is kept: a
// half-written file must not stop a working worker.
func (s *Source) Reload() (*Config, error) {
	cfg, err := Load(s.path)
	if err != nil {
		return s.cur.Load(), err
	}
	s.cur.Store(cfg)
	return cfg, nil
}
