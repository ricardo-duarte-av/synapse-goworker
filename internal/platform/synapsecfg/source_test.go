package synapsecfg

import (
	"os"
	"testing"
)

func TestSourceKeepsTheLastGoodCopy(t *testing.T) {
	p := write(t, "server_name: a.example\n")
	src, err := Open(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte("server_name: b.example\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if cfg, err := src.Reload(); err != nil || cfg.ServerName != "b.example" || src.Current().ServerName != "b.example" {
		t.Fatalf("reload: %v %v", cfg, err)
	}
	if err := os.WriteFile(p, []byte("server_name: [half-written\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := src.Reload(); err == nil {
		t.Fatal("a broken file was accepted")
	}
	if src.Current().ServerName != "b.example" {
		t.Errorf("a failed reload replaced the copy: %q", src.Current().ServerName)
	}
}
