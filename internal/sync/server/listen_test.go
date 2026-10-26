package server

import (
	"context"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestListenUnixSocketAndPermissions(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "w.sock")

	l, err := Listen(ListenSpec{Socket: path, SocketMode: 0o660})
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer func() { _ = l.Close() }()

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Mode()&os.ModeSocket == 0 {
		t.Fatal("path is not a socket")
	}
	if got := info.Mode().Perm(); got != 0o660 {
		t.Errorf("mode = %o, want 660", got)
	}
}

// Production listens on a unix socket. media-worker's notes record a bug that a
// TCP-only test run hid, so the transport is exercised here rather than assumed
// equivalent.
func TestServeOverUnixSocket(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "w.sock")
	l, err := Listen(ListenSpec{Socket: path})
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}

	srv := &http.Server{Handler: NewMux(Routes{})}
	go func() { _ = srv.Serve(l) }()
	defer func() { _ = srv.Close() }()

	resp, err := unixGet(path, "http://localhost/health")
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}

func TestListenRefusesNonSocketFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "not-a-socket")
	if err := os.WriteFile(path, []byte("hello"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := Listen(ListenSpec{Socket: path})
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "not a socket") {
		t.Errorf("error = %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Error("the file should not have been removed")
	}
}

func TestListenRefusesSocketInUse(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "w.sock")
	l, err := Listen(ListenSpec{Socket: path})
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer func() { _ = l.Close() }()

	_, err = Listen(ListenSpec{Socket: path})
	if err == nil {
		t.Fatal("expected an error for a socket already in use")
	}
	if !strings.Contains(err.Error(), "already in use") {
		t.Errorf("error = %v", err)
	}
}

func TestListenRemovesStaleSocket(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "w.sock")

	// What an unclean shutdown leaves behind: a socket file with nothing
	// listening. SetUnlinkOnClose(false) reproduces it, since Go otherwise
	// tidies up on Close and there would be nothing stale to test.
	stale, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("creating stale socket: %v", err)
	}
	stale.(*net.UnixListener).SetUnlinkOnClose(false)
	if err := stale.Close(); err != nil {
		t.Fatalf("closing stale socket: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("stale socket did not survive Close: %v", err)
	}

	l, err := Listen(ListenSpec{Socket: path})
	if err != nil {
		t.Fatalf("Listen over stale socket: %v", err)
	}
	_ = l.Close()
}

// The kernel's error for an over-long path is a bare "invalid argument", which
// gives no hint at the cause.
func TestListenRejectsOverlongSocketPath(t *testing.T) {
	long := "/tmp/" + strings.Repeat("a", 200) + ".sock"
	_, err := Listen(ListenSpec{Socket: long})
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "kernel limit") {
		t.Errorf("error should explain the limit, got: %v", err)
	}
}

// unixGet performs a GET over a unix socket, the way the deployment is reached.
func unixGet(socket, url string) (*http.Response, error) {
	client := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", socket)
		},
	}}
	return client.Get(url)
}
