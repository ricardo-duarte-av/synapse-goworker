// Package server holds the HTTP plumbing: the listener, request logging, and
// the router that ties handlers together.
package server

import (
	"errors"
	"fmt"
	"net"
	"os"
)

// ListenSpec describes where to listen. Exactly one of Socket or Addr is set.
type ListenSpec struct {
	Socket     string
	Addr       string
	SocketMode os.FileMode
}

// Listen opens the configured unix socket or TCP port. A stale socket from an
// unclean shutdown is removed first, since the worker is the only thing that
// should own that path.
func Listen(spec ListenSpec) (net.Listener, error) {
	if spec.Socket == "" {
		l, err := net.Listen("tcp", spec.Addr)
		if err != nil {
			return nil, fmt.Errorf("listening on %s: %w", spec.Addr, err)
		}
		return l, nil
	}
	// The kernel caps sockaddr_un paths at 108 bytes including the terminator,
	// and the error it returns otherwise ("invalid argument") gives no hint why.
	if len(spec.Socket) > 107 {
		return nil, fmt.Errorf("socket path %q is %d bytes; the kernel limit is 107",
			spec.Socket, len(spec.Socket))
	}
	if err := removeStaleSocket(spec.Socket); err != nil {
		return nil, err
	}
	l, err := net.Listen("unix", spec.Socket)
	if err != nil {
		return nil, fmt.Errorf("listening on %s: %w", spec.Socket, err)
	}
	mode := spec.SocketMode
	if mode == 0 {
		mode = 0o660
	}
	if err := os.Chmod(spec.Socket, mode); err != nil {
		_ = l.Close()
		return nil, fmt.Errorf("setting socket permissions: %w", err)
	}
	return l, nil
}

// removeStaleSocket deletes a socket file left behind by a previous run. It
// refuses to touch anything that is not a socket, and refuses to evict a
// process that is still listening.
func removeStaleSocket(path string) error {
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return fmt.Errorf("checking socket path: %w", err)
	}
	if info.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("%q exists and is not a socket; refusing to remove it", path)
	}
	if conn, err := net.Dial("unix", path); err == nil {
		_ = conn.Close()
		return fmt.Errorf("%q is already in use by a running process", path)
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("removing stale socket: %w", err)
	}
	return nil
}

// Describe renders a listen spec for logs.
func Describe(spec ListenSpec) string {
	if spec.Socket != "" {
		return "unix:" + spec.Socket
	}
	return spec.Addr
}
