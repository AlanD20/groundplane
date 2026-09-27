package runnerproxy

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// RUN-04: exhausting the peer limit must not prevent shutdown and revocation.
// Closing only the Unix socket leaves Accept blocked on the capacity semaphore.
func TestPeerListenerCloseRevokesSaturatedAdmission(t *testing.T) {
	// A short directory name keeps the repository-local path within sockaddr_un.
	directory, err := os.MkdirTemp("", "rp-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	socket, err := net.ListenUnix(
		"unix",
		&net.UnixAddr{Name: filepath.Join(directory, "proxy.sock"), Net: "unix"},
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	listener := &peerListener{UnixListener: socket, ctx: ctx, cancel: cancel, slots: make(chan struct{}, 1)}
	listener.slots <- struct{}{}
	result := make(chan error, 1)
	go func() {
		connection, err := listener.Accept()
		if connection != nil {
			_ = connection.Close()
		}
		result <- err
	}()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("closed listener retained admission authority: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown left the admission goroutine blocked")
	}
}
