package agentlistener

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
)

func TestListenAtCreatesRestrictedConnectableSocket(t *testing.T) {
	// Rationale: the Agent mount must expose a usable socket while host runtime
	// ancestors retain the accepted least-privilege modes.
	t.Parallel()

	hostRoot := shortUDSTestDir(t)
	if err := os.Mkdir(filepath.Join(hostRoot, "run"), 0o755); err != nil {
		t.Fatalf("create run: %v", err)
	}
	listener, err := listenAt(context.Background(), hostRoot, uint32(os.Geteuid()))
	if err != nil {
		t.Fatalf("listenAt() error = %v", err)
	}
	defer listener.Close()

	assertSocketMode(t, filepath.Join(hostRoot, "run"), 0o755, false)
	assertSocketMode(t, filepath.Join(hostRoot, "run", "groundplane"), 0o700, false)
	assertSocketMode(t, filepath.Join(hostRoot, "run", "groundplane", "controller"), 0o700, false)
	hostSocket := filepath.Join(hostRoot, filepath.FromSlash(runtimeSocketName()))
	assertSocketMode(t, hostSocket, 0o600, true)

	accepted := make(chan error, 1)
	go func() {
		connection, acceptErr := listener.Accept()
		if acceptErr == nil {
			acceptErr = connection.Close()
		}
		accepted <- acceptErr
	}()
	connection, err := net.Dial("unix", hostSocket)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	if err := connection.Close(); err != nil {
		t.Fatalf("close client: %v", err)
	}
	if err := <-accepted; err != nil {
		t.Fatalf("Accept() error = %v", err)
	}
}

func TestListenAtRefusesUntrustedExistingPaths(t *testing.T) {
	// Rationale: startup must fail closed instead of deleting a regular file or
	// following a symlink placed at a privileged runtime path.
	t.Parallel()

	for _, test := range []struct {
		name    string
		prepare func(t *testing.T, hostRoot string)
	}{
		{
			name: "symlink parent",
			prepare: func(t *testing.T, hostRoot string) {
				t.Helper()
				if err := os.MkdirAll(filepath.Join(hostRoot, "run"), 0o755); err != nil {
					t.Fatalf("create run: %v", err)
				}
				if err := os.Mkdir(filepath.Join(hostRoot, "target"), 0o700); err != nil {
					t.Fatalf("create target: %v", err)
				}
				if err := os.Symlink(filepath.Join(hostRoot, "target"), filepath.Join(hostRoot, "run", "groundplane")); err != nil {
					t.Fatalf("create symlink: %v", err)
				}
			},
		},
		{
			name: "regular socket path",
			prepare: func(t *testing.T, hostRoot string) {
				t.Helper()
				directory := filepath.Join(hostRoot, "run", "groundplane", "controller")
				if err := os.MkdirAll(directory, 0o700); err != nil {
					t.Fatalf("create directory: %v", err)
				}
				if err := os.WriteFile(filepath.Join(directory, "agent.sock"), []byte("keep"), 0o600); err != nil {
					t.Fatalf("write regular file: %v", err)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			hostRoot := shortUDSTestDir(t)
			test.prepare(t, hostRoot)
			if listener, err := listenAt(context.Background(), hostRoot, uint32(os.Geteuid())); err == nil {
				listener.Close()
				t.Fatal("listenAt() error = nil, want refusal")
			}
		})
	}
}

func TestListenAtHonorsCancellationBeforeCreatingRuntime(t *testing.T) {
	// Rationale: a cancelled Controller startup must not leave a socket or
	// partial runtime hierarchy behind.
	t.Parallel()

	hostRoot := shortUDSTestDir(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	listener, err := listenAt(ctx, hostRoot, uint32(os.Geteuid()))
	if !errors.Is(err, context.Canceled) || listener != nil {
		t.Fatalf("listenAt() = %v, %v; want nil, context.Canceled", listener, err)
	}
	if _, err := os.Lstat(filepath.Join(hostRoot, "run")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("runtime stat error = %v, want not exist", err)
	}
}

func runtimeSocketName() string {
	return filepath.ToSlash(agentprotocol.SocketPath[1:])
}

func shortUDSTestDir(t *testing.T) string {
	t.Helper()
	directory, err := os.MkdirTemp("/tmp", "gp-uds-")
	if err != nil {
		t.Fatalf("create short Unix socket directory: %v", err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(directory); err != nil {
			t.Errorf("remove short Unix socket directory: %v", err)
		}
	})
	return directory
}

func assertSocketMode(t *testing.T, name string, want os.FileMode, socket bool) {
	t.Helper()
	info, err := os.Lstat(name)
	if err != nil {
		t.Fatalf("Lstat(%s) error = %v", name, err)
	}
	if got := info.Mode().Perm(); got != want {
		t.Fatalf("mode %s = %04o, want %04o", name, got, want)
	}
	if got := info.Mode()&os.ModeSocket != 0; got != socket {
		t.Fatalf("socket %s = %v, want %v", name, got, socket)
	}
}
