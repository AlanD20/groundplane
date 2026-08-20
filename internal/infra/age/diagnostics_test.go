package age

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	filippoage "filippo.io/age"
)

// Rationale: diagnostics must derive the same deterministic public recipient
// from a valid identity without requiring the test process to run as root.
func TestReadControllerRecipientReadsExistingIdentity(t *testing.T) {
	directory := secureTempDir(t)
	identity, err := filippoage.GenerateX25519Identity()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	path := filepath.Join(directory, "controller.age")
	if err := os.WriteFile(path, []byte(identity.String()+"\n"), 0o600); err != nil {
		t.Fatalf("write identity: %v", err)
	}

	want := identity.Recipient().String()
	for range 2 {
		got, readErr := readControllerRecipient(context.Background(), path, uint32(os.Getuid()))
		if readErr != nil || got != want {
			t.Fatalf("readControllerRecipient() = %q, %v; want %q", got, readErr, want)
		}
	}
}

// Rationale: a missing diagnostic input must not bootstrap either the key or
// its absent parent directory.
func TestReadControllerRecipientDoesNotCreateMissingInput(t *testing.T) {
	parent := filepath.Join(secureTempDir(t), "missing")
	path := filepath.Join(parent, "controller.age")
	if _, err := ReadControllerRecipient(context.Background(), path); err == nil {
		t.Fatal("ReadControllerRecipient() error = nil")
	}
	if _, err := os.Lstat(parent); !os.IsNotExist(err) {
		t.Fatalf("missing parent was created or could not be inspected: %v", err)
	}
}

// Rationale: symlinks and non-regular files must not redirect root-only key
// inspection outside the configured file boundary.
func TestReadControllerRecipientRejectsSymlinkAndNonRegularFiles(t *testing.T) {
	for _, test := range []struct {
		name  string
		setup func(*testing.T, string)
	}{
		{name: "symlink", setup: func(t *testing.T, path string) {
			target := path + ".target"
			if err := os.WriteFile(target, []byte("not-used"), 0o600); err != nil {
				t.Fatalf("write target: %v", err)
			}
			if err := os.Symlink(target, path); err != nil {
				t.Fatalf("create symlink: %v", err)
			}
		}},
		{name: "directory", setup: func(t *testing.T, path string) {
			if err := os.Mkdir(path, 0o600); err != nil {
				t.Fatalf("create directory: %v", err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(secureTempDir(t), "controller.age")
			test.setup(t, path)
			if _, err := readControllerRecipient(context.Background(), path, uint32(os.Getuid())); err == nil {
				t.Fatal("readControllerRecipient() error = nil")
			}
		})
	}
}

// Rationale: permissions are part of the Controller key contract; accepting a
// group- or world-readable identity would expose every stored secret.
func TestReadControllerRecipientRejectsWrongMode(t *testing.T) {
	path := filepath.Join(secureTempDir(t), "controller.age")
	if err := os.WriteFile(path, []byte("malformed"), 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatalf("change key mode: %v", err)
	}
	if _, err := readControllerRecipient(context.Background(), path, uint32(os.Getuid())); err == nil {
		t.Fatal("readControllerRecipient() error = nil")
	}
}

// Rationale: bounded reads prevent an attacker-controlled key path from
// turning a local diagnostic into unbounded memory consumption.
func TestReadControllerRecipientRejectsOversizedIdentity(t *testing.T) {
	path := filepath.Join(secureTempDir(t), "controller.age")
	if err := os.WriteFile(path, []byte(strings.Repeat("x", maxIdentitySize+1)), 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	if _, err := readControllerRecipient(context.Background(), path, uint32(os.Getuid())); err == nil {
		t.Fatal("readControllerRecipient() error = nil")
	}
}

// Rationale: malformed identity text must never produce a plausible-looking
// fingerprint for a key the Controller itself cannot use.
func TestReadControllerRecipientRejectsMalformedIdentity(t *testing.T) {
	path := filepath.Join(secureTempDir(t), "controller.age")
	if err := os.WriteFile(path, []byte("not-an-age-identity\n"), 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	if _, err := readControllerRecipient(context.Background(), path, uint32(os.Getuid())); err == nil {
		t.Fatal("readControllerRecipient() error = nil")
	}
}

// Rationale: cancellation is process control, not a malformed-key result, and
// must be preserved before any filesystem access occurs.
func TestReadControllerRecipientPreservesCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := ReadControllerRecipient(ctx, "/does/not/exist")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ReadControllerRecipient() error = %v", err)
	}
}

func secureTempDir(t *testing.T) string {
	t.Helper()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatalf("secure temporary directory: %v", err)
	}
	return directory
}
