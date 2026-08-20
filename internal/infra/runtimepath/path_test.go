package runtimepath

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestPrepareDirectoryPreservesTrustedModeAndSecuresManagedPath(t *testing.T) {
	// Rationale: securing a Controller runtime must not change the host /run
	// contract while every Groundplane-owned descendant stays private.
	t.Parallel()

	hostRoot := t.TempDir()
	if err := os.Mkdir(filepath.Join(hostRoot, "run"), 0o755); err != nil {
		t.Fatalf("create trusted ancestor: %v", err)
	}
	root, err := os.OpenRoot(hostRoot)
	if err != nil {
		t.Fatalf("OpenRoot() error = %v", err)
	}
	defer root.Close()

	exists, err := PrepareDirectory(
		context.Background(), root, "run/groundplane/controller", uint32(os.Geteuid()), 1, true,
	)
	if err != nil || !exists {
		t.Fatalf("PrepareDirectory() = %v, %v; want true, nil", exists, err)
	}
	assertMode(t, filepath.Join(hostRoot, "run"), 0o755)
	assertMode(t, filepath.Join(hostRoot, "run", "groundplane"), 0o700)
	assertMode(t, filepath.Join(hostRoot, "run", "groundplane", "controller"), 0o700)
}

func TestPrepareDirectoryRefusesSymlinkAndWrongOwner(t *testing.T) {
	// Rationale: an attacker-controlled ancestor must never redirect runtime
	// materialization or cause creation below an untrusted owner.
	t.Parallel()

	for _, test := range []struct {
		name                string
		prepare             func(t *testing.T, hostRoot string)
		ownerUID            uint32
		managedMustNotExist bool
	}{
		{
			name: "symlink",
			prepare: func(t *testing.T, hostRoot string) {
				t.Helper()
				if err := os.Mkdir(filepath.Join(hostRoot, "target"), 0o700); err != nil {
					t.Fatalf("create target: %v", err)
				}
				if err := os.Symlink(filepath.Join(hostRoot, "target"), filepath.Join(hostRoot, "run")); err != nil {
					t.Fatalf("create symlink: %v", err)
				}
			},
			ownerUID: uint32(os.Geteuid()),
		},
		{
			name: "wrong owner",
			prepare: func(t *testing.T, hostRoot string) {
				t.Helper()
				if err := os.Mkdir(filepath.Join(hostRoot, "run"), 0o755); err != nil {
					t.Fatalf("create run: %v", err)
				}
			},
			ownerUID:            uint32(os.Geteuid()) ^ 1,
			managedMustNotExist: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			hostRoot := t.TempDir()
			test.prepare(t, hostRoot)
			root, err := os.OpenRoot(hostRoot)
			if err != nil {
				t.Fatalf("OpenRoot() error = %v", err)
			}
			defer root.Close()

			if _, err := PrepareDirectory(
				context.Background(), root, "run/groundplane/controller", test.ownerUID, 1, true,
			); err == nil {
				t.Fatal("PrepareDirectory() error = nil, want refusal")
			}
			if test.managedMustNotExist {
				if _, err := os.Lstat(filepath.Join(hostRoot, "run", "groundplane")); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("managed path stat error = %v, want not exist", err)
				}
			}
		})
	}
}

func assertMode(t *testing.T, name string, want os.FileMode) {
	t.Helper()
	info, err := os.Stat(name)
	if err != nil {
		t.Fatalf("stat %s: %v", name, err)
	}
	if got := info.Mode().Perm(); got != want {
		t.Fatalf("mode %s = %04o, want %04o", name, got, want)
	}
}
