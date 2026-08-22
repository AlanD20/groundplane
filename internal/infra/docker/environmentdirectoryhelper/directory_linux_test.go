//go:build linux

package environmentdirectoryhelper

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestCreateDirectoriesIsDescriptorRelativeSecureAndIdempotent(t *testing.T) {
	root := filepath.Join(t.TempDir(), "vol")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatalf("Mkdir(root) error = %v", err)
	}
	directory := root + "/tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV/prj_01ARZ3NDEKTSV4RRFFQ69G5FAV/env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	for attempt := 0; attempt < 2; attempt++ {
		if err := createDirectories(context.Background(), root, directory, uint32(os.Getuid())); err != nil {
			t.Fatalf("createDirectories(attempt %d) error = %v", attempt+1, err)
		}
	}
	for current := directory; current != root; current = filepath.Dir(current) {
		info, err := os.Stat(current)
		if err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 {
			t.Fatalf("directory %q = %#v, %v", current, info, err)
		}
	}
}

func TestCreateDirectoriesRejectsSymlinkTraversal(t *testing.T) {
	root := filepath.Join(t.TempDir(), "vol")
	outside := t.TempDir()
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatalf("Mkdir(root) error = %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV")); err != nil {
		t.Fatalf("Symlink() error = %v", err)
	}
	directory := root + "/tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV/prj_01ARZ3NDEKTSV4RRFFQ69G5FAV/env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	if err := createDirectories(context.Background(), root, directory, uint32(os.Getuid())); err == nil {
		t.Fatal("createDirectories() accepted symlink traversal")
	}
}

// Rationale: managed volume binds need durable direct children without
// weakening the private Environment namespace or following a hostile leaf.
func TestEnsureManagedVolumeDirectoriesCreatesDirectAccessibleLeaves(t *testing.T) {
	root := filepath.Join(t.TempDir(), "vol")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatalf("Mkdir(root) error = %v", err)
	}
	directory := root + "/tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV/prj_01ARZ3NDEKTSV4RRFFQ69G5FAV/" +
		"env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	if err := createDirectories(context.Background(), root, directory, uint32(os.Getuid())); err != nil {
		t.Fatalf("createDirectories() error = %v", err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		if err := ensureManagedVolumeDirectories(
			context.Background(), root, directory, []string{"app-data", "cache"}, uint32(os.Getuid()),
		); err != nil {
			t.Fatalf("ensureManagedVolumeDirectories(attempt %d) error = %v", attempt+1, err)
		}
	}
	for _, name := range []string{"app-data", "cache"} {
		info, err := os.Stat(filepath.Join(directory, name))
		if err != nil || !info.IsDir() || info.Mode().Perm() != 0o755 {
			t.Fatalf("managed volume directory %q = %#v, %v", name, info, err)
		}
	}
	environmentInfo, err := os.Stat(directory)
	if err != nil || environmentInfo.Mode().Perm() != 0o700 {
		t.Fatalf("Environment directory = %#v, %v", environmentInfo, err)
	}
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(directory, "hostile")); err != nil {
		t.Fatalf("Symlink() error = %v", err)
	}
	if err := ensureManagedVolumeDirectories(
		context.Background(), root, directory, []string{"hostile"}, uint32(os.Getuid()),
	); err == nil {
		t.Fatal("ensureManagedVolumeDirectories() accepted symlink leaf")
	}
}
