//go:build linux

package scriptrunner

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

func TestBodyStorePublishesAdoptsAndRemovesExactBody(t *testing.T) {
	root := t.TempDir()
	if err := os.Chmod(root, directoryMode); err != nil {
		t.Fatalf("Chmod(root) error = %v", err)
	}
	store, err := newBodyStore(root, uint32(os.Geteuid()), uint32(os.Getegid()))
	if err != nil {
		t.Fatalf("newBodyStore() error = %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	assignmentID := ids.New(ids.KindAssignment)
	executionID := ids.NewULID()
	body := []byte("php artisan migrate --force\n")
	prepared, err := store.Prepare(
		assignmentID,
		executionID,
		body,
		uint32(os.Geteuid()),
		uint32(os.Getegid()),
	)
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	wantPath := filepath.Join(root, bodyStoreTasks, assignmentID, executionID, bodyLeaf)
	if prepared.hostPath != wantPath || prepared.identity.Leaf != bodyLeaf {
		t.Fatalf("Prepare() = %#v", prepared)
	}
	info, err := os.Lstat(wantPath)
	if err != nil {
		t.Fatalf("Lstat(body) error = %v", err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || info.Mode().Perm() != bodyMode || stat.Nlink != 1 ||
		uint64(stat.Dev) != prepared.identity.Device || stat.Ino != prepared.identity.Inode {
		t.Fatalf("body evidence = %#v, %#v", info, stat)
	}

	adopted, err := store.Prepare(
		assignmentID,
		executionID,
		body,
		uint32(os.Geteuid()),
		uint32(os.Getegid()),
	)
	if err != nil {
		t.Fatalf("Prepare(adopt) error = %v", err)
	}
	if adopted.identity != prepared.identity {
		t.Fatalf("adopted identity = %#v, want %#v", adopted.identity, prepared.identity)
	}
	if _, err := store.Prepare(
		assignmentID,
		executionID,
		[]byte("different\n"),
		uint32(os.Geteuid()),
		uint32(os.Getegid()),
	); err == nil {
		t.Fatal("Prepare(different) error = nil")
	}
	if err := store.Remove(prepared); err != nil {
		t.Fatalf("Remove() error = %v", err)
	}
	if _, err := os.Lstat(filepath.Dir(wantPath)); !os.IsNotExist(err) {
		t.Fatalf("execution directory remains, error = %v", err)
	}
	if err := store.Remove(prepared); err != nil {
		t.Fatalf("Remove(replay) error = %v", err)
	}
}

func TestBodyStoreRefusesReplacementInode(t *testing.T) {
	root := t.TempDir()
	if err := os.Chmod(root, directoryMode); err != nil {
		t.Fatalf("Chmod(root) error = %v", err)
	}
	store, err := newBodyStore(root, uint32(os.Geteuid()), uint32(os.Getegid()))
	if err != nil {
		t.Fatalf("newBodyStore() error = %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	prepared, err := store.Prepare(
		ids.New(ids.KindAssignment),
		ids.NewULID(),
		[]byte("true\n"),
		uint32(os.Geteuid()),
		uint32(os.Getegid()),
	)
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	if err := os.Remove(prepared.hostPath); err != nil {
		t.Fatalf("Remove(body) error = %v", err)
	}
	if err := os.WriteFile(prepared.hostPath, []byte("false\n"), bodyMode); err != nil {
		t.Fatalf("WriteFile(replacement) error = %v", err)
	}
	if err := store.Remove(prepared); err == nil {
		t.Fatal("Remove(replacement) error = nil")
	}
	if _, err := os.Lstat(prepared.hostPath); err != nil {
		t.Fatalf("replacement body was removed: %v", err)
	}
}

func TestBodyStoreRejectsSymlinkedAssignment(t *testing.T) {
	root := t.TempDir()
	if err := os.Chmod(root, directoryMode); err != nil {
		t.Fatalf("Chmod(root) error = %v", err)
	}
	store, err := newBodyStore(root, uint32(os.Geteuid()), uint32(os.Getegid()))
	if err != nil {
		t.Fatalf("newBodyStore() error = %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	assignmentID := ids.New(ids.KindAssignment)
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, bodyStoreTasks, assignmentID)); err != nil {
		t.Fatalf("Symlink() error = %v", err)
	}
	if _, err := store.Prepare(
		assignmentID,
		ids.NewULID(),
		[]byte("true\n"),
		uint32(os.Geteuid()),
		uint32(os.Getegid()),
	); err == nil {
		t.Fatal("Prepare(symlink) error = nil")
	}
	entries, err := os.ReadDir(outside)
	if err != nil || len(entries) != 0 {
		t.Fatalf("outside directory changed: entries = %v, error = %v", entries, err)
	}
}
