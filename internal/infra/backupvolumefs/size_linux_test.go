//go:build linux

package backupvolumefs

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestApparentSizeCountsRegularFilesWithoutFollowingLinks(t *testing.T) {
	directory := t.TempDir()
	live := filepath.Join(directory, "data")
	if err := os.MkdirAll(filepath.Join(live, "nested"), 0o700); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(live, "nested", "data")
	if err := os.WriteFile(file, []byte("seven!!"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(file, filepath.Join(live, "hardlink")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(directory, filepath.Join(live, "outside")); err != nil {
		t.Fatal(err)
	}
	parent, err := unix.Open(directory, unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	fd, err := openDirectory(parent, "data")
	if err != nil {
		_ = unix.Close(parent)
		t.Fatal(err)
	}
	volume := &Volume{parentFD: parent, liveFD: fd, name: "data"}
	t.Cleanup(func() { _ = volume.Close() })
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		t.Fatal(err)
	}
	volume.dev, volume.ino = uint64(stat.Dev), stat.Ino
	for range 2 {
		if got, err := volume.ApparentSize(t.Context()); err != nil || got != 7 {
			t.Fatalf("apparent size = %d, %v; want 7", got, err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := volume.ApparentSize(ctx); err == nil {
		t.Fatal("canceled scan returned a measurement")
	}
	if err := os.Rename(live, live+"-old"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(live, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := volume.ApparentSize(t.Context()); err == nil {
		t.Fatal("replaced volume returned a measurement from the predecessor")
	}
}
