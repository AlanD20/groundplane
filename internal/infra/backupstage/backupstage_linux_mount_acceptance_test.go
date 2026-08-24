//go:build linux && backupstage_mount_acceptance

package backupstage

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

// Rationale: release acceptance must prove a configured bind root is allowed
// while an introduced descendant mount is rejected by RESOLVE_NO_XDEV.
func TestConfiguredRootAllowsBindMountAndDescendantsRejectMounts(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Fatal("backupstage_mount_acceptance requires root in a mount-capable namespace")
	}
	source := t.TempDir()
	normalizeRootTestFixture(t, source)
	target := filepath.Join(t.TempDir(), "stage")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatalf("create root mountpoint: %v", err)
	}
	if err := unix.Mount(source, target, "", unix.MS_BIND, ""); err != nil {
		t.Fatalf("bind configured root (run in a mount-capable namespace): %v", err)
	}
	t.Cleanup(func() {
		if err := unix.Unmount(target, 0); err != nil {
			t.Errorf("unmount configured root: %v", err)
		}
	})
	stager := newRootTestStager(t, target)
	descendant := filepath.Join(target, "task")
	if err := os.Mkdir(descendant, 0o700); err != nil {
		t.Fatalf("create descendant mountpoint: %v", err)
	}
	other := t.TempDir()
	normalizeRootTestFixture(t, other)
	if err := unix.Mount(other, descendant, "", unix.MS_BIND, ""); err != nil {
		t.Fatalf("bind descendant: %v", err)
	}
	t.Cleanup(func() {
		if err := unix.Unmount(descendant, 0); err != nil {
			t.Errorf("unmount descendant: %v", err)
		}
	})
	if _, err := stager.Prepare(
		context.Background(), IDs{Task: "task", Step: "step", Point: "point"}, Bounded(
			1024)); err == nil {
		t.Fatal("Prepare() crossed a descendant mount")
	}
}
