//go:build linux

package environmentdirectoryhelper

import (
	"context"
	"encoding/json"
	"fmt"
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

// Rationale: managed volume binds need durable private direct children without
// weakening the Environment namespace or following a hostile leaf.
func TestEnsureManagedVolumeDirectoriesCreatesPrivateDirectLeaves(t *testing.T) {
	root := filepath.Join(t.TempDir(), "vol")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatalf("Mkdir(root) error = %v", err)
	}
	directory := root + "/tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV/prj_01ARZ3NDEKTSV4RRFFQ69G5FAV/" +
		"env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	if err := createDirectories(context.Background(), root, directory, uint32(os.Getuid())); err != nil {
		t.Fatalf("createDirectories() error = %v", err)
	}
	if err := ensureManagedVolumeDirectoriesAs(context.Background(), ManagedVolumeEnsureRequest{
		TaskID: "task_01ARZ3NDEKTSV4RRFFQ69G5FAV", OperationID: "op_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		IntentSHA256: make([]byte, 32), VolumeRoot: root, VolumeDir: directory,
		Volumes: []ManagedVolume{{ID: "vol_01ARZ3NDEKTSV4RRFFQ69G5FAV", Key: "app-data"},
			{ID: "vol_01ARZ3NDEKTSV4RRFFQ69G5FAW", Key: "cache"}},
	}, uint32(os.Getuid())); err != nil {
		t.Fatalf("ensureManagedVolumeDirectories() error = %v", err)
	}
	for _, name := range []string{"app-data", "cache"} {
		info, err := os.Stat(filepath.Join(directory, name))
		if err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 {
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
	if err := ensureManagedVolumeDirectoriesAs(context.Background(), ManagedVolumeEnsureRequest{
		TaskID: "task_01ARZ3NDEKTSV4RRFFQ69G5FAV", OperationID: "op_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		IntentSHA256: make([]byte, 32), VolumeRoot: root, VolumeDir: directory,
		Volumes: []ManagedVolume{{ID: "vol_01ARZ3NDEKTSV4RRFFQ69G5FAW", Key: "hostile"}},
	}, uint32(os.Getuid())); err == nil {
		t.Fatal("ensureManagedVolumeDirectories() accepted symlink leaf")
	}
}

// Rationale: workloads may change the root mode and ownership of their bind
// directory; the private descriptor-validated Environment parent owns identity.
func TestEnsureManagedVolumeDirectoriesAcceptsFinalizedWorkloadOwnedLeaf(t *testing.T) {
	root := filepath.Join(t.TempDir(), "vol")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatalf("Mkdir(root) error = %v", err)
	}
	directory := root + "/tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV/prj_01ARZ3NDEKTSV4RRFFQ69G5FAV/env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	if err := createDirectories(context.Background(), root, directory, uint32(os.Getuid())); err != nil {
		t.Fatalf("createDirectories() error = %v", err)
	}
	if err := os.Mkdir(filepath.Join(directory, "app-data"), 0o755); err != nil {
		t.Fatalf("Mkdir(leaf) error = %v", err)
	}
	err := ensureManagedVolumeDirectoriesAs(context.Background(), ManagedVolumeEnsureRequest{
		TaskID: "task_01ARZ3NDEKTSV4RRFFQ69G5FAV", OperationID: "op_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		IntentSHA256: make([]byte, 32), VolumeRoot: root, VolumeDir: directory,
		Volumes: []ManagedVolume{{ID: "vol_01ARZ3NDEKTSV4RRFFQ69G5FAV", Key: "app-data"}},
	}, uint32(os.Getuid()))
	if err != nil {
		t.Fatalf("ensureManagedVolumeDirectories() error = %v", err)
	}
	if err := os.Chmod(filepath.Join(directory, "app-data"), 0o000); err != nil {
		t.Fatalf("Chmod(leaf) error = %v", err)
	}
	defer os.Chmod(filepath.Join(directory, "app-data"), 0o700)
	err = ensureManagedVolumeDirectoriesAs(context.Background(), ManagedVolumeEnsureRequest{
		TaskID: "task_01ARZ3NDEKTSV4RRFFQ69G5FAV", OperationID: "op_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		IntentSHA256: make([]byte, 32), VolumeRoot: root, VolumeDir: directory,
		Volumes: []ManagedVolume{{ID: "vol_01ARZ3NDEKTSV4RRFFQ69G5FAV", Key: "app-data"}},
	}, uint32(os.Getuid()))
	if err != nil {
		t.Fatalf("ensureManagedVolumeDirectories(after workload chmod) error = %v", err)
	}
}

func TestEnsureManagedVolumeDirectoriesResumesOwnedPrivateSibling(t *testing.T) {
	root := filepath.Join(t.TempDir(), "vol")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatalf("Mkdir(root) error = %v", err)
	}
	directory := root + "/tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV/prj_01ARZ3NDEKTSV4RRFFQ69G5FAV/env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	if err := createDirectories(context.Background(), root, directory, uint32(os.Getuid())); err != nil {
		t.Fatalf("createDirectories() error = %v", err)
	}
	request := ManagedVolumeEnsureRequest{
		TaskID: "task_01ARZ3NDEKTSV4RRFFQ69G5FAV", OperationID: "op_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		IntentSHA256: make([]byte, 32), VolumeRoot: root, VolumeDir: directory,
		Volumes: []ManagedVolume{{ID: "vol_01ARZ3NDEKTSV4RRFFQ69G5FAV", Key: "app-data"}},
	}
	marker, err := json.Marshal(volumeCreationEvidence{
		Schema: 1, VolumeID: request.Volumes[0].ID, OperationID: request.OperationID,
		TaskID: request.TaskID, ComposeKey: request.Volumes[0].Key,
		Intent: "0000000000000000000000000000000000000000000000000000000000000000",
	})
	if err != nil {
		t.Fatalf("Marshal(marker) error = %v", err)
	}
	if err := os.Mkdir(filepath.Join(directory, ".gp-volume-create-"+request.Volumes[0].ID+"-"+request.OperationID), 0o700); err != nil {
		t.Fatalf("Mkdir(sibling) error = %v", err)
	}
	sibling := filepath.Join(directory, ".gp-volume-create-"+request.Volumes[0].ID+"-"+request.OperationID)
	if err := os.WriteFile(filepath.Join(sibling, volumeCreationMarker), marker, 0o600); err != nil {
		t.Fatalf("WriteFile(marker) error = %v", err)
	}
	if err := ensureManagedVolumeDirectoriesAs(context.Background(), request, uint32(os.Getuid())); err != nil {
		t.Fatalf("ensureManagedVolumeDirectories() error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(directory, "app-data", volumeCreationMarker)); !os.IsNotExist(err) {
		t.Fatalf("published marker = %v, want absent", err)
	}
}

func TestRemoveManagedVolumeIsBoundedAndResumable(t *testing.T) {
	root := filepath.Join(t.TempDir(), "vol")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatalf("Mkdir(root) error = %v", err)
	}
	directory := root + "/tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV/prj_01ARZ3NDEKTSV4RRFFQ69G5FAV/env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	if err := createDirectories(context.Background(), root, directory, uint32(os.Getuid())); err != nil {
		t.Fatalf("createDirectories() error = %v", err)
	}
	leaf := filepath.Join(directory, "app-data")
	if err := os.Mkdir(leaf, 0o700); err != nil {
		t.Fatalf("Mkdir(leaf) error = %v", err)
	}
	for index := 0; index < 200; index++ {
		if err := os.WriteFile(filepath.Join(leaf, fmt.Sprintf("entry-%03d", index)), []byte("x"), 0o600); err != nil {
			t.Fatalf("WriteFile(entry-%03d) error = %v", index, err)
		}
	}
	request := ManagedVolumeDirectoryRemoveRequest{
		TaskID: "task_01ARZ3NDEKTSV4RRFFQ69G5FAV", OperationID: "op_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		IntentSHA256: make([]byte, 32), VolumeRoot: root, VolumeDir: directory,
		VolumeID: "vol_01ARZ3NDEKTSV4RRFFQ69G5FAV", ComposeKey: "app-data",
	}
	result, err := removeManagedVolume(context.Background(), request, uint32(os.Getuid()))
	if err != nil || result.Complete || result.MutationCount != 128 || len(result.NextCursor) == 0 {
		t.Fatalf("first removal = %#v, %v", result, err)
	}
	request.Cursor = result.NextCursor
	for !result.Complete {
		result, err = removeManagedVolume(context.Background(), request, uint32(os.Getuid()))
		if err != nil {
			t.Fatalf("resumed removal = %#v, %v", result, err)
		}
		request.Cursor = result.NextCursor
	}
	if _, err := os.Stat(leaf); !os.IsNotExist(err) {
		t.Fatalf("leaf = %v, want absent", err)
	}
}
