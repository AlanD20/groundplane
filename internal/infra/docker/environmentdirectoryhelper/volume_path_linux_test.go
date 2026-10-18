//go:build linux

package environmentdirectoryhelper

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

type volumePathTestCreator struct{}

func (volumePathTestCreator) Create(ctx context.Context, root, directory string) error {
	return createDirectories(ctx, root, directory, uint32(os.Getuid()))
}

func (volumePathTestCreator) RemoveManagedVolume(
	ctx context.Context,
	request ManagedVolumeDirectoryRemoveRequest,
) (ManagedVolumeDirectoryRemoveResult, error) {
	return removeManagedVolume(ctx, request, uint32(os.Getuid()))
}

// Rationale: a real helper replay must recover when its saved cursor names a
// child deleted by a call whose response was lost, without following symlinks.
// VOL-07: filesystem cursor replay only; native checkpoint delivery is separate.
func TestVolumePathDeletionRecoversLostBatchResponse(t *testing.T) {
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "vol")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(
		root,
		"tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		"prj_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		testEnvironmentID,
	)
	creator := volumePathTestCreator{}
	if err := creator.Create(ctx, root, directory); err != nil {
		t.Fatal(err)
	}
	leaf := filepath.Join(directory, "app-data")
	if err := os.MkdirAll(filepath.Join(leaf, "a"), 0o700); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 130; index++ {
		for _, parent := range []string{leaf, filepath.Join(leaf, "a")} {
			if err := os.WriteFile(filepath.Join(parent, fmt.Sprintf("z-%03d", index)), []byte("data"), 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
	sentinel := filepath.Join(directory, "retained")
	if err := os.WriteFile(sentinel, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(sentinel, filepath.Join(leaf, "a", "escape")); err != nil {
		t.Fatal(err)
	}
	intent := sha256.Sum256([]byte("authorized removal"))
	request := ManagedVolumeDirectoryRemoveRequest{
		TaskID: testTaskID, OperationID: testOperationID, VolumeID: testVolumeID,
		VolumeRoot: root, VolumeDir: directory, ComposeKey: "app-data", IntentSHA256: intent[:],
	}
	first, err := creator.RemoveManagedVolume(ctx, request)
	if err != nil || first.Complete || first.MutationCount != 128 {
		t.Fatalf("first bounded call: %+v %v", first, err)
	}
	request.Cursor = first.NextCursor
	lost, err := creator.RemoveManagedVolume(ctx, request)
	if err != nil || lost.Complete || lost.MutationCount > 128 {
		t.Fatalf("lost partial call: %+v %v", lost, err)
	}
	if _, err := os.Stat(filepath.Join(leaf, "a")); !os.IsNotExist(err) {
		t.Fatalf("pending child was not removed: %v", err)
	}
	recovered, err := creator.RemoveManagedVolume(ctx, request)
	if err != nil || !recovered.Complete || recovered.MutationCount > 128 {
		t.Fatalf("exact pending call replay: %+v %v", recovered, err)
	}
	if _, err := os.Stat(leaf); !os.IsNotExist(err) {
		t.Fatalf("Volume leaf remains: %v", err)
	}
	absent, err := creator.RemoveManagedVolume(ctx, request)
	if err != nil || !absent.Complete || absent.MutationCount != 0 {
		t.Fatalf("already-absent pending leaf replay: %+v %v", absent, err)
	}
	retained, err := os.ReadFile(sentinel)
	if err != nil || string(retained) != "keep" {
		t.Fatalf("symlink target changed: %v", err)
	}
}
