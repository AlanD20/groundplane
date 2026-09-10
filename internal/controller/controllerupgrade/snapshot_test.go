package controllerupgrade

import (
	"context"
	"encoding/json"
	"testing"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: a safe staged candidate does not prove the installed predecessor
// is immutable and is the actual running binary required for recovery.
func TestControllerUpdateSnapshotRefusesChangedInstalledPredecessor(t *testing.T) {
	h := newServiceHarness(t)
	h.catalog.installed = upgrade.Hash([]byte("changed installed predecessor"))
	snapshot, err := h.service.ControllerUpdateSnapshot(context.Background())
	if err != nil || snapshot.Available || snapshot.Error != "Installed Controller recovery identity is unavailable." {
		t.Fatalf("unsafe predecessor snapshot = %#v, %v", snapshot, err)
	}
}

// Rationale: queued Tasks are visible before any activation file exists. Failed
// metadata reads must not destroy the independent Host health response or turn
// unknown history into a fabricated successful update.
func TestControllerUpdateSnapshotUsesTaskAndHonestUnavailableState(t *testing.T) {
	h := newServiceHarness(t)
	ctx := context.Background()
	response, err := h.service.UpdateController(ctx, string(h.input.Release), "snapshot-native-key-0001")
	if err != nil {
		t.Fatal(err)
	}
	var accepted apiTypes.TaskAccepted
	if err := json.Unmarshal(response.Body, &accepted); err != nil {
		t.Fatal(err)
	}
	snapshot, err := h.service.ControllerUpdateSnapshot(ctx)
	if err != nil || !snapshot.Available || snapshot.RunningSHA256 != string(h.input.PreviousController) ||
		snapshot.LastUpdate == nil || snapshot.LastUpdate.TaskID != accepted.TaskID || snapshot.LastUpdate.Status != "pending" ||
		snapshot.LastUpdate.Phase != "" {
		t.Fatalf("queued snapshot = %#v, %v", snapshot, err)
	}
	h.storage.rangeError = errs.New(errs.KindStorageUnavailable, "private storage detail")
	snapshot, err = h.service.ControllerUpdateSnapshot(ctx)
	if err != nil || snapshot.Available || snapshot.Error != "Controller update history is unavailable." ||
		snapshot.LastUpdate != nil {
		t.Fatalf("unavailable history = %#v, %v", snapshot, err)
	}
}

// Rationale: damaged staging must disable new updates without hiding an
// independently valid accepted Task that the Console needs after reconnecting.
func TestControllerUpdateSnapshotRetainsHistoryWhenReleaseStateFails(t *testing.T) {
	h := newServiceHarness(t)
	ctx := context.Background()
	if _, err := h.service.UpdateController(ctx, string(h.input.Release), "snapshot-native-key-0002"); err != nil {
		t.Fatal(err)
	}
	h.catalog.failure = errs.New(errs.KindInternal, "private release detail")
	snapshot, err := h.service.ControllerUpdateSnapshot(ctx)
	if err != nil || snapshot.Available || snapshot.Error != "Native release state is unavailable or invalid." ||
		snapshot.LastUpdate == nil || snapshot.LastUpdate.Status != "pending" {
		t.Fatalf("invalid release state lost durable history: %#v, %v", snapshot, err)
	}
}
