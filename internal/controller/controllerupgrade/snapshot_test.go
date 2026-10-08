package controllerupgrade

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: advancing the storage epoch must not hide completed upgrade history
// or disable compatible new upgrades, but the old input must remain unexecutable.
func TestControllerUpdateSnapshotReadsPreviousEpochWithoutAuthorizingExecution(t *testing.T) {
	h := newServiceHarness(t)
	ctx := context.Background()
	if _, err := h.service.UpdateController(ctx, string(h.input.Release), "snapshot-recorded-epoch-0001"); err != nil {
		t.Fatal(err)
	}
	for key, entry := range h.storage.entries {
		task, err := etcd.DecodeTaskRecord(entry.Value)
		if err != nil {
			continue
		}
		input := h.input
		input.Manifest.StorageEpoch--
		manifest, _ := json.Marshal(input.Manifest)
		canonicalManifest, err := jcs.Canonicalize(manifest)
		if err != nil {
			t.Fatal(err)
		}
		input.Release = upgrade.Hash(canonicalManifest)
		raw, _ := json.Marshal(input)
		canonicalInput, err := jcs.Canonicalize(raw)
		if err != nil {
			t.Fatal(err)
		}
		task.Params[InputParam] = string(canonicalInput)
		task.PlanHash = string(upgrade.Hash(canonicalInput))[7:]
		task, err = etcd.TransitionTaskStatus(
			task,
			taskjournal.TaskStatusPending,
			taskjournal.TaskStatusRunning,
			h.now.Add(time.Second),
		)
		if err != nil {
			t.Fatal(err)
		}
		task, err = etcd.TransitionTaskStatus(
			task,
			taskjournal.TaskStatusRunning,
			taskjournal.TaskStatusCompleted,
			h.now.Add(2*time.Second),
		)
		if err != nil {
			t.Fatal(err)
		}
		entry.Value, err = etcd.EncodeTaskRecord(task)
		if err != nil {
			t.Fatal(err)
		}
		h.storage.entries[key] = entry
		if _, err := DecodeTask(task, task.CreatedAt, task.CreatedAt.Add(upgrade.TaskTimeoutSeconds*time.Second)); err == nil {
			t.Fatal("previous epoch was authorized for execution")
		}
		snapshot, err := h.service.ControllerUpdateSnapshot(ctx)
		if err != nil || !snapshot.Available || snapshot.LastUpdate == nil ||
			snapshot.LastUpdate.Release != string(input.Release) || snapshot.LastUpdate.Status != "completed" {
			t.Fatalf("completed previous-epoch history = %#v, %v", snapshot, err)
		}
		return
	}
	t.Fatal("published Task was not found")
}

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
