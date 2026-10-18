package app

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/volumeexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// TASK-57: directory batches are local durable effect intents. Reopening the
// journal must retain an ambiguous batch for inspection, not authorize replay;
// only saving its exact bounded result permits advancing the traversal cursor.
func TestNativeVolumeDirectoryJournalResumesExactBatch(t *testing.T) {
	f := newNativeVolumeFixture(t)
	create := f.start(t, f.created.TaskID)
	progress := f.progress(t, create)
	for progress.Stage != volumeexecution.Complete {
		progress = f.advance(t, create, progress)
	}
	f.complete(t, create, progress)
	view, _, _ := f.remove(t)
	attempt, volumeID := view.Attempt.Attempt, f.created.Volume.ID
	path := filepath.Join(t.TempDir(), "journal")
	journal, err := agentworkjournal.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := journal.Close(); err != nil {
			t.Error(err)
		}
	})
	progress = f.progress(t, view)
	for {
		progress, err = progress.Begin(
			attempt,
			agentpb.VolumeWorkAction_VOLUME_WORK_ACTION_REMOVE,
			volumeID,
			time.Now().UTC(),
		)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := journal.PrepareVolume(t.Context(), attempt, agentpb.VolumeWorkAction_VOLUME_WORK_ACTION_REMOVE, volumeID, progress); err != nil {
			t.Fatal(err)
		}
		if progress.Stage == volumeexecution.RemoveDirectory {
			if _, _, err := journal.PrepareVolumeDirectoryBatch(t.Context(), attempt, volumeID, time.Now().UTC()); err == nil {
				t.Fatal("directory deletion began before the Controller checkpoint acknowledgement")
			}
		}
		f.checkpoint(t, view, progress)
		checkpoint, err := progress.Checkpoint(attempt, agentpb.VolumeWorkAction_VOLUME_WORK_ACTION_REMOVE, volumeID)
		if err != nil {
			t.Fatal(err)
		}
		if err := journal.AcknowledgeVolume(t.Context(), attempt, agentpb.VolumeWorkAction_VOLUME_WORK_ACTION_REMOVE, volumeID, checkpoint); err != nil {
			t.Fatal(err)
		}
		if progress.Stage == volumeexecution.RemoveDirectory {
			break
		}
		progress = f.confirm(t, view, progress)
		if _, err := journal.PrepareVolume(t.Context(), attempt, agentpb.VolumeWorkAction_VOLUME_WORK_ACTION_REMOVE, volumeID, progress); err != nil {
			t.Fatal(err)
		}
		checkpoint, err = progress.Checkpoint(attempt, agentpb.VolumeWorkAction_VOLUME_WORK_ACTION_REMOVE, volumeID)
		if err != nil {
			t.Fatal(err)
		}
		if err := journal.AcknowledgeVolume(t.Context(), attempt, agentpb.VolumeWorkAction_VOLUME_WORK_ACTION_REMOVE, volumeID, checkpoint); err != nil {
			t.Fatal(err)
		}
	}
	first, fresh, err := journal.PrepareVolumeDirectoryBatch(t.Context(), attempt, volumeID, time.Now().UTC())
	if err != nil || !fresh || first.Batch.Ordinal != 1 || len(first.Batch.Cursor) != 0 {
		t.Fatalf("first bounded batch: %v", err)
	}
	expected, err := volumeexecution.EncodeDirectoryBatch(attempt, first.Boundary, first.Batch)
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	journal, err = agentworkjournal.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	resumed, fresh, err := journal.PrepareVolumeDirectoryBatch(t.Context(), attempt, volumeID, time.Now().UTC())
	if err != nil || fresh {
		t.Fatalf("ambiguous batch became new deletion permission: %v", err)
	}
	actual, err := volumeexecution.EncodeDirectoryBatch(attempt, resumed.Boundary, resumed.Batch)
	if err != nil || !bytes.Equal(expected, actual) {
		t.Fatalf("restart changed the pending traversal: %v", err)
	}
	digest := sha256.Sum256(expected)
	result := volumeexecution.DirectoryResult{
		RequestSHA256: hex.EncodeToString(digest[:]),
		NextCursor:    []byte("nested/cursor"),
		Mutations:     volumeexecution.DirectoryMutationBudget,
		ObservedAt:    time.Now().UTC(),
	}
	overBudget := result
	overBudget.Mutations++
	if err := journal.SaveVolumeDirectoryResult(t.Context(), attempt, volumeID, resumed.Batch, overBudget); err == nil {
		t.Fatal("oversized deletion result advanced the cursor")
	}
	if err := journal.SaveVolumeDirectoryResult(t.Context(), attempt, volumeID, resumed.Batch, result); err != nil {
		t.Fatal(err)
	}
	if err := journal.SaveVolumeDirectoryResult(t.Context(), attempt, volumeID, resumed.Batch, result); err != nil {
		t.Fatalf("exact result replay: %v", err)
	}
	changed := result
	changed.NextCursor = []byte("different/cursor")
	if err := journal.SaveVolumeDirectoryResult(t.Context(), attempt, volumeID, resumed.Batch, changed); err == nil {
		t.Fatal("changed result overwrote an acknowledged cursor")
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	journal, err = agentworkjournal.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	second, fresh, err := journal.PrepareVolumeDirectoryBatch(t.Context(), attempt, volumeID, time.Now().UTC())
	if err != nil || !fresh || second.Batch.Ordinal != 2 || !bytes.Equal(second.Batch.Cursor, result.NextCursor) {
		t.Fatalf("restart lost saved result/cursor: %v", err)
	}
	raw, err := volumeexecution.EncodeDirectoryBatch(attempt, second.Boundary, second.Batch)
	if err != nil {
		t.Fatal(err)
	}
	digest = sha256.Sum256(raw)
	final := volumeexecution.DirectoryResult{
		RequestSHA256: hex.EncodeToString(digest[:]),
		Mutations:     1,
		Absent:        true,
		ObservedAt:    time.Now().UTC(),
	}
	if err := journal.SaveVolumeDirectoryResult(t.Context(), attempt, volumeID, second.Batch, final); err != nil {
		t.Fatal(err)
	}
	last, fresh, err := journal.PrepareVolumeDirectoryBatch(t.Context(), attempt, volumeID, time.Now().UTC())
	if err != nil || fresh || last.Result == nil || !last.Result.Absent || last.Batch.Ordinal != 2 {
		t.Fatalf("proved absence started another traversal: %v", err)
	}
	// This is saved protocol evidence, not an actual filesystem deletion proof.
	progress = f.confirm(t, view, progress)
	f.complete(t, view, progress)
}
