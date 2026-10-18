package agentworkjournal

import (
	"bytes"
	"context"
	"math"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/volumeexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// PrepareVolumeDirectoryBatch returns a pending batch unchanged for inspection,
// never permission to replay it. Only a saved result permits the next cursor.
func (journal *Journal) PrepareVolumeDirectoryBatch(ctx context.Context, attempt workexecution.Attempt, volumeID string, at time.Time) (*VolumeDirectoryRecord, bool, error) {
	if attempt.Validate() != nil {
		return nil, false, invalid()
	}
	unlock, err := journal.lock(ctx)
	if err != nil {
		return nil, false, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil {
		return nil, false, err
	}
	if !matchesVolume(state, attempt, agentpb.VolumeWorkAction_VOLUME_WORK_ACTION_REMOVE, volumeID) ||
		!state.Volume.Acknowledged || state.Volume.Completion != nil || state.Volume.Failure != nil || !state.Volume.Progress.InFlight ||
		state.Volume.Progress.Stage != volumeexecution.RemoveDirectory {
		return nil, false, conflict()
	}
	previous := state.Volume.Directory
	if previous != nil && (previous.Result == nil || previous.Result.Absent) {
		return previous, false, nil
	}
	checkpoint, err := state.Volume.Progress.Checkpoint(attempt, state.Volume.Action, volumeID)
	if err != nil {
		return nil, false, err
	}
	batch := volumeexecution.DirectoryBatch{AttemptID: attempt.ID, VolumeID: volumeID,
		Checkpoint: checkpoint.Evidence, Ordinal: 1, PreparedAt: at.UTC()}
	if previous != nil {
		if previous.Batch.Ordinal == math.MaxUint64 || at.Before(previous.Result.ObservedAt) {
			return nil, false, conflict()
		}
		batch.Ordinal = previous.Batch.Ordinal + 1
		batch.Cursor = slices.Clone(previous.Result.NextCursor)
	}
	state.Volume.Directory = &VolumeDirectoryRecord{Boundary: state.Volume.Progress, Batch: batch}
	if err := journal.writeRecord(ctx, *state); err != nil {
		return nil, false, err
	}
	return state.Volume.Directory, true, nil
}

func (journal *Journal) SaveVolumeDirectoryResult(ctx context.Context, attempt workexecution.Attempt, volumeID string, batch volumeexecution.DirectoryBatch, result volumeexecution.DirectoryResult) error {
	if attempt.Validate() != nil {
		return invalid()
	}
	unlock, err := journal.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil {
		return err
	}
	if !matchesVolume(state, attempt, agentpb.VolumeWorkAction_VOLUME_WORK_ACTION_REMOVE, volumeID) ||
		!state.Volume.Acknowledged || state.Volume.Completion != nil || !state.Volume.Progress.InFlight || state.Volume.Directory == nil {
		return conflict()
	}
	directory := state.Volume.Directory
	expected, err := volumeexecution.EncodeDirectoryBatch(attempt, directory.Boundary, directory.Batch)
	provided, inputErr := volumeexecution.EncodeDirectoryBatch(attempt, directory.Boundary, batch)
	if err != nil || inputErr != nil || !bytes.Equal(expected, provided) {
		return conflict()
	}
	raw, err := volumeexecution.EncodeDirectoryResult(attempt, directory.Boundary, batch, result)
	if err != nil {
		return err
	}
	if previous := directory.Result; previous != nil {
		saved, err := volumeexecution.EncodeDirectoryResult(attempt, directory.Boundary, batch, *previous)
		if err != nil || !bytes.Equal(raw, saved) {
			return conflict()
		}
		return nil
	}
	directory.Result = &result
	return journal.writeRecord(ctx, *state)
}
