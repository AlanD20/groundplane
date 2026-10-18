package agentworkjournal

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/volumeexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (journal *Journal) PrepareVolume(ctx context.Context, attempt workexecution.Attempt, action agentpb.VolumeWorkAction, volumeID string, next volumeexecution.Progress) (*VolumeRecord, error) {
	if next.Validate(attempt, action, volumeID) != nil || next.Sequence == 0 {
		return nil, invalid()
	}
	unlock, err := journal.lock(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil {
		return nil, err
	}
	var previous *volumeexecution.Progress
	if state == nil {
		attempts, err := journal.attempts(ctx)
		if err != nil {
			return nil, err
		}
		if _, reserved := attempts[attempt.ID]; !reserved && len(attempts) >= MaximumAttempts {
			return nil, conflict()
		}
		state = &attemptRecord{Attempt: attempt}
	} else {
		if !matchesVolume(state, attempt, action, volumeID) {
			return nil, conflict()
		}
		if state.Volume.Failure != nil || state.Volume.Completion != nil {
			return nil, conflict()
		}
		previous = &state.Volume.Progress
		if volumeexecution.EqualProgress(*previous, next) {
			return state.Volume, nil
		}
		if !state.Volume.Acknowledged {
			return nil, conflict()
		}
	}
	if err := volumeexecution.ValidateTransition(attempt, action, volumeID, previous, next); err != nil {
		return nil, err
	}
	var directory *VolumeDirectoryRecord
	if state.Volume != nil {
		directory = state.Volume.Directory
	}
	state.Volume = &VolumeRecord{Action: action, VolumeID: volumeID, Progress: next, Directory: directory}
	state.Sequence, state.UpdatedAt = next.Sequence, next.UpdatedAt
	if err := journal.writeRecord(ctx, *state); err != nil {
		return nil, err
	}
	return state.Volume, nil
}

func (journal *Journal) Volume(ctx context.Context, attempt workexecution.Attempt, action agentpb.VolumeWorkAction, volumeID string) (*VolumeRecord, error) {
	if attempt.Validate() != nil {
		return nil, invalid()
	}
	unlock, err := journal.lock(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil || state == nil {
		return nil, err
	}
	if !matchesVolume(state, attempt, action, volumeID) {
		return nil, conflict()
	}
	return state.Volume, nil
}

func (journal *Journal) AcknowledgeVolume(ctx context.Context, attempt workexecution.Attempt, action agentpb.VolumeWorkAction, volumeID string, checkpoint workexecution.Checkpoint) error {
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
	if !matchesVolume(state, attempt, action, volumeID) {
		return conflict()
	}
	expected, err := state.Volume.Progress.Checkpoint(attempt, action, volumeID)
	if err != nil || checkpoint.AttemptID != expected.AttemptID || checkpoint.Sequence != expected.Sequence ||
		checkpoint.StageID != expected.StageID || checkpoint.Evidence != expected.Evidence || !checkpoint.RecordedAt.Equal(expected.RecordedAt) {
		return conflict()
	}
	if state.Volume.Acknowledged {
		return nil
	}
	state.Volume.Acknowledged = true
	return journal.writeRecord(ctx, *state)
}

func matchesVolume(state *attemptRecord, attempt workexecution.Attempt, action agentpb.VolumeWorkAction, volumeID string) bool {
	return state != nil && state.Volume != nil && workexecution.SameAssignment(state.Attempt, attempt) &&
		state.Attempt.StartedAt != nil && attempt.StartedAt != nil && state.Attempt.StartedAt.Equal(*attempt.StartedAt) &&
		state.Volume.Action == action && state.Volume.VolumeID == volumeID
}

func (journal *Journal) AcknowledgeVolumeCompletion(ctx context.Context, attempt workexecution.Attempt, action agentpb.VolumeWorkAction, volumeID string, terminal workexecution.Attempt) error {
	if attempt.Validate() != nil || terminal.Validate() != nil || terminal.FinishedAt == nil || terminal.Result == nil {
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
	if !matchesVolume(state, attempt, action, volumeID) {
		return conflict()
	}
	if previous := state.Volume.Completion; previous != nil {
		if !workexecution.SameAssignment(*previous, terminal) || previous.Outcome != terminal.Outcome ||
			!previous.FinishedAt.Equal(*terminal.FinishedAt) || *previous.Result != *terminal.Result ||
			terminal.StartedAt == nil || !previous.StartedAt.Equal(*terminal.StartedAt) {
			return conflict()
		}
		return nil
	}
	state.Volume.Completion = &terminal
	return journal.writeRecord(ctx, *state)
}
