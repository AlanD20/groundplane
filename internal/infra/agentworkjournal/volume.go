package agentworkjournal

import (
	"github.com/AlanD20/groundplane/internal/common/volumeexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// VolumeRecord retains a bounded checkpoint, never the captured Compose files.
// The attempt's input digest binds those files to this operation and Volume.
type VolumeRecord struct {
	Action       agentpb.VolumeWorkAction `json:"action"`
	VolumeID     string                   `json:"volume_id"`
	Progress     volumeexecution.Progress `json:"progress"`
	Acknowledged bool                     `json:"acknowledged"`
	Completion   *workexecution.Attempt   `json:"completion,omitempty"`
	Directory    *VolumeDirectoryRecord   `json:"directory,omitempty"`
	Failure      *VolumeFailureDelivery   `json:"failure,omitempty"`
}

type VolumeFailureDelivery struct {
	Reason   volumeexecution.FailureReason `json:"reason"`
	Evidence []byte                        `json:"evidence,omitempty"`
	Receipt  *workexecution.Attempt        `json:"receipt,omitempty"`
}

type VolumeDirectoryRecord struct {
	Boundary volumeexecution.Progress         `json:"boundary"`
	Batch    volumeexecution.DirectoryBatch   `json:"batch"`
	Result   *volumeexecution.DirectoryResult `json:"result,omitempty"`
}

func (record attemptRecord) validateVolume() error {
	if record.Volume == nil || record.VolumeRecovery != nil || record.AttachRecovery != nil || record.AttachFailure != nil || record.AttachRemoval != nil ||
		record.EnvironmentFailure != nil || record.EnvironmentRemoval != nil || record.ScriptFailure != nil ||
		record.ScriptRecovery != nil || record.ScriptCompletion != nil || record.Release != nil || record.Effect != nil ||
		record.Completion != nil || record.Failure != nil || record.Recovery != nil || len(record.Script) != 0 {
		return invalid()
	}
	volume := record.Volume
	if volume.Progress.Sequence != record.Sequence || !volume.Progress.UpdatedAt.Equal(record.UpdatedAt) {
		return invalid()
	}
	if err := volume.Progress.Validate(record.Attempt, volume.Action, volume.VolumeID); err != nil {
		return err
	}
	if volume.Progress.Sequence == 0 && (volume.Failure == nil || volume.Acknowledged || volume.Directory != nil || volume.Completion != nil) {
		return invalid()
	}
	if failure := volume.Failure; failure != nil {
		if !failure.Reason.Valid() || volume.Completion != nil || volume.Progress.Stage == volumeexecution.Complete {
			return invalid()
		}
		if terminal := failure.Receipt; terminal != nil {
			if terminal.Validate() != nil || terminal.FinishedAt == nil || terminal.Result == nil || terminal.Outcome == workexecution.Succeeded ||
				!workexecution.SameAssignment(record.Attempt, *terminal) || terminal.StartedAt == nil ||
				!terminal.StartedAt.Equal(*record.Attempt.StartedAt) || terminal.FinishedAt.Before(record.UpdatedAt) {
				return invalid()
			}
			captured, err := volumeexecution.DecodeFailure(*terminal, failure.Evidence, *terminal.Result)
			if err != nil || captured.Reason != failure.Reason || !captured.RecordedAt.Equal(*terminal.FinishedAt) {
				return invalid()
			}
		} else if len(failure.Evidence) != 0 {
			return invalid()
		}
	}
	if directory := volume.Directory; directory != nil {
		if volume.Action != agentpb.VolumeWorkAction_VOLUME_WORK_ACTION_REMOVE || directory.Batch.VolumeID != volume.VolumeID ||
			directory.Batch.Validate(record.Attempt, directory.Boundary) != nil || directory.Boundary.Sequence > volume.Progress.Sequence ||
			directory.Boundary.Sequence == volume.Progress.Sequence && !volumeexecution.EqualProgress(directory.Boundary, volume.Progress) {
			return invalid()
		}
		if directory.Result != nil && directory.Result.Validate(record.Attempt, directory.Boundary, directory.Batch) != nil {
			return invalid()
		}
	}
	if terminal := volume.Completion; terminal != nil {
		checkpoint, err := volume.Progress.Checkpoint(record.Attempt, volume.Action, volume.VolumeID)
		if err != nil {
			return err
		}
		if !volume.Acknowledged || volume.Progress.Stage != volumeexecution.Complete || terminal.Validate() != nil ||
			!workexecution.SameAssignment(record.Attempt, *terminal) || terminal.StartedAt == nil ||
			!terminal.StartedAt.Equal(*record.Attempt.StartedAt) || terminal.FinishedAt == nil ||
			terminal.FinishedAt.Before(record.UpdatedAt) || terminal.Outcome != workexecution.Succeeded ||
			terminal.Result == nil || *terminal.Result != checkpoint.Evidence {
			return invalid()
		}
	}
	return nil
}
