package agentworkjournal

import (
	"slices"

	"github.com/AlanD20/groundplane/internal/common/entryexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

type EntryConfigurationRecord struct {
	Layout       entryexecution.Layout              `json:"layout"`
	Progress     entryexecution.Progress            `json:"progress"`
	Acknowledged bool                               `json:"acknowledged"`
	Completion   *workexecution.Attempt             `json:"completion,omitempty"`
	Failure      *EntryConfigurationFailureDelivery `json:"failure,omitempty"`
}

type EntryConfigurationFailureDelivery struct {
	Reason   entryexecution.FailureReason `json:"reason"`
	Evidence []byte                       `json:"evidence,omitempty"`
	Receipt  *workexecution.Attempt       `json:"receipt,omitempty"`
}

func (record attemptRecord) validateEntryConfiguration() error {
	if record.EntryConfiguration == nil || record.Component != nil || record.ComponentRecovery != nil ||
		record.Network != nil || record.NetworkRecovery != nil || record.Volume != nil || record.VolumeRecovery != nil ||
		record.AttachRecovery != nil || record.AttachFailure != nil || record.AttachRemoval != nil ||
		record.EnvironmentFailure != nil || record.EnvironmentRemoval != nil || record.ScriptFailure != nil ||
		record.ScriptRecovery != nil || record.ScriptCompletion != nil || record.Release != nil || record.Effect != nil ||
		record.Completion != nil || record.Failure != nil || record.Recovery != nil || len(record.Script) != 0 ||
		record.Attempt.FinishedAt != nil {
		return invalid()
	}
	entry := record.EntryConfiguration
	if entry.Progress.Sequence != record.Sequence ||
		!entry.Progress.UpdatedAt.Equal(record.UpdatedAt) ||
		entry.Progress.ValidateLayout(record.Attempt, entry.Layout) != nil {
		return invalid()
	}
	if entry.Progress.Sequence == 0 && (entry.Failure == nil || entry.Acknowledged || entry.Completion != nil) {
		return invalid()
	}
	if failure := entry.Failure; failure != nil {
		if !failure.Reason.Valid() || entry.Completion != nil || entry.Progress.Stage == entryexecution.Complete {
			return invalid()
		}
		if terminal := failure.Receipt; terminal != nil {
			if terminal.Validate() != nil || terminal.FinishedAt == nil || terminal.Result == nil ||
				terminal.Outcome == workexecution.Succeeded || !workexecution.SameAssignment(record.Attempt, *terminal) ||
				terminal.StartedAt == nil || !terminal.StartedAt.Equal(*record.Attempt.StartedAt) ||
				terminal.FinishedAt.Before(record.UpdatedAt) {
				return invalid()
			}
			captured, err := entryexecution.DecodeLayoutFailure(*terminal, entry.Layout, failure.Evidence, *terminal.Result)
			if err != nil || captured.Reason != failure.Reason || !captured.RecordedAt.Equal(*terminal.FinishedAt) {
				return invalid()
			}
		} else if len(failure.Evidence) != 0 {
			return invalid()
		}
	}
	if terminal := entry.Completion; terminal != nil {
		checkpoint, err := entry.Progress.LayoutCheckpoint(record.Attempt, entry.Layout)
		if err != nil || !entry.Acknowledged || entry.Progress.Stage != entryexecution.Complete ||
			terminal.Validate() != nil || !workexecution.SameAssignment(record.Attempt, *terminal) ||
			terminal.StartedAt == nil || !terminal.StartedAt.Equal(*record.Attempt.StartedAt) ||
			terminal.FinishedAt == nil || terminal.FinishedAt.Before(record.UpdatedAt) ||
			(terminal.Outcome != workexecution.Succeeded && terminal.Outcome != workexecution.Cancelled && terminal.Outcome != workexecution.Superseded) ||
			terminal.Result == nil || *terminal.Result != checkpoint.Evidence {
			return invalid()
		}
	}
	return nil
}

func sameEntryLayout(left, right entryexecution.Layout) bool {
	return left.EnvironmentID == right.EnvironmentID && left.Input == right.Input && left.Removal == right.Removal &&
		left.FileCount == right.FileCount && slices.Equal(left.Consumers, right.Consumers)
}
