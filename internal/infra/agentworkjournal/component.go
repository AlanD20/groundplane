package agentworkjournal

import (
	"github.com/AlanD20/groundplane/internal/common/componentexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

type ComponentRecord struct {
	Layout       componentexecution.Layout   `json:"layout"`
	Progress     componentexecution.Progress `json:"progress"`
	Acknowledged bool                        `json:"acknowledged"`
	Completion   *workexecution.Attempt      `json:"completion,omitempty"`
	Failure      *ComponentFailureDelivery   `json:"failure,omitempty"`
}

type ComponentFailureDelivery struct {
	Reason   componentexecution.FailureReason `json:"reason"`
	Evidence []byte                           `json:"evidence,omitempty"`
	Receipt  *workexecution.Attempt           `json:"receipt,omitempty"`
}

func (record attemptRecord) validateComponent() error {
	if record.Component == nil || record.ComponentRecovery != nil || record.Network != nil || record.NetworkRecovery != nil ||
		record.Volume != nil || record.VolumeRecovery != nil || record.AttachRecovery != nil ||
		record.AttachFailure != nil || record.AttachRemoval != nil || record.EnvironmentFailure != nil ||
		record.EnvironmentRemoval != nil || record.ScriptFailure != nil || record.ScriptRecovery != nil ||
		record.ScriptCompletion != nil || record.Release != nil || record.Effect != nil ||
		record.Completion != nil || record.Failure != nil || record.Recovery != nil || len(record.Script) != 0 ||
		record.Attempt.Executor.Executor != workexecution.Agent || record.Attempt.FinishedAt != nil {
		return invalid()
	}
	component := record.Component
	if component.Progress.Sequence != record.Sequence ||
		!component.Progress.UpdatedAt.Equal(record.UpdatedAt) ||
		component.Progress.ValidateLayout(record.Attempt, component.Layout) != nil {
		return invalid()
	}
	if component.Progress.Sequence == 0 && (component.Failure == nil || component.Acknowledged || component.Completion != nil) {
		return invalid()
	}
	if failure := component.Failure; failure != nil {
		if !failure.Reason.Valid() || component.Completion != nil || component.Progress.Stage == componentexecution.Complete {
			return invalid()
		}
		if terminal := failure.Receipt; terminal != nil {
			if terminal.Validate() != nil || terminal.FinishedAt == nil || terminal.Result == nil ||
				terminal.Outcome == workexecution.Succeeded || !workexecution.SameAssignment(record.Attempt, *terminal) ||
				terminal.StartedAt == nil || !terminal.StartedAt.Equal(*record.Attempt.StartedAt) || terminal.FinishedAt.Before(record.UpdatedAt) {
				return invalid()
			}
			captured, err := componentexecution.DecodeLayoutFailure(*terminal, component.Layout, failure.Evidence, *terminal.Result)
			if err != nil || captured.Reason != failure.Reason || !captured.RecordedAt.Equal(*terminal.FinishedAt) {
				return invalid()
			}
		} else if len(failure.Evidence) != 0 {
			return invalid()
		}
	}
	if terminal := component.Completion; terminal != nil {
		checkpoint, err := component.Progress.LayoutCheckpoint(record.Attempt, component.Layout)
		if err != nil || !component.Acknowledged || component.Progress.Stage != componentexecution.Complete ||
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
