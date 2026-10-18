package agentworkjournal

import (
	"github.com/AlanD20/groundplane/internal/common/lifecycleexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

// LifecycleRecord retains only checkpoint metadata. The full captured runtime
// and private hook inputs are never written to this delivery journal.
type LifecycleRecord struct {
	Stages       []lifecycleexecution.Stage  `json:"stages"`
	Progress     lifecycleexecution.Progress `json:"progress"`
	Acknowledged bool                        `json:"acknowledged"`
	Completion   *workexecution.Attempt      `json:"completion,omitempty"`
	Failure      *LifecycleFailureDelivery   `json:"failure,omitempty"`
	Hook         *LifecycleHookRecord        `json:"hook,omitempty"`
}

type LifecycleFailureDelivery struct {
	Reason   lifecycleexecution.FailureReason `json:"reason"`
	Evidence []byte                           `json:"evidence,omitempty"`
	Receipt  *workexecution.Attempt           `json:"receipt,omitempty"`
}

func (record attemptRecord) validateLifecycle() error {
	if record.Lifecycle == nil || record.AttachProvisionRecovery != nil || record.AttachProvisionFailure != nil || record.AttachProvision != nil ||
		record.EntryRecovery != nil || record.EntryConfiguration != nil || record.ComponentRecovery != nil || record.Component != nil ||
		record.NetworkRecovery != nil || record.Network != nil || record.VolumeRecovery != nil || record.Volume != nil ||
		record.AttachRecovery != nil || record.AttachFailure != nil || record.AttachRemoval != nil || record.EnvironmentFailure != nil ||
		record.EnvironmentRemoval != nil || record.ScriptFailure != nil || record.ScriptRecovery != nil || len(record.Script) != 0 ||
		record.Release != nil || record.Effect != nil || record.Completion != nil || record.Failure != nil || record.Recovery != nil || record.ScriptCompletion != nil {
		return invalid()
	}
	lifecycle := record.Lifecycle
	if record.Attempt.Executor.Executor != workexecution.Agent || record.Attempt.FinishedAt != nil ||
		lifecycle.Progress.Sequence != record.Sequence ||
		!lifecycle.Progress.UpdatedAt.Equal(record.UpdatedAt) || lifecycle.Progress.ValidateStages(record.Attempt, lifecycle.Stages) != nil {
		return invalid()
	}
	if lifecycle.Progress.Sequence == 0 && (lifecycle.Failure == nil || lifecycle.Acknowledged || lifecycle.Completion != nil) {
		return invalid()
	}
	if lifecycle.Hook != nil && lifecycle.Hook.validate(record.Attempt, lifecycle.Stages, lifecycle.Progress) != nil {
		return invalid()
	}
	if failure := lifecycle.Failure; failure != nil {
		if !failure.Reason.Valid() || lifecycle.Completion != nil {
			return invalid()
		}
		if len(lifecycle.Progress.Completed) == len(lifecycle.Stages) {
			if !lifecycle.Acknowledged || lifecycle.Progress.BeganAt != nil || failure.Receipt == nil ||
				failure.Reason != lifecycleexecution.ExecutionInterrupted ||
				failure.Receipt.Outcome != workexecution.Cancelled && failure.Receipt.Outcome != workexecution.Superseded {
				return invalid()
			}
		}
		if terminal := failure.Receipt; terminal != nil {
			if terminal.Validate() != nil || terminal.FinishedAt == nil || terminal.Result == nil || terminal.Outcome == workexecution.Succeeded ||
				!workexecution.SameAssignment(record.Attempt, *terminal) || terminal.StartedAt == nil ||
				!terminal.StartedAt.Equal(*record.Attempt.StartedAt) || terminal.FinishedAt.Before(record.UpdatedAt) {
				return invalid()
			}
			captured, err := lifecycleexecution.DecodeFailureStages(*terminal, lifecycle.Stages, failure.Evidence, *terminal.Result)
			if err != nil || captured.Reason != failure.Reason || !captured.RecordedAt.Equal(*terminal.FinishedAt) {
				return invalid()
			}
			if len(lifecycle.Progress.Completed) == len(lifecycle.Stages) {
				checkpoint, err := lifecycle.Progress.CheckpointStages(record.Attempt, lifecycle.Stages)
				if err != nil || captured.Checkpoint == nil || *captured.Checkpoint != checkpoint {
					return invalid()
				}
			}
		} else if len(failure.Evidence) != 0 {
			return invalid()
		}
	}
	if terminal := lifecycle.Completion; terminal != nil {
		checkpoint, err := lifecycle.Progress.CheckpointStages(record.Attempt, lifecycle.Stages)
		if err != nil || !lifecycle.Acknowledged || lifecycle.Progress.BeganAt != nil || len(lifecycle.Progress.Completed) != len(lifecycle.Stages) ||
			terminal.Validate() != nil || !workexecution.SameAssignment(record.Attempt, *terminal) || terminal.StartedAt == nil ||
			!terminal.StartedAt.Equal(*record.Attempt.StartedAt) || terminal.FinishedAt == nil || terminal.FinishedAt.Before(record.UpdatedAt) ||
			(terminal.Outcome != workexecution.Succeeded && terminal.Outcome != workexecution.Cancelled && terminal.Outcome != workexecution.Superseded) || terminal.Result == nil || *terminal.Result != checkpoint.Evidence {
			return invalid()
		}
	}
	return nil
}
