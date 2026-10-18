package agentworkjournal

import (
	"github.com/AlanD20/groundplane/internal/common/backingprovision"
	"github.com/AlanD20/groundplane/internal/common/backingruntimefact"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

// BackingRecord stores only compact progress, actual container evidence and
// replay receipts. Compose, file bytes and resolved hook values are excluded.
type BackingRecord struct {
	Layout       backingprovision.Layout         `json:"layout"`
	Progress     *backingprovision.Progress      `json:"progress,omitempty"`
	Acknowledged bool                            `json:"acknowledged"`
	Hook         *BackingHookRecord              `json:"hook,omitempty"`
	Runtime      *backingruntimefact.Observation `json:"runtime,omitempty"`
	Completion   *workexecution.Attempt          `json:"completion,omitempty"`
	Failure      *BackingFailureDelivery         `json:"failure,omitempty"`
}

type BackingFailureDelivery struct {
	Reason   backingprovision.FailureReason `json:"reason"`
	Evidence []byte                         `json:"evidence,omitempty"`
	Receipt  *workexecution.Attempt         `json:"receipt,omitempty"`
}

func (record attemptRecord) validateBacking() error {
	backing := record.Backing
	if backing == nil || record.Attempt.Validate() != nil || record.Attempt.Direction != workexecution.Forward ||
		record.Attempt.Executor.Executor != workexecution.Agent || record.Attempt.FinishedAt != nil ||
		backing.Layout.Validate(record.Attempt) != nil || record.UpdatedAt.IsZero() || record.UpdatedAt.Before(record.Attempt.AssignedAt) {
		return invalid()
	}
	for _, present := range []bool{record.EnvironmentProvision != nil, record.Backup != nil, record.RemovalRecovery != nil,
		record.LifecycleRecovery != nil, record.Lifecycle != nil, record.AttachProvisionRecovery != nil,
		record.AttachProvisionFailure != nil, record.AttachProvision != nil, record.EntryRecovery != nil,
		record.EntryConfiguration != nil, record.ComponentRecovery != nil, record.Component != nil,
		record.NetworkRecovery != nil, record.Network != nil, record.VolumeRecovery != nil, record.Volume != nil,
		record.AttachRecovery != nil, record.AttachFailure != nil, record.AttachRemoval != nil,
		record.EnvironmentFailure != nil, record.EnvironmentRemoval != nil, record.ScriptFailure != nil,
		record.ScriptRecovery != nil, len(record.Script) != 0, record.Release != nil, record.Effect != nil,
		record.Completion != nil, record.Failure != nil, record.Recovery != nil, record.ScriptCompletion != nil} {
		if present {
			return invalid()
		}
	}
	if backing.Progress == nil {
		if record.Sequence != 0 || backing.Failure == nil || backing.Acknowledged || backing.Runtime != nil || backing.Completion != nil || backing.Hook != nil {
			return invalid()
		}
	} else if backing.Progress.Validate(record.Attempt, backing.Layout) != nil || backing.Progress.Sequence == 0 ||
		record.Sequence != backing.Progress.Sequence || !record.UpdatedAt.Equal(backing.Progress.UpdatedAt) {
		return invalid()
	}
	if backing.Hook != nil && backing.Hook.validate(record.Attempt, backing.Layout, backing.Progress) != nil {
		return invalid()
	}
	if backing.Runtime != nil {
		if backing.Progress == nil || record.Attempt.StartedAt == nil || backingruntimefact.ValidateObservation(*backing.Runtime) != nil ||
			backing.Runtime.ServiceID != backing.Layout.ServiceID || backing.Runtime.ObservedAt.Before(*record.Attempt.StartedAt) ||
			backing.Progress.Stage != backingprovision.VerifyRuntime && backing.Progress.Stage != backingprovision.Complete {
			return invalid()
		}
		if backing.Progress.Last == nil || backing.Runtime.ContainerID != backing.Progress.Last.ContainerID ||
			backing.Progress.Stage == backingprovision.Complete && !backing.Runtime.ObservedAt.Equal(backing.Progress.Last.At) {
			return invalid()
		}
	}
	if failure := backing.Failure; failure != nil {
		if failure.Reason != backingprovision.ExecutionFailed && failure.Reason != backingprovision.DeadlineExpired && failure.Reason != backingprovision.Interrupted ||
			backing.Completion != nil || backing.Progress != nil && backing.Progress.Stage == backingprovision.Complete {
			return invalid()
		}
		if receipt := failure.Receipt; receipt != nil {
			if !sameBackingTerminal(record.Attempt, *receipt) || receipt.Outcome == workexecution.Succeeded {
				return invalid()
			}
			saved, err := backingprovision.DecodeFailure(*receipt, backing.Layout, failure.Evidence, *receipt.Result)
			if err != nil || saved.Reason != failure.Reason || !saved.RecordedAt.Equal(*receipt.FinishedAt) {
				return invalid()
			}
			if saved.Checkpoint != nil && (backing.Progress == nil || saved.Checkpoint.Sequence > backing.Progress.Sequence) {
				return invalid()
			}
		} else if len(failure.Evidence) != 0 {
			return invalid()
		}
	}
	if terminal := backing.Completion; terminal != nil {
		if !sameBackingTerminal(record.Attempt, *terminal) || backing.Failure != nil || backing.Progress == nil ||
			backing.Progress.Stage != backingprovision.Complete || !backing.Acknowledged || backing.Runtime == nil {
			return invalid()
		}
		checkpoint, err := backing.Progress.Checkpoint(record.Attempt, backing.Layout)
		if err != nil {
			return err
		}
		evidence, err := backingprovision.CompletionEvidence(*terminal, checkpoint, *backing.Runtime)
		if err != nil || evidence != *terminal.Result || backing.Runtime.ObservedAt.After(*terminal.FinishedAt) {
			return invalid()
		}
	}
	return nil
}

func sameBackingTerminal(active, terminal workexecution.Attempt) bool {
	if terminal.Validate() != nil || !workexecution.SameAssignment(active, terminal) || terminal.FinishedAt == nil || terminal.Result == nil ||
		(active.StartedAt == nil) != (terminal.StartedAt == nil) {
		return false
	}
	return active.StartedAt == nil || active.StartedAt.Equal(*terminal.StartedAt)
}
