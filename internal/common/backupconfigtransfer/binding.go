package backupconfigtransfer

import (
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Bind selects one captured Config source. Runtime start permission and durable
// receiver credit are separate requirements; a binding alone grants neither.
func Bind(assignment *agentpb.BackupWorkAssignment, identity workexecution.Identity,
	ordinal uint32, direction agentpb.BackupConfigDirection,
) (Binding, error) {
	owned, attempt, err := executionplan.OpenBackupWorkAssignment(assignment, identity)
	if err != nil {
		return Binding{}, err
	}
	return bindSource(attempt, owned.Input, ordinal, direction)
}

// BindRecoverySource preserves the failed attempt's transfer identity. It does
// not grant new transfer writes; those require active recovery authorization.
func BindRecoverySource(assignment *agentpb.BackupRecoveryAssignment, identity workexecution.Identity,
	ordinal uint32, direction agentpb.BackupConfigDirection,
) (Binding, error) {
	owned, _, source, err := executionplan.OpenBackupRecoveryAssignment(assignment, identity)
	if err != nil {
		return Binding{}, err
	}
	wire, err := workexecution.EncodeAgentAttempt(source.Forward)
	if err != nil {
		return Binding{}, err
	}
	original, err := workexecution.DecodeAgentAttempt(wire, source.Forward.Executor)
	if err != nil {
		return Binding{}, err
	}
	return bindSource(original, owned.Input, ordinal, direction)
}

func bindSource(attempt workexecution.Attempt, input *agentpb.BackupWorkInput,
	ordinal uint32, direction agentpb.BackupConfigDirection,
) (Binding, error) {
	if uint64(ordinal) >= uint64(len(input.Steps)) {
		return Binding{}, invalid("Config source ordinal is invalid")
	}
	step := input.Steps[ordinal]
	switch direction {
	case agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_CAPTURE:
		if step.GetCapture().GetConfig() == nil {
			return Binding{}, invalid("Config capture source is required")
		}
	case agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_RESTORE:
		if step.GetRestore().GetConfig() == nil {
			return Binding{}, invalid("Config Restore source is required")
		}
	default:
		return Binding{}, invalid("Config transfer direction is invalid")
	}
	binding := Binding{Attempt: attempt, SourceOrdinal: ordinal, TransferID: step.ExecutionId, Direction: direction}
	copy(binding.AuthorityDigest[:], step.StepDigest)
	return binding, binding.Validate()
}

func (binding Binding) wireAttempt() *agentpb.WorkAttemptIdentity {
	value, err := workexecution.EncodeAgentAttempt(binding.Attempt)
	if err != nil {
		return nil
	} // Constructors validate the binding first.
	return value
}

func (binding Binding) matchesAttempt(value *agentpb.WorkAttemptIdentity) bool {
	attempt, err := workexecution.DecodeAgentAttempt(value, binding.Attempt.Executor)
	return err == nil && workexecution.SameAssignment(attempt, binding.Attempt)
}
