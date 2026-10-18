package backupvolumetransfer

import (
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Bind selects a Volume source from authenticated immutable work input. This
// does not grant execution or accept a frame; the runtime must still enforce
// its active attempt and the transfer's durable credit.
func Bind(assignment *agentpb.BackupWorkAssignment, identity workexecution.Identity,
	ordinal uint32, direction agentpb.BackupVolumeManifestDirection,
) (Binding, error) {
	owned, attempt, err := executionplan.OpenBackupWorkAssignment(assignment, identity)
	if err != nil {
		return Binding{}, err
	}
	return bindSource(attempt, owned.Input, ordinal, direction)
}

// BindRecoverySource identifies retained files belonging to the failed forward
// execution. It grants no frame transfer or renewed forward execution.
func BindRecoverySource(assignment *agentpb.BackupRecoveryAssignment, identity workexecution.Identity,
	ordinal uint32, direction agentpb.BackupVolumeManifestDirection,
) (Binding, error) {
	owned, _, source, err := executionplan.OpenBackupRecoveryAssignment(assignment, identity)
	if err != nil {
		return Binding{}, err
	}
	wire, err := workexecution.EncodeAgentAttempt(source.Forward)
	if err != nil {
		return Binding{}, err
	}
	// Binding stores assignment identity, without later start/result metadata.
	original, err := workexecution.DecodeAgentAttempt(wire, source.Forward.Executor)
	if err != nil {
		return Binding{}, err
	}
	return bindSource(original, owned.Input, ordinal, direction)
}

func bindSource(attempt workexecution.Attempt, input *agentpb.BackupWorkInput,
	ordinal uint32, direction agentpb.BackupVolumeManifestDirection,
) (Binding, error) {
	if uint64(ordinal) >= uint64(len(input.Steps)) {
		return Binding{}, invalidManifest()
	}
	step := input.Steps[ordinal]
	switch direction {
	case agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_CAPTURE:
		if step.GetCapture().GetVolume() == nil {
			return Binding{}, invalidManifest()
		}
	case agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_NEW,
		agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_OLD:
		if step.GetRestore().GetVolume() == nil {
			return Binding{}, invalidManifest()
		}
	default:
		return Binding{}, invalidManifest()
	}
	id, err := TransferID(step.ExecutionId, direction)
	if err != nil {
		return Binding{}, err
	}
	binding := Binding{Attempt: attempt, SourceOrdinal: ordinal, TransferID: id, Direction: direction}
	copy(binding.AuthorityDigest[:], step.StepDigest)
	return binding, binding.Validate()
}

func (binding Binding) wireAttempt() *agentpb.WorkAttemptIdentity {
	value, err := workexecution.EncodeAgentAttempt(binding.Attempt)
	if err != nil {
		// Frame constructors validate Binding before reaching this helper.
		// An invalid binding cannot manufacture a partial wire identity.
		return nil
	}
	return value
}

func (binding Binding) matchesAttempt(value *agentpb.WorkAttemptIdentity) bool {
	attempt, err := workexecution.DecodeAgentAttempt(value, binding.Attempt.Executor)
	return err == nil && workexecution.SameAssignment(attempt, binding.Attempt)
}
