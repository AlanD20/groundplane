package executionplan

import (
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// BackupConsumerInput contains the owned, captured workload definition.
// Host execution must additionally present its acknowledged effect checkpoint;
// selecting a resource is not permission to stop or restart it.
type BackupConsumerInput struct {
	Attempt  workexecution.Attempt
	Source   *agentpb.BackupStepAuthority
	Artifact *agentpb.ComposeArtifact
	Service  *agentpb.ComposeService
	Fact     *agentpb.BackupServiceFact
}

func OpenBackupConsumer(assignment *agentpb.BackupWorkAssignment, authenticated workexecution.Identity,
	ordinal uint32, serviceID string,
) (BackupConsumerInput, error) {
	owned, attempt, err := OpenBackupWorkAssignment(assignment, authenticated)
	if err != nil {
		return BackupConsumerInput{}, err
	}
	return backupConsumerInput(owned.Input, attempt, ordinal, serviceID)
}

func OpenBackupRecoveryConsumer(assignment *agentpb.BackupRecoveryAssignment, authenticated workexecution.Identity,
	ordinal uint32, serviceID string,
) (BackupConsumerInput, error) {
	owned, attempt, _, err := OpenBackupRecoveryAssignment(assignment, authenticated)
	if err != nil {
		return BackupConsumerInput{}, err
	}
	if owned.RestoreMode != agentpb.BackupRestoreRecoveryMode_BACKUP_RESTORE_RECOVERY_MODE_LIVE_MUTATION {
		return BackupConsumerInput{}, errs.New(errs.KindStateConflict, "Consumer recovery requires a captured live mutation")
	}
	return backupConsumerInput(owned.Input, attempt, ordinal, serviceID)
}

func backupConsumerInput(input *agentpb.BackupWorkInput, attempt workexecution.Attempt,
	ordinal uint32, serviceID string,
) (BackupConsumerInput, error) {
	if uint64(ordinal) >= uint64(len(input.Steps)) {
		return BackupConsumerInput{}, errs.New(errs.KindStateConflict, "Volume consumer source is not captured")
	}
	step := input.Steps[ordinal]
	if step.GetRestore().GetVolume() == nil && step.GetRestore().GetPostgres() == nil && step.GetRestore().GetMysql() == nil {
		return BackupConsumerInput{}, errs.New(errs.KindStateConflict, "Volume consumer action requires a captured Restore source")
	}
	artifact, service, err := BackupConsumer(input.EnvironmentId, input.Services, input.Artifacts, step, serviceID)
	if err != nil {
		return BackupConsumerInput{}, err
	}
	var fact *agentpb.BackupServiceFact
	for _, candidate := range input.Services {
		if candidate.ServiceId == serviceID {
			fact = candidate
			break
		}
	}
	if fact == nil || fact.PriorRuntimeIntent == nil {
		return BackupConsumerInput{}, errs.New(errs.KindStateConflict, "Volume consumer runtime intent is unavailable")
	}
	return BackupConsumerInput{Attempt: attempt, Source: step, Artifact: artifact, Service: service, Fact: fact}, nil
}
