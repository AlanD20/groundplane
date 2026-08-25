package etcd

import (
	"bytes"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const backupTaskTimeoutSeconds = 6 * 60 * 60

type backupTaskPlanValidator func(*agentpb.ExecutionPlan) error

// Rationale: the public Task journal identifies one exact sealed private
// procedure without copying source controls, object locators, or secrets into
// generic Params or materialization fields.
func validateBackupTaskSealedPlan(
	authority backupTaskPublicationAuthority,
	record TaskRecord,
	sealed *agentpb.ExecutionPlan,
) error {
	validated, err := executionplan.Validate(sealed)
	if err != nil {
		return err
	}
	if authority.validatePlan == nil || record.PlanID != validated.PlanId ||
		record.PlanHash != hex.EncodeToString(validated.PlanHash) ||
		record.RenderGeneration != 0 || record.TimeoutSeconds != backupTaskTimeoutSeconds ||
		len(record.Params) != 0 || len(record.Materializations) != 0 ||
		len(record.Steps) != len(validated.Steps) {
		return errs.New(errs.KindValidationFailed, "backup Task sealed plan identity is invalid")
	}
	for index, step := range validated.Steps {
		if record.Steps[index].ID != step.StepId {
			return errs.New(errs.KindValidationFailed, "backup Task step order is invalid")
		}
	}
	return authority.validatePlan(validated)
}

func validateBackupRunExecutionPlan(run BackupRunRecord, plan *agentpb.ExecutionPlan) error {
	if plan.Operation != agentpb.PlanOperation_PLAN_OPERATION_BACKUP ||
		plan.TargetId != run.EnvironmentID || len(plan.Steps) != len(run.Sources) {
		return errs.New(errs.KindValidationFailed, "backup run execution plan is invalid")
	}
	for index, source := range run.Sources {
		capture := plan.Steps[index].GetBackupSourceCapture()
		if capture == nil || source.Ordinal != uint32(index) ||
			capture.SourceId != source.SourceID || capture.SourceRevision != uint64(source.SourceRevision) ||
			capture.TargetId != source.TargetID || capture.TargetRevision != uint64(source.TargetRevision) ||
			capture.PointId != source.RecoveryPointID || capture.ConnectorId != run.ConnectorID ||
			capture.ConnectorRevision != uint64(run.ConnectorRevision) ||
			capture.SourceFormat != backupPlanSourceFormat(source.Format) ||
			capture.Encryption != backupPlanEncryption(run.Encryption) ||
			capture.KeyEra != uint64(run.KeyEra) || capture.AgeRecipient != run.Recipient ||
			!backupRunSourcePlanSnapshotEqual(source, capture) {
			return errs.New(errs.KindValidationFailed, "backup run source plan evidence is invalid")
		}
	}
	return nil
}

func backupRunSourcePlanSnapshotEqual(
	source BackupRunSourceAttemptRecord,
	capture *agentpb.BackupSourceCapture,
) bool {
	switch source.Kind {
	case BackupRuntimeSourceAttach:
		snapshot := source.Snapshot.Postgres
		attach := capture.GetAttach()
		return snapshot != nil && attach != nil &&
			attach.BackingServiceId == snapshot.BackingServiceID &&
			attach.BackingServiceRevision == uint64(snapshot.BackingServiceRevision) &&
			attach.Database == snapshot.Database && attach.Role == snapshot.Role
	case BackupRuntimeSourceConfig:
		snapshot := source.Snapshot.Config
		config := capture.GetConfig()
		return snapshot != nil && config != nil && config.SnapshotRevision == uint64(snapshot.ReadRevision)
	case BackupRuntimeSourceVolume:
		snapshot := source.Snapshot.Volume
		volume := capture.GetVolume()
		if snapshot == nil || volume == nil || len(volume.Services) != len(snapshot.Services) {
			return false
		}
		for index, service := range snapshot.Services {
			planned := volume.Services[index]
			if planned == nil || planned.ServiceId != service.ServiceID ||
				planned.ServiceRevision != uint64(service.ServiceRevision) ||
				planned.PriorIntent != backupPlanServiceIntent(service.PriorIntent) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func backupPlanSourceFormat(value BackupRuntimeFormat) agentpb.BackupSourceFormat {
	switch value {
	case BackupRuntimeFormatPostgres:
		return agentpb.BackupSourceFormat_BACKUP_SOURCE_FORMAT_POSTGRES_CUSTOM_V1
	case BackupRuntimeFormatConfig:
		return agentpb.BackupSourceFormat_BACKUP_SOURCE_FORMAT_ENVIRONMENT_CONFIG_V1
	case BackupRuntimeFormatVolume:
		return agentpb.BackupSourceFormat_BACKUP_SOURCE_FORMAT_VOLUME_TAR_V1
	default:
		return agentpb.BackupSourceFormat_BACKUP_SOURCE_FORMAT_UNSPECIFIED
	}
}

func backupPlanEncryption(value BackupRuntimeEncryption) agentpb.BackupEncryption {
	switch value {
	case BackupRuntimeEncryptionNone:
		return agentpb.BackupEncryption_BACKUP_ENCRYPTION_NONE
	case BackupRuntimeEncryptionAge:
		return agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE
	default:
		return agentpb.BackupEncryption_BACKUP_ENCRYPTION_UNSPECIFIED
	}
}

func backupPlanServiceIntent(value BackupServiceRuntimeIntent) agentpb.BackupServiceRuntimeIntent {
	switch value {
	case BackupServiceIntentRunning:
		return agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_RUNNING
	case BackupServiceIntentStopped:
		return agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_STOPPED
	case BackupServiceIntentAbsent:
		return agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_ABSENT
	default:
		return agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_UNSPECIFIED
	}
}

type backupPruneExecutionEvidence struct {
	prune               BackupRecoveryPointPruneRecord
	pruneRevision       int64
	pointRevision       int64
	sourceRevision      int64
	environmentRevision int64
	connectorRevision   int64
	connectorEndpoint   string
	connectorBucket     string
	connectorPrefix     string
	connectorRegion     string
	connectorPathStyle  bool
}

func validateBackupPruneExecutionPlan(
	dispatch BackupRecoveryPointPruneDispatchRecord,
	evidence []backupPruneExecutionEvidence,
	plan *agentpb.ExecutionPlan,
) error {
	if plan.Operation != agentpb.PlanOperation_PLAN_OPERATION_BACKUP_PRUNE ||
		plan.TargetId != dispatch.EnvironmentID || len(plan.Steps) != len(evidence) {
		return errs.New(errs.KindValidationFailed, "backup prune execution plan is invalid")
	}
	for index, item := range evidence {
		step := plan.Steps[index].GetBackupArtifactPrune()
		point := item.prune.Point
		digest, err := hex.DecodeString(point.SHA256)
		if err != nil || step == nil || step.Ordinal != uint32(index+1) ||
			step.PruneOperationId != dispatch.OperationID || step.PruneRevision != uint64(item.pruneRevision) ||
			step.PointId != point.ID || step.PointRevision != uint64(item.pointRevision) ||
			step.SourceId != point.SourceID || step.SourceRevision != uint64(item.sourceRevision) ||
			step.EnvironmentId != point.EnvironmentID ||
			step.EnvironmentRevision != uint64(item.environmentRevision) ||
			step.ConnectorId != point.ConnectorID || step.ConnectorRevision != uint64(item.connectorRevision) ||
			step.ConnectorEndpoint != item.connectorEndpoint || step.ConnectorBucket != item.connectorBucket ||
			step.ConnectorPrefix != item.connectorPrefix || step.ConnectorRegion != item.connectorRegion ||
			step.ConnectorAddressing != backupPlanAddressing(item.connectorPathStyle) ||
			step.ProtectedObjectKey != point.ObjectKey || step.StoredSizeBytes != uint64(point.SizeBytes) ||
			!bytes.Equal(step.StoredSha256, digest) {
			return errs.New(errs.KindValidationFailed, "backup prune point plan evidence is invalid")
		}
	}
	return nil
}

func backupPlanAddressing(pathStyle bool) agentpb.BackupS3Addressing {
	if pathStyle {
		return agentpb.BackupS3Addressing_BACKUP_S3_ADDRESSING_PATH_STYLE
	}
	return agentpb.BackupS3Addressing_BACKUP_S3_ADDRESSING_VIRTUAL_HOSTED_STYLE
}
