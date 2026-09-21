package backupplanning

import (
	"bytes"
	"encoding/hex"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func ValidateBackupRunExecutionPlan(run backupruntime.BackupRunRecord, plan *agentpb.ExecutionPlan) error {
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
	source backupruntime.BackupRunSourceAttemptRecord,
	capture *agentpb.BackupSourceCapture,
) bool {
	switch source.Kind {
	case backupruntime.BackupRuntimeSourceAttach:
		snapshot := source.Snapshot.Postgres
		attach := capture.GetAttach()
		return snapshot != nil && attach != nil &&
			attach.BackingServiceId == snapshot.BackingServiceID &&
			attach.BackingServiceRevision == uint64(snapshot.BackingServiceRevision) &&
			attach.Database == snapshot.Database && attach.Role == snapshot.Role
	case backupruntime.BackupRuntimeSourceConfig:
		snapshot := source.Snapshot.Config
		config := capture.GetConfig()
		return snapshot != nil && config != nil && config.SnapshotRevision == uint64(snapshot.ReadRevision)
	case backupruntime.BackupRuntimeSourceVolume:
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

func backupPlanSourceFormat(value backupruntime.BackupRuntimeFormat) agentpb.BackupSourceFormat {
	switch value {
	case backupruntime.BackupRuntimeFormatPostgres:
		return agentpb.BackupSourceFormat_BACKUP_SOURCE_FORMAT_POSTGRES_CUSTOM_V1
	case backupruntime.BackupRuntimeFormatConfig:
		return agentpb.BackupSourceFormat_BACKUP_SOURCE_FORMAT_ENVIRONMENT_CONFIG_V1
	case backupruntime.BackupRuntimeFormatVolume:
		return agentpb.BackupSourceFormat_BACKUP_SOURCE_FORMAT_VOLUME_TAR_V1
	default:
		return agentpb.BackupSourceFormat_BACKUP_SOURCE_FORMAT_UNSPECIFIED
	}
}

func backupPlanEncryption(value backupruntime.BackupRuntimeEncryption) agentpb.BackupEncryption {
	switch value {
	case backupruntime.BackupRuntimeEncryptionNone:
		return agentpb.BackupEncryption_BACKUP_ENCRYPTION_NONE
	case backupruntime.BackupRuntimeEncryptionAge:
		return agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE
	default:
		return agentpb.BackupEncryption_BACKUP_ENCRYPTION_UNSPECIFIED
	}
}

func backupPlanServiceIntent(value backupruntime.BackupServiceRuntimeIntent) agentpb.BackupServiceRuntimeIntent {
	switch value {
	case backupruntime.BackupServiceIntentRunning:
		return agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_RUNNING
	case backupruntime.BackupServiceIntentStopped:
		return agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_STOPPED
	case backupruntime.BackupServiceIntentAbsent:
		return agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_ABSENT
	default:
		return agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_UNSPECIFIED
	}
}

type PruneExecutionEvidence struct {
	Prune               backupruntime.BackupRecoveryPointPruneRecord
	PruneRevision       int64
	PointRevision       int64
	SourceRevision      int64
	EnvironmentRevision int64
	ConnectorRevision   int64
	ConnectorEndpoint   string
	ConnectorBucket     string
	ConnectorPrefix     string
	ConnectorRegion     string
	ConnectorPathStyle  bool
}

func ValidateBackupPruneExecutionPlan(
	dispatch backupruntime.BackupRecoveryPointPruneDispatchRecord,
	evidence []PruneExecutionEvidence,
	plan *agentpb.ExecutionPlan,
) error {
	if plan.Operation != agentpb.PlanOperation_PLAN_OPERATION_BACKUP_PRUNE ||
		plan.TargetId != dispatch.EnvironmentID || len(plan.Steps) != len(evidence) {
		return errs.New(errs.KindValidationFailed, "backup prune execution plan is invalid")
	}
	for index, item := range evidence {
		step := plan.Steps[index].GetBackupArtifactPrune()
		point := item.Prune.Point
		digest, err := hex.DecodeString(point.SHA256)
		if err != nil || step == nil || step.Ordinal != uint32(index+1) ||
			step.PruneOperationId != dispatch.OperationID || step.PruneRevision != uint64(item.PruneRevision) ||
			step.PointId != point.ID || step.PointRevision != uint64(item.PointRevision) ||
			step.SourceId != point.SourceID || step.SourceRevision != uint64(item.SourceRevision) ||
			step.EnvironmentId != point.EnvironmentID ||
			step.EnvironmentRevision != uint64(item.EnvironmentRevision) ||
			step.ConnectorId != point.ConnectorID || step.ConnectorRevision != uint64(item.ConnectorRevision) ||
			step.ConnectorEndpoint != item.ConnectorEndpoint || step.ConnectorBucket != item.ConnectorBucket ||
			step.ConnectorPrefix != item.ConnectorPrefix || step.ConnectorRegion != item.ConnectorRegion ||
			step.ConnectorAddressing != backupPlanAddressing(item.ConnectorPathStyle) ||
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
