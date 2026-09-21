package backup

import (
	"bytes"
	"encoding/hex"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"slices"
)

const (
	backupRunTaskTimeoutSeconds int64 = 6 * 60 * 60
)

// BackupRunPlanInput contains the already-persisted task and fixed plan
// evidence. Source order and all source identities come from the run record;
// the caller cannot add, remove, or reorder captures here.
type BackupRunPlanInput struct {
	Task   etcd.TaskRecord
	Run    backupruntime.BackupRunRecord
	Upload []BackupSourceUploadAuthority
}

// BackupSourceUploadAuthority is the typed internal seam for the accepted
// immutable-upload protocol.
type BackupSourceUploadAuthority struct {
	SourceID                    string
	PointID                     string
	ConnectorID                 string
	ConnectorRevision           int64
	ConnectorEndpoint           string
	ConnectorBucket             string
	ConnectorPrefix             string
	ConnectorRegion             string
	ConnectorAddressing         string
	ObjectKey                   string
	ImmutableCreate             bool
	PutAfterArtifactPreparedAck bool
	HeadAfterUploadCompletedAck bool
	RequireStoppedServiceIDs    []string
	Volume                      *BackupVolumeExecutionAuthority
}

// BackupVolumeExecutionAuthority is the immutable Volume input carried by the
// active execution-plan schema.
type BackupVolumeExecutionAuthority struct {
	ArtifactID          string
	ArtifactDigest      string
	ArtifactRevision    int64
	ProjectionRoot      int64
	RenderGeneration    uint64
	ComposeVolumeKey    string
	DockerVolumeName    string
	AuthorizedVolumeDir string
	Consumers           []BackupVolumeConsumerAuthority
}

type BackupVolumeConsumerAuthority struct {
	ServiceID       string
	ServiceRevision int64
	ComposeKey      string
	MountPaths      []string
	PriorIntent     backupruntime.BackupServiceRuntimeIntent
}

func BackupRunUploadAuthorities(run backupruntime.BackupRunRecord) []BackupSourceUploadAuthority {
	result := make([]BackupSourceUploadAuthority, len(run.Sources))
	for index, source := range run.Sources {
		result[index] = BackupSourceUploadAuthority{
			SourceID:          source.SourceID,
			PointID:           source.RecoveryPointID,
			ConnectorID:       run.ConnectorID,
			ConnectorRevision: run.ConnectorRevision,
			ConnectorEndpoint: run.ConnectorEndpoint,
			ConnectorBucket:   run.ConnectorBucket,
			ConnectorPrefix:   run.ConnectorPrefix,
			ConnectorRegion:   run.ConnectorRegion,
			ConnectorAddressing: backupConnectorAddressing(
				run.ConnectorPathStyle,
			),
			ObjectKey:                   source.ObjectKey,
			ImmutableCreate:             true,
			PutAfterArtifactPreparedAck: true,
			HeadAfterUploadCompletedAck: true,
		}
		if source.Snapshot.Volume != nil {
			volume := source.Snapshot.Volume
			result[index].Volume = &BackupVolumeExecutionAuthority{
				ArtifactID:          volume.ArtifactID,
				ArtifactDigest:      volume.ArtifactDigest,
				ArtifactRevision:    volume.ArtifactRevision,
				ProjectionRoot:      volume.ProjectionRoot,
				RenderGeneration:    volume.RenderGeneration,
				ComposeVolumeKey:    volume.ComposeVolumeKey,
				DockerVolumeName:    volume.DockerVolumeName,
				AuthorizedVolumeDir: volume.AuthorizedVolumeDir,
			}
			for _, service := range source.Snapshot.Volume.Services {
				result[index].RequireStoppedServiceIDs = append(
					result[index].RequireStoppedServiceIDs,
					service.ServiceID,
				)
				result[index].Volume.Consumers = append(
					result[index].Volume.Consumers,
					BackupVolumeConsumerAuthority{
						ServiceID:       service.ServiceID,
						ServiceRevision: service.ServiceRevision,
						ComposeKey:      service.ComposeKey,
						MountPaths:      append([]string(nil), service.MountPaths...),
						PriorIntent:     service.PriorIntent,
					},
				)
			}
		}
	}
	return result
}

func backupConnectorAddressing(pathStyle bool) string {
	if pathStyle {
		return "path"
	}
	return "virtual-hosted"
}

// BuildBackupRunPlan builds and seals the immutable agent plan for a backup
// task. It intentionally has no credential, database, or other
// plaintext-bearing fields.
func BuildBackupRunPlan(input BackupRunPlanInput) (*agentpb.ExecutionPlan, error) {
	task := input.Task
	run := input.Run
	if task.Type != taskjournal.TaskBackup {
		return nil, errs.New(errs.KindValidationFailed, "backup run task type must be backup")
	}
	if task.Executor != taskjournal.TaskExecutorAgent {
		return nil, errs.New(
			errs.KindValidationFailed,
			"backup run task must be agent-executed",
		)
	}
	switch {
	case run.RetryOfTaskID != "":
		if task.Actor != etcd.TaskActorOperator || task.RetryOf != run.RetryOfTaskID {
			return nil, errs.New(
				errs.KindValidationFailed,
				"backup retry task must be operator-acted and name its source",
			)
		}
	case run.Initiator == backupruntime.BackupRunInitiatorOperator:
		if task.Actor != etcd.TaskActorOperator {
			return nil, errs.New(errs.KindValidationFailed, "operator backup run task must be operator-acted")
		}
	case run.Initiator == backupruntime.BackupRunInitiatorSchedule:
		if task.Actor != etcd.TaskActorSystem {
			return nil, errs.New(errs.KindValidationFailed, "scheduled backup run task must be system-acted")
		}
	default:
		return nil, errs.New(errs.KindValidationFailed, "backup run initiator is invalid")
	}
	if task.Status != taskjournal.TaskStatusPending && task.Status != taskjournal.TaskStatusRunning {
		return nil, errs.New(errs.KindValidationFailed, "backup run task status must be pending or running")
	}
	if task.Target == "" || task.Target != run.EnvironmentID {
		return nil, errs.New(
			errs.KindValidationFailed,
			"backup run task target must match run environment",
		)
	}
	if task.ID == "" || task.OperationID == "" || task.PlanID == "" || run.TaskID != task.ID ||
		run.OperationID != task.OperationID || task.RetryOf != run.RetryOfTaskID {
		return nil, errs.New(
			errs.KindValidationFailed,
			"backup run task and run identities do not match",
		)
	}
	if task.RenderGeneration != 0 {
		return nil, errs.New(
			errs.KindValidationFailed,
			"backup run task plan metadata is not unrendered",
		)
	}
	if task.TimeoutSeconds != backupRunTaskTimeoutSeconds {
		return nil, errs.New(errs.KindValidationFailed, "backup run task timeout must be six hours")
	}
	if len(task.Params) != 0 || len(task.Materializations) != 0 {
		return nil, errs.New(
			errs.KindValidationFailed,
			"backup run task params and materializations must be empty",
		)
	}
	if !((task.Status == taskjournal.TaskStatusPending && run.State == backupruntime.BackupRunQueued) ||
		(task.Status == taskjournal.TaskStatusRunning && run.State == backupruntime.BackupRunRunning)) ||
		(run.Initiator != backupruntime.BackupRunInitiatorOperator && run.Initiator != backupruntime.BackupRunInitiatorSchedule) {
		return nil, errs.New(
			errs.KindValidationFailed,
			"backup run state must match its pending or running task and have a valid initiator",
		)
	}
	if len(run.Sources) == 0 || len(run.Sources) > executionplan.MaximumBackupSources {
		return nil, errs.New(
			errs.KindValidationFailed,
			"backup run source count must be between one and twelve",
		)
	}
	if len(task.Steps) != len(run.Sources) {
		return nil, errs.New(
			errs.KindValidationFailed,
			"backup run task step count must match source count",
		)
	}
	if err := validateBackupRunUploadAuthorities(run, input.Upload); err != nil {
		return nil, err
	}
	if ids.Validate(ids.KindTask, task.ID) != nil ||
		ids.Validate(ids.KindOperation, task.OperationID) != nil ||
		ids.Validate(ids.KindPlan, task.PlanID) != nil {
		return nil, errs.New(errs.KindValidationFailed, "backup run task identities are invalid")
	}

	plan := &agentpb.ExecutionPlan{
		Schema:    executionplan.SchemaVersion,
		PlanId:    task.PlanID,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_BACKUP,
		TargetId:  run.EnvironmentID,
		Steps:     make([]*agentpb.ExecutionStep, 0, len(run.Sources)),
	}
	for index, source := range run.Sources {
		step := task.Steps[index]
		if ids.Validate(ids.KindStep, step.ID) != nil {
			return nil, errs.New(errs.KindValidationFailed, "backup run step identity is invalid")
		}
		capture, err := backupRunCapture(source, run, input.Upload[index])
		if err != nil {
			return nil, err
		}
		plan.Steps = append(plan.Steps, &agentpb.ExecutionStep{
			StepId:         step.ID,
			TimeoutSeconds: uint32(backupRunTaskTimeoutSeconds),
			Payload: &agentpb.ExecutionStep_BackupSourceCapture{
				BackupSourceCapture: capture,
			},
		})
	}
	sealed, err := executionplan.Seal(plan)
	if err != nil {
		return nil, err
	}
	if task.PlanHash != "" {
		durableHash, decodeErr := hex.DecodeString(task.PlanHash)
		if decodeErr != nil || !bytes.Equal(durableHash, sealed.PlanHash) {
			return nil, errs.New(
				errs.KindStateConflict, "backup run sealed plan changed during reconnect",
			)
		}
	}
	return sealed, nil
}

func validateBackupRunUploadAuthorities(
	run backupruntime.BackupRunRecord,
	authorities []BackupSourceUploadAuthority,
) error {
	if len(authorities) != len(run.Sources) {
		return errs.New(errs.KindValidationFailed, "backup upload authority count is invalid")
	}
	for index, authority := range authorities {
		source := run.Sources[index]
		if authority.SourceID != source.SourceID || authority.PointID != source.RecoveryPointID ||
			authority.ConnectorID != run.ConnectorID || authority.ConnectorRevision != run.ConnectorRevision ||
			authority.ConnectorEndpoint != run.ConnectorEndpoint || authority.ConnectorBucket != run.ConnectorBucket ||
			authority.ConnectorPrefix != run.ConnectorPrefix || authority.ConnectorRegion != run.ConnectorRegion ||
			authority.ConnectorAddressing != backupConnectorAddressing(run.ConnectorPathStyle) ||
			authority.ConnectorEndpoint == "" || authority.ConnectorBucket == "" || authority.ConnectorRegion == "" ||
			authority.ObjectKey != source.ObjectKey || !authority.ImmutableCreate ||
			!authority.PutAfterArtifactPreparedAck || !authority.HeadAfterUploadCompletedAck {
			return errs.New(
				errs.KindValidationFailed,
				"backup immutable upload authority is invalid",
			)
		}
		expectedStopped := []string(nil)
		if source.Snapshot.Volume != nil {
			volume := source.Snapshot.Volume
			if authority.Volume == nil || authority.Volume.ArtifactID != volume.ArtifactID ||
				authority.Volume.ArtifactDigest != volume.ArtifactDigest ||
				authority.Volume.ArtifactRevision != volume.ArtifactRevision ||
				authority.Volume.ProjectionRoot != volume.ProjectionRoot ||
				authority.Volume.RenderGeneration != volume.RenderGeneration ||
				authority.Volume.ComposeVolumeKey != volume.ComposeVolumeKey ||
				authority.Volume.DockerVolumeName != volume.DockerVolumeName ||
				authority.Volume.AuthorizedVolumeDir != volume.AuthorizedVolumeDir ||
				authority.Volume.ArtifactID == "" || len(authority.Volume.ArtifactDigest) != 64 ||
				authority.Volume.ArtifactRevision <= 0 || authority.Volume.ProjectionRoot <= 0 ||
				authority.Volume.RenderGeneration == 0 ||
				authority.Volume.ComposeVolumeKey == "" || authority.Volume.DockerVolumeName == "" ||
				authority.Volume.AuthorizedVolumeDir == "" ||
				len(authority.Volume.Consumers) != len(volume.Services) {
				return errs.New(
					errs.KindStateConflict,
					"backup Volume authority is invalid",
				)
			}
			for _, service := range source.Snapshot.Volume.Services {
				expectedStopped = append(expectedStopped, service.ServiceID)
			}
			for consumerIndex, consumer := range authority.Volume.Consumers {
				service := volume.Services[consumerIndex]
				if consumer.ServiceID != service.ServiceID || consumer.ServiceRevision != service.ServiceRevision ||
					consumer.ComposeKey != service.ComposeKey || !slices.Equal(consumer.MountPaths, service.MountPaths) ||
					consumer.PriorIntent != service.PriorIntent ||
					consumer.ComposeKey == "" ||
					len(consumer.MountPaths) == 0 {
					return errs.New(
						errs.KindStateConflict,
						"backup Volume consumer authority is incomplete",
					)
				}
			}
		} else if authority.Volume != nil {
			return errs.New(errs.KindValidationFailed, "non-Volume source has Volume authority")
		}
		if !slices.Equal(authority.RequireStoppedServiceIDs, expectedStopped) {
			return errs.New(
				errs.KindValidationFailed,
				"backup Volume stopped-consumer authority is incomplete",
			)
		}
	}
	return nil
}

func backupRunCapture(
	source backupruntime.BackupRunSourceAttemptRecord,
	run backupruntime.BackupRunRecord,
	upload BackupSourceUploadAuthority,
) (*agentpb.BackupSourceCapture, error) {
	if source.SourceID == "" || source.TargetID == "" || source.RecoveryPointID == "" ||
		source.ObjectKey == "" {
		return nil, errs.New(
			errs.KindValidationFailed,
			"source, target, point, and object identities are required",
		)
	}
	capture := &agentpb.BackupSourceCapture{
		SourceId:          source.SourceID,
		SourceRevision:    uint64(source.SourceRevision),
		TargetId:          source.TargetID,
		TargetRevision:    uint64(source.TargetRevision),
		PointId:           source.RecoveryPointID,
		ConnectorId:       run.ConnectorID,
		ConnectorRevision: uint64(run.ConnectorRevision),
		SourceFormat:      backupRunPlanFormat(source.Format),
		Encryption:        backupRunPlanEncryption(run.Encryption),
		KeyEra:            uint64(run.KeyEra),
		AgeRecipient:      run.Recipient,
		Upload: &agentpb.BackupUploadAuthority{
			ConnectorEndpoint:           upload.ConnectorEndpoint,
			ConnectorBucket:             upload.ConnectorBucket,
			ConnectorPrefix:             upload.ConnectorPrefix,
			ConnectorRegion:             upload.ConnectorRegion,
			ConnectorAddressing:         backupRunPlanAddressing(upload.ConnectorAddressing),
			ProtectedObjectKey:          upload.ObjectKey,
			ImmutableCreate:             upload.ImmutableCreate,
			PutAfterArtifactPreparedAck: upload.PutAfterArtifactPreparedAck,
			HeadAfterUploadCompletedAck: upload.HeadAfterUploadCompletedAck,
		},
	}
	switch source.Kind {
	case backupruntime.BackupRuntimeSourceAttach:
		if source.Snapshot.Postgres == nil {
			return nil, errs.New(errs.KindValidationFailed, "postgres snapshot is required")
		}
		postgres := source.Snapshot.Postgres
		capture.Source = &agentpb.BackupSourceCapture_Attach{Attach: &agentpb.BackupAttachSource{
			BackingServiceId:       postgres.BackingServiceID,
			BackingServiceRevision: uint64(postgres.BackingServiceRevision),
			Database:               postgres.Database,
			Role:                   postgres.Role,
		}}
	case backupruntime.BackupRuntimeSourceConfig:
		if source.Snapshot.Config == nil {
			return nil, errs.New(errs.KindValidationFailed, "config snapshot is required")
		}
		capture.Source = &agentpb.BackupSourceCapture_Config{Config: &agentpb.BackupConfigSource{
			SnapshotRevision: uint64(source.Snapshot.Config.ReadRevision),
		}}
	case backupruntime.BackupRuntimeSourceVolume:
		if source.Snapshot.Volume == nil {
			return nil, errs.New(errs.KindValidationFailed, "volume snapshot is required")
		}
		artifactSHA256, err := hex.DecodeString(source.Snapshot.Volume.ArtifactDigest)
		if err != nil {
			return nil, errs.New(errs.KindValidationFailed, "volume artifact digest is invalid")
		}
		services := make([]*agentpb.BackupVolumeService, 0, len(source.Snapshot.Volume.Services))
		for _, service := range source.Snapshot.Volume.Services {
			services = append(services, &agentpb.BackupVolumeService{
				ServiceId:       service.ServiceID,
				ServiceRevision: uint64(service.ServiceRevision),
				PriorIntent:     backupRunPlanServiceIntent(service.PriorIntent),
				ComposeKey:      service.ComposeKey,
				MountPaths:      append([]string(nil), service.MountPaths...),
			})
		}
		volume := source.Snapshot.Volume
		capture.Source = &agentpb.BackupSourceCapture_Volume{
			Volume: &agentpb.BackupVolumeSource{
				Services: services, ArtifactId: volume.ArtifactID, ArtifactSha256: artifactSHA256,
				ArtifactRevision: uint64(volume.ArtifactRevision), ProjectionRoot: uint64(volume.ProjectionRoot),
				RenderGeneration: volume.RenderGeneration, ComposeVolumeKey: volume.ComposeVolumeKey,
				DockerVolumeName: volume.DockerVolumeName, AuthorizedVolumeDir: volume.AuthorizedVolumeDir,
			},
		}
	default:
		return nil, errs.New(
			errs.KindStrategyNotImplemented,
			"backup source strategy is not implemented",
		)
	}
	return capture, nil
}

func backupRunPlanAddressing(value string) agentpb.BackupS3Addressing {
	if value == "path" {
		return agentpb.BackupS3Addressing_BACKUP_S3_ADDRESSING_PATH_STYLE
	}
	if value == "virtual-hosted" {
		return agentpb.BackupS3Addressing_BACKUP_S3_ADDRESSING_VIRTUAL_HOSTED_STYLE
	}
	return agentpb.BackupS3Addressing_BACKUP_S3_ADDRESSING_UNSPECIFIED
}

func backupRunPlanFormat(format backupruntime.BackupRuntimeFormat) agentpb.BackupSourceFormat {
	switch format {
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

func backupRunPlanEncryption(encryption backupruntime.BackupRuntimeEncryption) agentpb.BackupEncryption {
	switch encryption {
	case backupruntime.BackupRuntimeEncryptionNone:
		return agentpb.BackupEncryption_BACKUP_ENCRYPTION_NONE
	case backupruntime.BackupRuntimeEncryptionAge:
		return agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE
	default:
		return agentpb.BackupEncryption_BACKUP_ENCRYPTION_UNSPECIFIED
	}
}

func backupRunPlanServiceIntent(
	intent backupruntime.BackupServiceRuntimeIntent,
) agentpb.BackupServiceRuntimeIntent {
	switch intent {
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
