package etcd

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"strings"
	"time"
)

type BackupPostgresSourceSnapshot struct {
	ConsumerEnvironmentID      string `json:"consumer_environment_id"`
	AttachID                   string `json:"attach_id"`
	AttachRevision             int64  `json:"attach_revision"`
	BackingProjectID           string `json:"backing_project_id"`
	BackingProjectRevision     int64  `json:"backing_project_revision"`
	BackingEnvironmentID       string `json:"backing_environment_id"`
	BackingEnvironmentRevision int64  `json:"backing_environment_revision"`
	BackingServiceID           string `json:"backing_service_id"`
	BackingServiceRevision     int64  `json:"backing_service_revision"`
	AttachFactsRevision        int64  `json:"attach_facts_revision"`
	Database                   string `json:"database"`
	Role                       string `json:"role"`
}

type BackupVolumeServiceSnapshot struct {
	ServiceID       string                     `json:"service_id"`
	ServiceRevision int64                      `json:"service_revision"`
	ComposeKey      string                     `json:"compose_key,omitempty"`
	MountPaths      []string                   `json:"mount_paths,omitempty"`
	PriorIntent     BackupServiceRuntimeIntent `json:"prior_intent"`
}

type BackupVolumeSourceSnapshot struct {
	EnvironmentID       string                        `json:"environment_id"`
	EnvironmentRevision int64                         `json:"environment_revision"`
	VolumeID            string                        `json:"volume_id"`
	DesiredRevisionID   string                        `json:"desired_revision_id"`
	ProjectionRoot      int64                         `json:"projection_root_revision"`
	DependencyDigest    string                        `json:"dependency_digest"`
	ArtifactID          string                        `json:"artifact_id,omitempty"`
	ArtifactDigest      string                        `json:"artifact_digest,omitempty"`
	ArtifactRevision    int64                         `json:"artifact_revision,omitempty"`
	RenderGeneration    uint64                        `json:"render_generation"`
	ComposeVolumeKey    string                        `json:"compose_volume_key"`
	DockerVolumeName    string                        `json:"docker_volume_name"`
	AuthorizedVolumeDir string                        `json:"authorized_volume_dir"`
	Services            []BackupVolumeServiceSnapshot `json:"services"`
}

type BackupConfigSourceSnapshot struct {
	ConfigSnapshotID string `json:"config_snapshot_id"`
	ReadRevision     int64  `json:"read_revision"`
}

type BackupRunSourceSnapshot struct {
	Postgres *BackupPostgresSourceSnapshot `json:"postgres,omitempty"`
	Volume   *BackupVolumeSourceSnapshot   `json:"volume,omitempty"`
	Config   *BackupConfigSourceSnapshot   `json:"config,omitempty"`
}

type BackupRunSourceAttemptRecord struct {
	Ordinal                uint32                   `json:"ordinal"`
	SourceID               string                   `json:"source_id"`
	Kind                   BackupRuntimeSourceKind  `json:"kind"`
	TargetID               string                   `json:"target_id"`
	SourceRevision         int64                    `json:"source_revision"`
	TargetRevision         int64                    `json:"target_revision"`
	Snapshot               BackupRunSourceSnapshot  `json:"snapshot"`
	Format                 BackupRuntimeFormat      `json:"format"`
	RecoveryPointID        string                   `json:"recovery_point_id"`
	RecoveryPointCreatedAt time.Time                `json:"recovery_point_created_at"`
	ObjectKey              string                   `json:"object_key"`
	State                  BackupSourceAttemptState `json:"state"`
	Phase                  BackupSourceAttemptPhase `json:"phase"`
	SizeBytes              int64                    `json:"size_bytes,omitempty"`
	SHA256                 string                   `json:"sha256,omitempty"`
	FailureCode            BackupFailureCode        `json:"failure_code,omitempty"`
}

type BackupRunRecord struct {
	TaskID                        string                         `json:"task_id"`
	OperationID                   string                         `json:"operation_id"`
	RetryOfTaskID                 string                         `json:"retry_of_task_id,omitempty"`
	EnvironmentID                 string                         `json:"environment_id"`
	PolicyRevision                int64                          `json:"policy_revision"`
	RetentionKeep                 int64                          `json:"retention_keep"`
	Initiator                     BackupRunInitiator             `json:"initiator"`
	ScheduledAt                   *time.Time                     `json:"scheduled_at,omitempty"`
	ConnectorID                   string                         `json:"connector_id"`
	ConnectorRevision             int64                          `json:"connector_revision"`
	ConnectorEndpoint             string                         `json:"connector_endpoint,omitempty"`
	ConnectorBucket               string                         `json:"connector_bucket,omitempty"`
	ConnectorPrefix               string                         `json:"connector_prefix,omitempty"`
	ConnectorRegion               string                         `json:"connector_region,omitempty"`
	ConnectorPathStyle            bool                           `json:"connector_path_style"`
	ConnectorHasDirectCredentials bool                           `json:"connector_has_direct_credentials"`
	ConnectorCredentialsRevision  int64                          `json:"connector_credentials_revision"`
	Encryption                    BackupRuntimeEncryption        `json:"encryption"`
	BackupKeyRecordRevision       int64                          `json:"backup_key_record_revision,omitempty"`
	BackupKeyValueRevision        int64                          `json:"backup_key_value_revision,omitempty"`
	KeyEra                        int                            `json:"key_era,omitempty"`
	Recipient                     string                         `json:"recipient,omitempty"`
	State                         BackupRunState                 `json:"state"`
	Sources                       []BackupRunSourceAttemptRecord `json:"sources"`
	CreatedAt                     time.Time                      `json:"created_at"`
	UpdatedAt                     time.Time                      `json:"updated_at"`
}

func validateBackupRunRecord(record BackupRunRecord) error {
	if recordcodec.ValidateID(ids.KindTask, record.TaskID) != nil ||
		recordcodec.ValidateID(ids.KindOperation, record.OperationID) != nil ||
		recordcodec.ValidateID(ids.KindEnvironment, record.EnvironmentID) != nil ||
		recordcodec.ValidateID(
			ids.KindConnector,
			record.ConnectorID,
		) != nil || record.PolicyRevision <= 0 || record.RetentionKeep <= 0 ||
		record.RetentionKeep > MaximumBackupPolicyKeep ||
		record.ConnectorRevision <= 0 || record.ConnectorCredentialsRevision < 0 ||
		(record.ConnectorHasDirectCredentials != (record.ConnectorCredentialsRevision > 0)) ||
		!validBackupRunState(
			record.State,
		) || !validBackupRuntimeLifecycle(record.CreatedAt, record.UpdatedAt) {
		return invalidBackupRuntimeRecord("backup run identity or lifecycle is invalid")
	}
	hasUploadEndpoint := record.ConnectorEndpoint != "" || record.ConnectorBucket != "" ||
		record.ConnectorRegion != "" || record.ConnectorPathStyle
	if hasUploadEndpoint && (record.ConnectorEndpoint == "" || strings.HasSuffix(record.ConnectorEndpoint, "/") ||
		record.ConnectorBucket == "" || record.ConnectorRegion == "") {
		return invalidBackupRuntimeRecord("backup run Connector upload authority is incomplete")
	}
	if record.RetryOfTaskID != "" {
		if recordcodec.ValidateID(ids.KindTask, record.RetryOfTaskID) != nil ||
			record.RetryOfTaskID == record.TaskID {
			return invalidBackupRuntimeRecord("backup run retry task is invalid")
		}
	}
	if err := validateBackupRunInitiator(record); err != nil {
		return err
	}
	if err := validateBackupRuntimeEncryption(
		record.Encryption,
		record.BackupKeyRecordRevision,
		record.BackupKeyValueRevision,
		record.KeyEra,
		record.Recipient,
	); err != nil {
		return err
	}
	if len(record.Sources) == 0 || len(record.Sources) > MaximumBackupPolicySources {
		return invalidBackupRuntimeRecord("backup run source count is invalid")
	}
	seenSources := make(map[string]struct{}, len(record.Sources))
	seenPoints := make(map[string]struct{}, len(record.Sources))
	allSucceeded := true
	for index, source := range record.Sources {
		if source.Ordinal != uint32(index) {
			return invalidBackupRuntimeRecord("backup run source ordinals are not contiguous")
		}
		if err := validateBackupRunSourceAttempt(
			record.EnvironmentID,
			record.ConnectorPrefix,
			source,
		); err != nil {
			return err
		}
		if _, exists := seenSources[source.SourceID]; exists {
			return invalidBackupRuntimeRecord("backup run source ids are not unique")
		}
		if _, exists := seenPoints[source.RecoveryPointID]; exists {
			return invalidBackupRuntimeRecord("backup run recovery point ids are not unique")
		}
		seenSources[source.SourceID] = struct{}{}
		seenPoints[source.RecoveryPointID] = struct{}{}
		allSucceeded = allSucceeded && source.State == BackupSourceAttemptSucceeded
		if source.Kind == BackupRuntimeSourceConfig &&
			record.Encryption != BackupRuntimeEncryptionAge {
			return invalidBackupRuntimeRecord("config backup requires age encryption")
		}
	}
	if record.State == BackupRunCompleted && !allSucceeded {
		return invalidBackupRuntimeRecord("completed backup run has an incomplete source")
	}
	return validateBackupRunAttemptTable(record)
}

func validateBackupRunAttemptTable(record BackupRunRecord) error {
	if record.State == BackupRunCompleted {
		return nil
	}
	boundary := -1
	for index, source := range record.Sources {
		if source.State == BackupSourceAttemptSucceeded {
			if boundary >= 0 {
				return invalidBackupRuntimeRecord("backup run violates fail-fast source order")
			}
			continue
		}
		if boundary < 0 {
			boundary = index
		}
	}
	if boundary < 0 {
		if record.State == BackupRunRunning {
			return nil
		}
		return invalidBackupRuntimeRecord("non-completed backup run has no remaining source")
	}
	boundarySource := record.Sources[boundary]
	suffix := record.Sources[boundary+1:]
	suffixAll := func(state BackupSourceAttemptState) bool {
		for _, source := range suffix {
			if source.State != state || source.FailureCode != "" {
				return false
			}
		}
		return true
	}
	switch record.State {
	case BackupRunQueued:
		if boundarySource.FailureCode != "" || !suffixAll(BackupSourceAttemptPending) ||
			(boundarySource.State != BackupSourceAttemptPending &&
				boundarySource.State != BackupSourceAttemptPointCommitted) {
			return invalidBackupRuntimeRecord("queued backup run checkpoint is invalid")
		}
	case BackupRunRunning:
		if boundarySource.FailureCode != "" || !suffixAll(BackupSourceAttemptPending) ||
			!activeBackupSourceAttemptState(boundarySource.State) {
			return invalidBackupRuntimeRecord("running backup run checkpoint is invalid")
		}
	case BackupRunFailed:
		if boundarySource.FailureCode == "" || !suffixAll(BackupSourceAttemptUnstarted) ||
			!failedBackupSourceCheckpoint(
				boundarySource.State,
				boundarySource.Phase,
				boundarySource.FailureCode,
			) ||
			boundarySource.FailureCode == BackupFailureAborted ||
			boundarySource.FailureCode == BackupFailureTimedOut {
			return invalidBackupRuntimeRecord("failed backup run checkpoint is invalid")
		}
	case BackupRunAborted:
		if (boundarySource.State != BackupSourceAttemptFailed &&
			boundarySource.State != BackupSourceAttemptOrphaned) ||
			boundarySource.FailureCode != BackupFailureAborted ||
			!suffixAll(BackupSourceAttemptUnstarted) {
			return invalidBackupRuntimeRecord("aborted backup run checkpoint is invalid")
		}
	case BackupRunTimedOut:
		if (boundarySource.State != BackupSourceAttemptFailed &&
			boundarySource.State != BackupSourceAttemptOrphaned) ||
			boundarySource.FailureCode != BackupFailureTimedOut ||
			!suffixAll(BackupSourceAttemptUnstarted) {
			return invalidBackupRuntimeRecord("timed-out backup run checkpoint is invalid")
		}
	}
	return nil
}

func validateBackupRunInitiator(record BackupRunRecord) error {
	switch record.Initiator {
	case BackupRunInitiatorOperator:
		if record.ScheduledAt != nil {
			return invalidBackupRuntimeRecord("operator backup run cannot carry a scheduled time")
		}
	case BackupRunInitiatorSchedule:
		if record.ScheduledAt == nil || !validBackupRuntimeInstant(*record.ScheduledAt) ||
			record.ScheduledAt.After(record.CreatedAt) {
			return invalidBackupRuntimeRecord("scheduled backup run time is invalid")
		}
	default:
		return invalidBackupRuntimeRecord("backup run initiator is invalid")
	}
	return nil
}

func validateBackupRunSourceAttempt(
	environmentID string,
	connectorPrefix string,
	record BackupRunSourceAttemptRecord,
) error {
	if recordcodec.ValidateID(ids.KindBackupSource, record.SourceID) != nil ||
		record.SourceRevision <= 0 ||
		record.TargetRevision <= 0 ||
		recordcodec.ValidateID(ids.KindRecoveryPoint, record.RecoveryPointID) != nil ||
		!validBackupRuntimeInstant(record.RecoveryPointCreatedAt) ||
		!recoveryPointIDMatchesInstant(record.RecoveryPointID, record.RecoveryPointCreatedAt) ||
		!validBackupSourceAttemptState(record.State) ||
		!validBackupSourceAttemptPhase(record.Phase) ||
		!validBackupObjectKey(
			record.ObjectKey,
			environmentID,
			record.SourceID,
			connectorPrefix,
			record.RecoveryPointID,
		) {
		return invalidBackupRuntimeRecord("backup run source attempt is invalid")
	}
	if err := validateBackupSourceIdentity(
		record.Kind,
		record.TargetID,
		record.Format,
	); err != nil {
		return err
	}
	if record.Kind == BackupRuntimeSourceConfig && record.TargetID != environmentID {
		return invalidBackupRuntimeRecord(
			"config backup source target does not match its Environment",
		)
	}
	if err := validateBackupRunSourceSnapshot(
		record.Kind,
		environmentID,
		record.TargetID,
		record.TargetRevision,
		record.Snapshot,
	); err != nil {
		return err
	}
	if (record.SizeBytes == 0) != (record.SHA256 == "") || record.SizeBytes < 0 ||
		(record.SHA256 != "" && !recordcodec.ValidSHA256(record.SHA256)) {
		return invalidBackupRuntimeRecord("backup run source artifact evidence is invalid")
	}
	if sourceAttemptRequiresArtifact(record.State, record.Phase) &&
		(record.SizeBytes <= 0 || record.SHA256 == "") {
		return invalidBackupRuntimeRecord("backup run source state requires artifact evidence")
	}
	if sourceAttemptForbidsArtifact(record.State) &&
		(record.SizeBytes != 0 || record.SHA256 != "") {
		return invalidBackupRuntimeRecord("backup run source state cannot carry artifact evidence")
	}
	if !validBackupPhaseForAttemptState(record.State, record.Phase) ||
		!validBackupFailureCodeForAttempt(record.State, record.Phase, record.FailureCode) {
		return invalidBackupRuntimeRecord("backup source failure checkpoint is invalid")
	}
	return nil
}

func validateBackupRunSourceSnapshot(
	kind BackupRuntimeSourceKind,
	environmentID string,
	targetID string,
	targetRevision int64,
	snapshot BackupRunSourceSnapshot,
) error {
	count := pointerCount(snapshot.Postgres != nil, snapshot.Volume != nil, snapshot.Config != nil)
	if count != 1 {
		return invalidBackupRuntimeRecord("backup source snapshot must contain exactly one kind")
	}
	switch kind {
	case BackupRuntimeSourceAttach:
		if snapshot.Postgres == nil || validateBackupPostgresSnapshot(*snapshot.Postgres) != nil ||
			snapshot.Postgres.ConsumerEnvironmentID != environmentID || snapshot.Postgres.AttachID != targetID ||
			snapshot.Postgres.AttachRevision != targetRevision {
			return invalidBackupRuntimeRecord("postgres backup source snapshot is invalid")
		}
	case BackupRuntimeSourceVolume:
		if snapshot.Volume == nil || validateBackupVolumeSnapshot(*snapshot.Volume) != nil ||
			snapshot.Volume.EnvironmentID != environmentID || snapshot.Volume.VolumeID != targetID ||
			targetRevision <= 0 {
			return invalidBackupRuntimeRecord("volume backup source snapshot is invalid")
		}
	case BackupRuntimeSourceConfig:
		if snapshot.Config == nil || recordcodec.ValidateID(
			ids.KindTask,
			snapshot.Config.ConfigSnapshotID,
		) != nil || snapshot.Config.ReadRevision <= 0 {
			return invalidBackupRuntimeRecord("config backup source snapshot is invalid")
		}
	default:
		return invalidBackupRuntimeRecord("backup source snapshot kind is invalid")
	}
	return nil
}
