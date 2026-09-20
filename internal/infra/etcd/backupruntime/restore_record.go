package backupruntime

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"time"
)

type BackupRestoreTargetSnapshot struct {
	Postgres *BackupPostgresSourceSnapshot `json:"postgres,omitempty"`
	Volume   *BackupVolumeSourceSnapshot   `json:"volume,omitempty"`
	Config   *BackupRestoreConfigTarget    `json:"config,omitempty"`
}

type BackupRestoreConfigTarget struct {
	EnvironmentID       string `json:"environment_id"`
	EnvironmentRevision int64  `json:"environment_revision"`
}

type BackupRestoreConfigProgress struct {
	CurrentEntryOrdinal        uint32 `json:"current_entry_ordinal"`
	NextDescriptorChunkOrdinal uint32 `json:"next_descriptor_chunk_ordinal"`
	NextValueChunkOrdinal      uint32 `json:"next_value_chunk_ordinal"`
	FinalizedEntryCount        uint32 `json:"finalized_entry_count"`
	DescriptorChunkCount       uint32 `json:"descriptor_chunk_count"`
	ValueChunkCount            uint32 `json:"value_chunk_count"`
	PlainValueBytes            uint64 `json:"plain_value_bytes"`
	DescriptorChainSHA256      string `json:"descriptor_chain_sha256,omitempty"`
	StoredValueChainSHA256     string `json:"stored_value_chain_sha256,omitempty"`
	StoredManifestSHA256       string `json:"stored_manifest_sha256,omitempty"`
	DeleteCursor               string `json:"delete_cursor,omitempty"`
	UpsertEntryOrdinal         uint32 `json:"upsert_entry_ordinal"`
	MaterializationGeneration  uint64 `json:"materialization_generation"`
}

type BackupRestoreRecord struct {
	TaskID                        string                       `json:"task_id"`
	OperationID                   string                       `json:"operation_id"`
	EnvironmentID                 string                       `json:"environment_id"`
	RestoreGenerationID           string                       `json:"restore_generation_id,omitempty"`
	RecoveryPointRevision         int64                        `json:"recovery_point_revision"`
	Point                         BackupRecoveryPointSnapshot  `json:"point"`
	SourceRevision                int64                        `json:"source_revision"`
	CurrentTarget                 BackupRestoreTargetSnapshot  `json:"current_target"`
	ConnectorRevision             int64                        `json:"connector_revision"`
	ConnectorHasDirectCredentials bool                         `json:"connector_has_direct_credentials"`
	ConnectorCredentialsRevision  int64                        `json:"connector_credentials_revision"`
	ExpectedKeyRecordRevision     int64                        `json:"expected_key_record_revision,omitempty"`
	ExpectedKeyValueRevision      int64                        `json:"expected_key_value_revision,omitempty"`
	UsesOldIdentity               bool                         `json:"uses_old_identity"`
	Artifact                      *BackupArtifactEvidence      `json:"artifact,omitempty"`
	StagedTreeManifestSHA256      string                       `json:"staged_tree_manifest_sha256,omitempty"`
	State                         BackupRestoreState           `json:"state"`
	MutationStarted               bool                         `json:"mutation_started"`
	ServiceCount                  uint32                       `json:"service_count"`
	ConfigProgress                *BackupRestoreConfigProgress `json:"config_progress,omitempty"`
	Verification                  BackupVerificationState      `json:"verification"`
	CreatedAt                     time.Time                    `json:"created_at"`
	UpdatedAt                     time.Time                    `json:"updated_at"`
}

type BackupRestoreServiceRecord struct {
	TaskID          string                     `json:"task_id"`
	Ordinal         uint32                     `json:"ordinal"`
	ServiceID       string                     `json:"service_id"`
	ServiceRevision int64                      `json:"service_revision"`
	PriorIntent     BackupServiceRuntimeIntent `json:"prior_intent"`
}

func validateBackupRestoreRecord(record BackupRestoreRecord) error {
	if recordcodec.ValidateID(ids.KindTask, record.TaskID) != nil ||
		recordcodec.ValidateID(ids.KindOperation, record.OperationID) != nil ||
		recordcodec.ValidateID(ids.KindEnvironment, record.EnvironmentID) != nil ||
		record.RecoveryPointRevision <= 0 || record.SourceRevision <= 0 || record.ConnectorRevision <= 0 ||
		record.ConnectorCredentialsRevision < 0 ||
		(record.ConnectorHasDirectCredentials != (record.ConnectorCredentialsRevision > 0)) ||
		!validBackupRestoreState(record.State) ||
		!validBackupVerificationState(record.Verification) ||
		!validBackupRuntimeLifecycle(record.CreatedAt, record.UpdatedAt) {
		return invalidBackupRuntimeRecord("backup restore identity or lifecycle is invalid")
	}
	if err := ValidateBackupRecoveryPointSnapshot(record.Point); err != nil {
		return err
	}
	if record.Point.EnvironmentID != record.EnvironmentID {
		return invalidBackupRuntimeRecord("backup restore point belongs to another environment")
	}
	if err := validateBackupRestoreTarget(record.Point, record.CurrentTarget); err != nil {
		return err
	}
	if record.Point.SourceKind == BackupRuntimeSourceConfig {
		if recordcodec.ValidateID(ids.KindConfig, record.RestoreGenerationID) != nil {
			return invalidBackupRuntimeRecord("config restore generation id is invalid")
		}
	} else if record.RestoreGenerationID != "" {
		return invalidBackupRuntimeRecord("non-config restore cannot carry a restore generation id")
	}
	if err := validateBackupRestoreKeyReferences(record); err != nil {
		return err
	}
	if record.Artifact != nil {
		if !validBackupArtifact(*record.Artifact) ||
			record.Artifact.SizeBytes != record.Point.SizeBytes ||
			record.Artifact.SHA256 != record.Point.SHA256 {
			return invalidBackupRuntimeRecord(
				"backup restore artifact evidence does not match its point",
			)
		}
	} else if restoreStateRequiresArtifact(record.State) {
		return invalidBackupRuntimeRecord(
			"backup restore state requires verified artifact evidence",
		)
	}
	if err := validateBackupRestoreStateTable(record); err != nil {
		return err
	}
	if err := validateBackupRestoreVolumeManifest(record); err != nil {
		return err
	}
	if err := validateBackupRestoreProgress(record); err != nil {
		return err
	}
	return validateBackupRestoreVerification(record)
}

func validateBackupRestoreTarget(
	point BackupRecoveryPointSnapshot,
	target BackupRestoreTargetSnapshot,
) error {
	if pointerCount(target.Postgres != nil, target.Volume != nil, target.Config != nil) != 1 {
		return invalidBackupRuntimeRecord("backup restore target must contain exactly one kind")
	}
	switch point.SourceKind {
	case BackupRuntimeSourceAttach:
		if target.Postgres == nil || validateBackupPostgresSnapshot(*target.Postgres) != nil ||
			target.Postgres.ConsumerEnvironmentID != point.EnvironmentID || target.Postgres.AttachID != point.TargetID {
			return invalidBackupRuntimeRecord("postgres restore target is invalid")
		}
	case BackupRuntimeSourceVolume:
		if target.Volume == nil || validateBackupVolumeSnapshot(*target.Volume) != nil ||
			target.Volume.EnvironmentID != point.EnvironmentID || target.Volume.VolumeID != point.TargetID {
			return invalidBackupRuntimeRecord("volume restore target is invalid")
		}
	case BackupRuntimeSourceConfig:
		if target.Config == nil ||
			recordcodec.ValidateID(ids.KindEnvironment, target.Config.EnvironmentID) != nil ||
			target.Config.EnvironmentRevision <= 0 ||
			target.Config.EnvironmentID != point.TargetID {
			return invalidBackupRuntimeRecord("config restore target is invalid")
		}
	default:
		return invalidBackupRuntimeRecord("backup restore target kind is invalid")
	}
	return nil
}

func validateBackupRestoreStateTable(record BackupRestoreRecord) error {
	allowed := false
	expectedMutation := false
	switch record.Point.SourceKind {
	case BackupRuntimeSourceAttach:
		switch record.State {
		case BackupRestoreQueued, BackupRestoreDownloading, BackupRestoreArtifactVerified,
			BackupRestoreConsumersStopped, BackupRestoreFailedSafe:
			allowed = true
		case BackupRestoreRestoring, BackupRestoreConsumersRestored, BackupRestoreVerified,
			BackupRestoreCompleted, BackupRestoreRecoveryRequired:
			allowed, expectedMutation = true, true
		}
	case BackupRuntimeSourceVolume:
		switch record.State {
		case BackupRestoreQueued, BackupRestoreDownloading, BackupRestoreArtifactVerified,
			BackupRestoreConsumersStopped, BackupRestoreRestoring, BackupRestoreTreeValidated,
			BackupRestoreExchangeReady, BackupRestoreFailedSafe:
			allowed = true
		case BackupRestoreExchanged, BackupRestoreConsumersRestored, BackupRestoreVerified,
			BackupRestoreCompleted, BackupRestoreRecoveryRequired:
			allowed, expectedMutation = true, true
		}
	case BackupRuntimeSourceConfig:
		switch record.State {
		case BackupRestoreQueued, BackupRestoreDownloading, BackupRestoreArtifactVerified,
			BackupRestoreReceiving, BackupRestoreStaged, BackupRestoreApplyingDeletes,
			BackupRestoreApplyingUpserts, BackupRestoreFailedSafe:
			allowed = true
		case BackupRestoreCanonicalComplete, BackupRestoreMaterializing, BackupRestoreVerified,
			BackupRestoreCompleted, BackupRestoreRecoveryRequired:
			allowed, expectedMutation = true, true
		}
	}
	if !allowed || record.MutationStarted != expectedMutation {
		return invalidBackupRuntimeRecord(
			"backup restore source state or mutation checkpoint is invalid",
		)
	}
	return nil
}

func validateBackupRestoreVolumeManifest(record BackupRestoreRecord) error {
	if record.Point.SourceKind != BackupRuntimeSourceVolume {
		if record.StagedTreeManifestSHA256 != "" {
			return invalidBackupRuntimeRecord(
				"non-Volume restore cannot carry a staged-tree manifest",
			)
		}
		return nil
	}
	if record.StagedTreeManifestSHA256 != "" && !recordcodec.ValidSHA256(record.StagedTreeManifestSHA256) {
		return invalidBackupRuntimeRecord("volume restore staged-tree manifest is invalid")
	}
	switch record.State {
	case BackupRestoreTreeValidated, BackupRestoreExchangeReady, BackupRestoreExchanged,
		BackupRestoreConsumersRestored, BackupRestoreVerified, BackupRestoreCompleted,
		BackupRestoreRecoveryRequired:
		if record.StagedTreeManifestSHA256 == "" {
			return invalidBackupRuntimeRecord(
				"Volume restore state requires a staged-tree manifest",
			)
		}
	}
	return nil
}

func validateBackupRestoreKeyReferences(record BackupRestoreRecord) error {
	if record.Point.Encryption == BackupRuntimeEncryptionNone {
		if record.UsesOldIdentity || record.ExpectedKeyRecordRevision != 0 ||
			record.ExpectedKeyValueRevision != 0 {
			return invalidBackupRuntimeRecord("unencrypted restore cannot carry age key references")
		}
		return nil
	}
	if record.UsesOldIdentity {
		if record.ExpectedKeyRecordRevision != 0 || record.ExpectedKeyValueRevision != 0 {
			return invalidBackupRuntimeRecord(
				"old-identity restore cannot carry current key revisions",
			)
		}
		return nil
	}
	if record.ExpectedKeyRecordRevision <= 0 || record.ExpectedKeyValueRevision <= 0 {
		return invalidBackupRuntimeRecord("current-era restore requires current key revisions")
	}
	return nil
}

func validateBackupRestoreProgress(record BackupRestoreRecord) error {
	if record.Point.SourceKind == BackupRuntimeSourceConfig {
		if record.ServiceCount != 0 || record.ConfigProgress == nil {
			return invalidBackupRuntimeRecord("config restore progress is invalid")
		}
		return validateBackupRestoreConfigProgress(record.State, *record.ConfigProgress)
	}
	if record.ConfigProgress != nil {
		return invalidBackupRuntimeRecord("non-config restore cannot carry config progress")
	}
	if record.Point.SourceKind == BackupRuntimeSourceVolume &&
		record.ServiceCount != uint32(len(record.CurrentTarget.Volume.Services)) {
		return invalidBackupRuntimeRecord(
			"volume restore service count does not match its snapshot",
		)
	}
	return nil
}

func validateBackupRestoreConfigProgress(
	state BackupRestoreState,
	progress BackupRestoreConfigProgress,
) error {
	if progress.NextDescriptorChunkOrdinal > progress.DescriptorChunkCount ||
		progress.NextValueChunkOrdinal > progress.ValueChunkCount {
		return invalidBackupRuntimeRecord("config restore chunk cursor exceeds received chunks")
	}
	if progress.MaterializationGeneration == 0 ||
		progress.CurrentEntryOrdinal != progress.FinalizedEntryCount ||
		(progress.FinalizedEntryCount > 0 && progress.DescriptorChunkCount < progress.FinalizedEntryCount) ||
		!optionalDigestMatchesCount(
			progress.DescriptorChainSHA256,
			progress.DescriptorChunkCount,
		) ||
		!optionalDigestMatchesCount(progress.StoredValueChainSHA256, progress.ValueChunkCount) ||
		(progress.StoredManifestSHA256 != "" && !recordcodec.ValidSHA256(progress.StoredManifestSHA256)) {
		return invalidBackupRuntimeRecord("config restore progress digest is invalid")
	}
	if progress.DeleteCursor != "" &&
		recordcodec.ValidateID(ids.KindEnvEntry, progress.DeleteCursor) != nil {
		return invalidBackupRuntimeRecord("config restore delete cursor is invalid")
	}
	if configRestoreStateRequiresManifest(state) && progress.StoredManifestSHA256 == "" {
		return invalidBackupRuntimeRecord("config restore state requires a manifest digest")
	}
	preManifest := state == BackupRestoreQueued || state == BackupRestoreDownloading ||
		state == BackupRestoreArtifactVerified || state == BackupRestoreReceiving
	if preManifest && progress.StoredManifestSHA256 != "" {
		return invalidBackupRuntimeRecord("config restore has a premature manifest digest")
	}
	if state != BackupRestoreReceiving &&
		(progress.NextDescriptorChunkOrdinal != 0 || progress.NextValueChunkOrdinal != 0) {
		return invalidBackupRuntimeRecord("config restore has an out-of-phase chunk cursor")
	}
	if (state == BackupRestoreQueued || state == BackupRestoreDownloading || state == BackupRestoreArtifactVerified) &&
		(progress.FinalizedEntryCount != 0 || progress.DescriptorChunkCount != 0 || progress.ValueChunkCount != 0 ||
			progress.PlainValueBytes != 0 || progress.DescriptorChainSHA256 != "" ||
			progress.StoredValueChainSHA256 != "" || progress.DeleteCursor != "" ||
			progress.UpsertEntryOrdinal != 0) {
		return invalidBackupRuntimeRecord("config restore has progress before receiving")
	}
	if (state == BackupRestoreReceiving || state == BackupRestoreStaged) &&
		(progress.DeleteCursor != "" || progress.UpsertEntryOrdinal != 0) {
		return invalidBackupRuntimeRecord("config restore has an out-of-phase apply cursor")
	}
	if state == BackupRestoreApplyingDeletes && progress.UpsertEntryOrdinal != 0 {
		return invalidBackupRuntimeRecord("config restore started upserts before deletes completed")
	}
	if progress.UpsertEntryOrdinal > progress.FinalizedEntryCount {
		return invalidBackupRuntimeRecord("config restore upsert cursor exceeds staged Entries")
	}
	if (state == BackupRestoreCanonicalComplete || state == BackupRestoreMaterializing ||
		state == BackupRestoreVerified || state == BackupRestoreCompleted ||
		state == BackupRestoreRecoveryRequired) && progress.UpsertEntryOrdinal != progress.FinalizedEntryCount {
		return invalidBackupRuntimeRecord(
			"config restore canonical switch precedes complete upserts",
		)
	}
	return nil
}

func validateBackupRestoreVerification(record BackupRestoreRecord) error {
	switch record.Verification {
	case BackupVerificationPassed:
		if record.State != BackupRestoreVerified && record.State != BackupRestoreCompleted {
			return invalidBackupRuntimeRecord("passed restore verification has an invalid state")
		}
	case BackupVerificationFailed:
		if record.State != BackupRestoreFailedSafe &&
			record.State != BackupRestoreRecoveryRequired {
			return invalidBackupRuntimeRecord("failed restore verification has an invalid state")
		}
	case BackupVerificationPending:
		if record.State == BackupRestoreVerified || record.State == BackupRestoreCompleted {
			return invalidBackupRuntimeRecord("completed restore requires passed verification")
		}
	}
	if record.State == BackupRestoreRecoveryRequired && !record.MutationStarted {
		return invalidBackupRuntimeRecord("recovery-required restore must have started mutation")
	}
	return nil
}

func validateBackupRestoreServiceRecord(record BackupRestoreServiceRecord) error {
	if recordcodec.ValidateID(ids.KindTask, record.TaskID) != nil ||
		recordcodec.ValidateID(ids.KindService, record.ServiceID) != nil || record.ServiceRevision <= 0 ||
		!validBackupServiceRuntimeIntent(record.PriorIntent) {
		return invalidBackupRuntimeRecord("backup restore service is invalid")
	}
	return nil
}
