package backupruntime

import (
	"github.com/AlanD20/groundplane/internal/common/databaseversion"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/oklog/ulid/v2"
	"time"
)

type BackupRestoreTargetSnapshot struct {
	Postgres *BackupRestorePostgresTarget `json:"postgres,omitempty"`
	MySQL    *BackupRestoreMySQLTarget    `json:"mysql,omitempty"`
	Volume   *BackupVolumeSourceSnapshot  `json:"volume,omitempty"`
	Config   *BackupRestoreConfigTarget   `json:"config,omitempty"`
}

type BackupRestoreConfigTarget struct {
	EnvironmentID        string `json:"environment_id"`
	EnvironmentRevision  int64  `json:"environment_revision"`
	BaselineRevisionID   string `json:"baseline_revision_id"`
	BaselineHeadRevision int64  `json:"baseline_head_revision"`
	RenderGeneration     uint64 `json:"render_generation"`
	FileContextSHA256    string `json:"file_context_sha256"`
}

type BackupRestoreConfigProgress struct {
	RevisionRootSHA256            string `json:"revision_root_sha256,omitempty"`
	RevisionRootRevision          int64  `json:"revision_root_revision"`
	ProjectionSHA256              string `json:"projection_sha256,omitempty"`
	IdentitiesSHA256              string `json:"identities_sha256,omitempty"`
	ExpectedMaterializationSHA256 string `json:"expected_materialization_sha256,omitempty"`
	DeleteEntryCount              uint32 `json:"delete_entry_count"`
	DeleteEntryOrdinal            uint32 `json:"delete_entry_ordinal"`
	UpsertEntryOrdinal            uint32 `json:"upsert_entry_ordinal"`
	PublishedHeadRevision         int64  `json:"published_head_revision"`
	MaterializationSHA256         string `json:"materialization_sha256,omitempty"`
	SourceCleanupCompleted        bool   `json:"source_cleanup_completed"`
}

type BackupRestoreVolumeProgress struct {
	OldEntryCount            uint64 `json:"old_entry_count"`
	OldContentManifestSHA256 string `json:"old_content_manifest_sha256,omitempty"`
	OldFullTreeSHA256        string `json:"old_full_tree_sha256,omitempty"`
	ConstructionCursor       uint64 `json:"construction_cursor"`
	FinalizationCursor       uint64 `json:"finalization_cursor"`
	DeletionCursor           uint64 `json:"deletion_cursor"`
	PendingConstruction      uint64 `json:"pending_construction"`
	PendingFinalization      uint64 `json:"pending_finalization"`
	PendingDeletion          uint64 `json:"pending_deletion"`
	ExchangeIntent           bool   `json:"exchange_intent"`
	Exchanged                bool   `json:"exchanged"`
	OldRemoved               bool   `json:"old_removed"`
	ServiceMutationStarted   bool   `json:"service_mutation_started"`
	ConsumersStopped         bool   `json:"consumers_stopped"`
	ServiceCursor            uint32 `json:"service_cursor"`
	ServicesRecovered        bool   `json:"services_recovered"`
}

type BackupRestoreRecord struct {
	TargetVersions                *databaseversion.Target        `json:"target_versions,omitempty"`
	VersionReviewSHA256           string                         `json:"version_review_sha256,omitempty"`
	VersionDifferenceAcknowledged bool                           `json:"version_difference_acknowledged"`
	TaskID                        string                         `json:"task_id"`
	OperationID                   string                         `json:"operation_id"`
	EnvironmentID                 string                         `json:"environment_id"`
	RestoreGenerationID           string                         `json:"restore_generation_id,omitempty"`
	RecoveryPointRevision         int64                          `json:"recovery_point_revision"`
	Point                         BackupRecoveryPointSnapshot    `json:"point"`
	SourceRevision                int64                          `json:"source_revision"`
	CurrentTarget                 BackupRestoreTargetSnapshot    `json:"current_target"`
	ConnectorRevision             int64                          `json:"connector_revision"`
	ConnectorHasDirectCredentials bool                           `json:"connector_has_direct_credentials"`
	ConnectorCredentialsRevision  int64                          `json:"connector_credentials_revision"`
	ExpectedKeyRecordRevision     int64                          `json:"expected_key_record_revision,omitempty"`
	ExpectedKeyValueRevision      int64                          `json:"expected_key_value_revision,omitempty"`
	UsesOldIdentity               bool                           `json:"uses_old_identity"`
	Artifact                      *BackupArtifactEvidence        `json:"artifact,omitempty"`
	StagedTreeManifestSHA256      string                         `json:"staged_tree_manifest_sha256,omitempty"`
	State                         BackupRestoreState             `json:"state"`
	MutationStarted               bool                           `json:"mutation_started"`
	ServiceCount                  uint32                         `json:"service_count"`
	ConfigProgress                *BackupRestoreConfigProgress   `json:"config_progress,omitempty"`
	VolumeProgress                *BackupRestoreVolumeProgress   `json:"volume_progress,omitempty"`
	DatabaseProgress              *BackupRestoreDatabaseProgress `json:"database_progress,omitempty"`
	Verification                  BackupVerificationState        `json:"verification"`
	CreatedAt                     time.Time                      `json:"created_at"`
	UpdatedAt                     time.Time                      `json:"updated_at"`
}

type BackupRestoreServiceRecord struct {
	TaskID          string                     `json:"task_id"`
	Ordinal         uint32                     `json:"ordinal"`
	ServiceID       string                     `json:"service_id"`
	ServiceRevision int64                      `json:"service_revision"`
	PriorIntent     BackupServiceRuntimeIntent `json:"prior_intent"`
}

func ValidateBackupRestoreRecord(record BackupRestoreRecord) error {
	if err := ValidateBackupRestoreSelection(record); err != nil {
		return err
	}
	if record.Point.SourceKind == BackupRuntimeSourceAttach {
		return ValidateRestoreVersionReview(record)
	}
	return nil
}

// Selection is read-only preflight state. Durable database Restore records
// additionally require the exact observed target and accepted version review.
func ValidateBackupRestoreSelection(record BackupRestoreRecord) error {
	if record.TargetVersions != nil &&
		(record.TargetVersions.Validate() != nil || !recordcodec.ValidSHA256(record.VersionReviewSHA256)) {
		return invalidBackupRuntimeRecord("Restore version review is invalid")
	}
	if record.Point.SourceKind != BackupRuntimeSourceAttach &&
		(record.TargetVersions != nil || record.VersionReviewSHA256 != "" || record.VersionDifferenceAcknowledged) {
		return invalidBackupRuntimeRecord("non-database Restore cannot contain database version review")
	}
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
	} else if record.Point.SourceKind == BackupRuntimeSourceVolume || record.Point.SourceKind == BackupRuntimeSourceAttach {
		if _, err := ulid.ParseStrict(record.RestoreGenerationID); err != nil {
			return invalidBackupRuntimeRecord("restore generation id is invalid")
		}
	} else if record.RestoreGenerationID != "" {
		return invalidBackupRuntimeRecord("non-config restore cannot carry a restore generation id")
	}
	if err := validateBackupRestoreKeyReferences(record); err != nil {
		return err
	}
	if record.Artifact != nil {
		if !validBackupArtifact(*record.Artifact) ||
			*record.Artifact != record.Point.Evidence {
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
	if pointerCount(target.Postgres != nil, target.MySQL != nil, target.Volume != nil, target.Config != nil) != 1 {
		return invalidBackupRuntimeRecord("backup restore target must contain exactly one kind")
	}
	switch point.SourceKind {
	case BackupRuntimeSourceAttach:
		switch point.SourceFormat {
		case BackupRuntimeFormatPostgres:
			if target.Postgres == nil || validateBackupRestorePostgresTarget(*target.Postgres, point) != nil {
				return invalidBackupRuntimeRecord("postgres restore target is invalid")
			}
		case BackupRuntimeFormatMySQL:
			if target.MySQL == nil || validateBackupRestoreMySQLTarget(*target.MySQL, point) != nil {
				return invalidBackupRuntimeRecord("MySQL restore target is invalid")
			}
		default:
			return invalidBackupRuntimeRecord("database restore target format is invalid")
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
			recordcodec.ValidateID(ids.KindTask, target.Config.BaselineRevisionID) != nil ||
			target.Config.BaselineHeadRevision <= 0 || target.Config.RenderGeneration == 0 ||
			!recordcodec.ValidSHA256(target.Config.FileContextSHA256) ||
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
		case BackupRestoreQueued, BackupRestoreDownloading, BackupRestoreArtifactVerified, BackupRestoreFailedSafe:
			allowed = true
		case BackupRestoreConsumersStopped:
			allowed, expectedMutation = true, record.MutationStarted
		case BackupRestoreRestoring, BackupRestoreConsumersRestored, BackupRestoreVerified,
			BackupRestoreCompleted, BackupRestoreRecoveryRequired:
			allowed, expectedMutation = true, true
		}
	case BackupRuntimeSourceVolume:
		switch record.State {
		case BackupRestoreQueued, BackupRestoreDownloading, BackupRestoreArtifactVerified, BackupRestoreFailedSafe:
			allowed = true
		case BackupRestoreConsumersStopped, BackupRestoreRestoring, BackupRestoreTreeValidated,
			BackupRestoreExchangeReady, BackupRestoreExchanged, BackupRestoreConsumersRestored, BackupRestoreVerified,
			BackupRestoreCompleted, BackupRestoreRecoveryRequired:
			allowed, expectedMutation = true, true
		}
	case BackupRuntimeSourceConfig:
		switch record.State {
		case BackupRestoreQueued, BackupRestoreDownloading, BackupRestoreArtifactVerified,
			BackupRestoreReceiving, BackupRestoreStaged, BackupRestoreFailedSafe:
			allowed = true
		case BackupRestoreApplyingDeletes, BackupRestoreApplyingUpserts,
			BackupRestoreCanonicalComplete, BackupRestoreMaterializing, BackupRestoreVerified,
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
		BackupRestoreConsumersRestored, BackupRestoreVerified, BackupRestoreCompleted:
		if record.StagedTreeManifestSHA256 == "" {
			return invalidBackupRuntimeRecord(
				"Volume restore state requires a staged-tree manifest",
			)
		}
	case BackupRestoreRecoveryRequired:
		if record.VolumeProgress != nil &&
			record.VolumeProgress.FinalizationCursor == record.Point.VolumeArchive.EntryCount &&
			record.StagedTreeManifestSHA256 == "" {
			return invalidBackupRuntimeRecord("Volume recovery lost a finalized hidden-tree manifest")
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
		return validateBackupRestoreConfigProgress(record)
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
	if record.Point.SourceKind == BackupRuntimeSourceVolume {
		return validateBackupRestoreVolumeProgress(record)
	}
	if record.VolumeProgress != nil {
		return invalidBackupRuntimeRecord("non-Volume Restore cannot carry Volume progress")
	}
	if record.Point.SourceKind == BackupRuntimeSourceAttach {
		if record.CurrentTarget.Postgres != nil {
			return validateBackupRestoreDatabaseProgress(record, len(record.CurrentTarget.Postgres.Consumers))
		}
		if record.CurrentTarget.MySQL != nil {
			return validateBackupRestoreDatabaseProgress(record, len(record.CurrentTarget.MySQL.Consumers))
		}
		return invalidBackupRuntimeRecord("database Restore target is unavailable")
	}
	if record.DatabaseProgress != nil {
		return invalidBackupRuntimeRecord("non-database Restore cannot carry database progress")
	}
	return nil
}

func validateBackupRestoreConfigProgress(record BackupRestoreRecord) error {
	progress := *record.ConfigProgress
	if progress.SourceCleanupCompleted &&
		(progress.MaterializationSHA256 == "" || (record.State != BackupRestoreVerified && record.State != BackupRestoreCompleted && record.State != BackupRestoreRecoveryRequired)) {
		return invalidBackupRuntimeRecord("config restore cleanup precedes verified materialization")
	}
	if record.State == BackupRestoreCompleted && !progress.SourceCleanupCompleted {
		return invalidBackupRuntimeRecord("completed config restore lacks source cleanup evidence")
	}
	if progress.DeleteEntryCount > 512 || progress.DeleteEntryOrdinal > progress.DeleteEntryCount ||
		progress.UpsertEntryOrdinal > record.Point.ConfigArchive.EntryCount || progress.RevisionRootRevision < 0 ||
		progress.PublishedHeadRevision < 0 ||
		(progress.RevisionRootSHA256 == "") != (progress.RevisionRootRevision == 0) ||
		(progress.RevisionRootRevision == 0 && (progress.ProjectionSHA256 != "" || progress.IdentitiesSHA256 != "" || progress.ExpectedMaterializationSHA256 != "")) ||
		(progress.RevisionRootSHA256 != "" && !recordcodec.ValidSHA256(progress.RevisionRootSHA256)) ||
		(progress.MaterializationSHA256 != "" && !recordcodec.ValidSHA256(progress.MaterializationSHA256)) {
		return invalidBackupRuntimeRecord("config restore publication progress is invalid")
	}
	prePublication := record.State == BackupRestoreQueued || record.State == BackupRestoreDownloading ||
		record.State == BackupRestoreArtifactVerified || record.State == BackupRestoreReceiving
	if prePublication {
		if progress != (BackupRestoreConfigProgress{}) {
			return invalidBackupRuntimeRecord("config restore has publication progress before staging")
		}
		return nil
	}
	if record.State == BackupRestoreFailedSafe {
		if progress.DeleteEntryOrdinal != 0 || progress.UpsertEntryOrdinal != 0 ||
			progress.PublishedHeadRevision != 0 || progress.MaterializationSHA256 != "" ||
			(progress.RevisionRootRevision == 0 && progress.DeleteEntryCount != 0) ||
			(progress.RevisionRootRevision > 0 && (!recordcodec.ValidSHA256(progress.ProjectionSHA256) ||
				!recordcodec.ValidSHA256(progress.IdentitiesSHA256) || !recordcodec.ValidSHA256(progress.ExpectedMaterializationSHA256))) {
			return invalidBackupRuntimeRecord("failed-safe config restore has live mutation progress")
		}
		return nil
	}
	if progress.RevisionRootRevision == 0 {
		return invalidBackupRuntimeRecord("config restore publication lacks its immutable revision root")
	}
	if !recordcodec.ValidSHA256(progress.ProjectionSHA256) || !recordcodec.ValidSHA256(progress.IdentitiesSHA256) ||
		!recordcodec.ValidSHA256(progress.ExpectedMaterializationSHA256) {
		return invalidBackupRuntimeRecord("config restore publication lacks its complete metadata seals")
	}
	if progress.UpsertEntryOrdinal > 0 && progress.DeleteEntryOrdinal != progress.DeleteEntryCount {
		return invalidBackupRuntimeRecord("config restore started upserts before completing deletes")
	}
	complete := progress.DeleteEntryOrdinal == progress.DeleteEntryCount &&
		progress.UpsertEntryOrdinal == record.Point.ConfigArchive.EntryCount
	if progress.PublishedHeadRevision != 0 &&
		(!complete || progress.PublishedHeadRevision <= record.CurrentTarget.Config.BaselineHeadRevision ||
			progress.PublishedHeadRevision <= progress.RevisionRootRevision) {
		return invalidBackupRuntimeRecord("config restore head switch precedes complete publication")
	}
	switch record.State {
	case BackupRestoreStaged:
		if progress.DeleteEntryOrdinal != 0 || progress.UpsertEntryOrdinal != 0 || progress.PublishedHeadRevision != 0 {
			return invalidBackupRuntimeRecord("staged config restore has live publication progress")
		}
	case BackupRestoreApplyingDeletes:
		if progress.UpsertEntryOrdinal != 0 || progress.PublishedHeadRevision != 0 {
			return invalidBackupRuntimeRecord("config restore delete phase has later publication progress")
		}
	case BackupRestoreApplyingUpserts:
		if progress.DeleteEntryOrdinal != progress.DeleteEntryCount || progress.PublishedHeadRevision != 0 {
			return invalidBackupRuntimeRecord("config restore upsert phase has contradictory progress")
		}
	case BackupRestoreCanonicalComplete, BackupRestoreMaterializing, BackupRestoreVerified, BackupRestoreCompleted:
		if !complete || progress.PublishedHeadRevision == 0 {
			return invalidBackupRuntimeRecord("config restore canonical publication is incomplete")
		}
	case BackupRestoreRecoveryRequired:
		// An interrupted live publication may be only partly deleted/upserted.
		// The durable cursor remains authoritative; recovery cannot invent completion.
	default:
		return invalidBackupRuntimeRecord("config restore publication state is invalid")
	}
	if record.State == BackupRestoreVerified || record.State == BackupRestoreCompleted {
		if progress.MaterializationSHA256 == "" ||
			progress.MaterializationSHA256 != progress.ExpectedMaterializationSHA256 {
			return invalidBackupRuntimeRecord("verified config restore lacks materialization evidence")
		}
	} else if record.State != BackupRestoreRecoveryRequired && progress.MaterializationSHA256 != "" {
		return invalidBackupRuntimeRecord("config restore has premature materialization evidence")
	}
	if progress.MaterializationSHA256 != "" && progress.PublishedHeadRevision == 0 {
		return invalidBackupRuntimeRecord("config restore materialization precedes head publication")
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
