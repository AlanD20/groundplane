package backupruntime

import (
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/backupformat"
	"github.com/AlanD20/groundplane/internal/common/backupobject"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/s3connector"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
)

type BackupArtifactEvidence struct {
	SourceSizeBytes uint64 `json:"source_size_bytes"`
	SourceSHA256    string `json:"source_sha256"`
	StoredSizeBytes uint64 `json:"stored_size_bytes"`
	StoredSHA256    string `json:"stored_sha256"`
}

// BackupObjectTarget retains the frozen locator even when Put's outcome is
// unknown. It has no discriminator and cannot authorize exact object deletion.
type BackupObjectTarget struct {
	ConnectorID        string `json:"connector_id"`
	ConnectorPrefix    string `json:"connector_prefix,omitempty"`
	ConnectorEndpoint  string `json:"connector_endpoint"`
	ConnectorBucket    string `json:"connector_bucket"`
	ConnectorRegion    string `json:"connector_region"`
	ConnectorPathStyle bool   `json:"connector_path_style"`
	ObjectKey          string `json:"object_key"`
}

type BackupObjectDiscriminator struct {
	Kind  backupobject.DiscriminatorKind `json:"kind"`
	Value string                         `json:"value"`
}

type BackupObjectIdentity struct {
	Target        BackupObjectTarget        `json:"target"`
	Discriminator BackupObjectDiscriminator `json:"discriminator"`
}

type BackupUploadOutcomeKind string

const (
	BackupUploadPrepared BackupUploadOutcomeKind = "prepared"
	BackupUploadReturned BackupUploadOutcomeKind = "returned"
	BackupUploadUnknown  BackupUploadOutcomeKind = "unknown"
)

// Kind is a closed union. Only returned carries an immutable provider object;
// prepared and unknown carry the same sealed target and never guess identity.
type BackupUploadOutcome struct {
	Kind           BackupUploadOutcomeKind `json:"kind"`
	Target         BackupObjectTarget      `json:"target"`
	ReturnedObject BackupObjectIdentity    `json:"returned_object,omitempty"`
}

func validBackupObjectTarget(value BackupObjectTarget) bool {
	return recordcodec.ValidateID(ids.KindConnector, value.ConnectorID) == nil &&
		s3connector.ValidEndpoint(value.ConnectorEndpoint) && s3connector.ValidBucket(value.ConnectorBucket) &&
		s3connector.ValidRegion(
			value.ConnectorRegion,
		) && s3connector.ValidPrefix(value.ConnectorPrefix) && value.ObjectKey != ""
}

func validBackupObjectIdentity(value BackupObjectIdentity) bool {
	return validBackupObjectTarget(value.Target) && (backupobject.Discriminator{
		Kind: value.Discriminator.Kind, Value: value.Discriminator.Value,
	}).Validate() == nil
}

func validBackupUploadOutcome(value BackupUploadOutcome) bool {
	if !validBackupObjectTarget(value.Target) {
		return false
	}
	switch value.Kind {
	case BackupUploadPrepared, BackupUploadUnknown:
		return value.ReturnedObject == (BackupObjectIdentity{})
	case BackupUploadReturned:
		return validBackupObjectIdentity(value.ReturnedObject) && value.ReturnedObject.Target == value.Target
	default:
		return false
	}
}

func validBackupArtifactForTarget(value BackupArtifactEvidence, target BackupRecoveryPointTargetSnapshot) bool {
	if !validBackupArtifact(value) {
		return false
	}
	artifact := backupobject.Artifact{
		Key: target.ObjectKey, EnvironmentID: target.EnvironmentID, SourceID: target.SourceID, RecoveryPointID: target.ID,
		SourceFormat: backupobject.SourceFormat(
			target.SourceFormat,
		), Encryption: backupobject.Encryption(target.Encryption),
		Evidence: backupobject.Evidence{SourceSizeBytes: value.SourceSizeBytes, StoredSizeBytes: value.StoredSizeBytes},
	}
	if target.Encryption == BackupRuntimeEncryptionAge {
		if target.KeyEra <= 0 {
			return false
		}
		era := uint64(target.KeyEra)
		artifact.KeyEra = &era
		stored, err := backupformat.AgeStoredSize(value.SourceSizeBytes)
		if err != nil || stored != value.StoredSizeBytes {
			return false
		}
	}
	sourceDigest, err := hex.DecodeString(value.SourceSHA256)
	if err != nil {
		return false
	}
	storedDigest, err := hex.DecodeString(value.StoredSHA256)
	if err != nil {
		return false
	}
	copy(artifact.Evidence.SourceSHA256[:], sourceDigest)
	copy(artifact.Evidence.StoredSHA256[:], storedDigest)
	return artifact.Validate() == nil
}

func backupSourceTarget(run BackupRunRecord, source BackupRunSourceAttemptRecord) BackupRecoveryPointTargetSnapshot {
	return BackupRecoveryPointTargetSnapshot{
		ID: source.RecoveryPointID, EnvironmentID: run.EnvironmentID, SourceID: source.SourceID, SourceKind: source.Kind,
		TargetID: source.TargetID, ConnectorID: run.ConnectorID, ConnectorPrefix: run.ConnectorPrefix,
		ConnectorEndpoint: run.ConnectorEndpoint, ConnectorBucket: run.ConnectorBucket, ConnectorRegion: run.ConnectorRegion,
		ConnectorPathStyle: run.ConnectorPathStyle, ObjectKey: source.ObjectKey, SourceFormat: source.Format,
		Encryption: run.Encryption, KeyEra: run.KeyEra, Recipient: run.Recipient, CreatedAt: source.RecoveryPointCreatedAt,
	}
}

func validBackupSourceArtifactState(state BackupSourceAttemptState, phase BackupSourceAttemptPhase,
	evidence BackupArtifactEvidence, upload BackupUploadOutcome, object BackupObjectIdentity,
) bool {
	if evidence == (BackupArtifactEvidence{}) {
		return !SourceAttemptRequiresArtifact(state, phase) && upload == (BackupUploadOutcome{}) &&
			object == (BackupObjectIdentity{})
	}
	if sourceAttemptForbidsArtifact(state) || !validBackupArtifact(evidence) || !validBackupUploadOutcome(upload) {
		return false
	}
	switch phase {
	case BackupSourcePhaseStaging, BackupSourcePhaseUpload:
		return upload.Kind == BackupUploadPrepared && object == (BackupObjectIdentity{})
	case BackupSourcePhaseHeadVerification:
		return upload.Kind != BackupUploadPrepared && object == (BackupObjectIdentity{})
	case BackupSourcePhasePointCommit, BackupSourcePhaseRetention, BackupSourcePhaseCleanup:
		return upload.Kind != BackupUploadPrepared && validBackupObjectIdentity(object) &&
			object.Target == upload.Target &&
			(upload.Kind != BackupUploadReturned || object == upload.ReturnedObject)
	default:
		return false
	}
}
