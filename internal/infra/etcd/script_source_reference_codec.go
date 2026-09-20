package etcd

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	secretrecord "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	"io"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	coreproof "github.com/AlanD20/groundplane/internal/core/materializationproof"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	ref "github.com/AlanD20/groundplane/internal/infra/scriptsourcereference"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func validateScriptSourceReference(reference ScriptSourceReference) error {
	if ids.Validate(ids.KindOperation, reference.OperationID) != nil ||
		!validRawScriptExecutionID(reference.ScriptExecutionID) ||
		validateScriptSourceIdentity(reference.Source) != nil ||
		reference.SourceOwnerID == "" {
		return errs.New(errs.KindValidationFailed, "Script source reference is invalid")
	}
	if reference.Source.Kind == ScriptSourceSecretValue {
		if reference.SourceOwnerID != scriptSourcePlatformOwner &&
			ids.Validate(ids.KindProject, reference.SourceOwnerID) != nil {
			return errs.New(errs.KindValidationFailed, "Script Secret source owner is invalid")
		}
	} else if ids.Validate(ids.KindEnvironment, reference.SourceOwnerID) != nil {
		return errs.New(errs.KindValidationFailed, "Script source owner is invalid")
	}
	if reference.Source.Kind == ScriptSourceBody && reference.SourceOwnerID != reference.Source.EnvironmentID {
		return errs.New(errs.KindValidationFailed, "Script body source owner is invalid")
	}
	requiresDigest := reference.Source.Kind == ScriptSourceBody ||
		(reference.Source.Kind == ScriptSourceService && reference.SourceDigest != "") ||
		reference.Source.Kind == ScriptSourceRunnerSnapshot ||
		reference.Source.Kind == ScriptSourceRelease ||
		reference.Source.Kind == ScriptSourceNetwork ||
		reference.Source.Kind == ScriptSourceVolume ||
		reference.Source.Kind == ScriptSourceEntryValue ||
		reference.Source.Kind == ScriptSourceSecretValue ||
		reference.Source.Kind == ScriptSourceMaterialization
	if (requiresDigest && !validLowerSHA256(reference.SourceDigest)) ||
		(!requiresDigest && reference.SourceDigest != "") {
		return errs.New(errs.KindValidationFailed, "Script source digest is invalid")
	}
	return nil
}

func validateScriptSourceIdentity(source ScriptSourceIdentity) error {
	valid := false
	copy := ScriptSourceIdentity{Kind: source.Kind}
	switch source.Kind {
	case ScriptSourceBody:
		valid = ids.Validate(ids.KindEnvironment, source.EnvironmentID) == nil &&
			scriptrecord.ValidateScriptSetGeneration(
				scriptrecord.SetGenerationRecord{
					EnvironmentID: source.EnvironmentID,
					GenerationID:  source.ScriptSetGeneration,
				},
			) == nil &&
			ids.Validate(ids.KindScript, source.ScriptID) == nil && source.BodyGeneration > 0
		copy.EnvironmentID, copy.ScriptSetGeneration, copy.ScriptID, copy.BodyGeneration = source.EnvironmentID, source.ScriptSetGeneration, source.ScriptID, source.BodyGeneration
	case ScriptSourceRunnerSnapshot:
		valid, copy.SnapshotID = validRawScriptExecutionID(source.SnapshotID), source.SnapshotID
	case ScriptSourceService:
		valid, copy.ServiceID = ids.Validate(ids.KindService, source.ServiceID) == nil, source.ServiceID
	case ScriptSourceRelease:
		valid, copy.ReleaseID = ids.Validate(ids.KindDeployment, source.ReleaseID) == nil, source.ReleaseID
	case ScriptSourceNetwork:
		valid, copy.NetworkID = ids.Validate(ids.KindNetwork, source.NetworkID) == nil, source.NetworkID
	case ScriptSourceVolume:
		valid, copy.VolumeID = ids.Validate(ids.KindVolume, source.VolumeID) == nil, source.VolumeID
	case ScriptSourceEntryValue:
		valid = ids.Validate(ids.KindEnvEntry, source.EntryID) == nil &&
			ids.Validate(ids.KindConfig, source.ValueGenerationID) == nil
		copy.EntryID, copy.ValueGenerationID = source.EntryID, source.ValueGenerationID
	case ScriptSourceSecretValue:
		valid = ids.Validate(ids.KindSecret, source.SecretID) == nil && source.ValueGenerationID == source.SecretID
		copy.SecretID, copy.ValueGenerationID = source.SecretID, source.ValueGenerationID
	case ScriptSourceMaterialization:
		valid = ids.Validate(ids.KindConfig, source.MaterializationID) == nil && source.RenderGeneration > 0
		copy.MaterializationID, copy.RenderGeneration = source.MaterializationID, source.RenderGeneration
	}
	if !valid || copy != source {
		return errs.New(errs.KindValidationFailed, "Script source identity is invalid")
	}
	return nil
}

type storedScriptRunnerSnapshot struct {
	ExecutionID string `json:"script_execution_id"`
	SnapshotID  string `json:"snapshot_id"`
	SHA256      string `json:"sha256"`
	Payload     []byte `json:"payload"`
}

func validateScriptSourceRecord(key string, value []byte, reference ScriptSourceReference) error {
	source := reference.Source
	switch source.Kind {
	case ScriptSourceBody:
		generation, err := scriptrecord.DecodeScriptBodyGeneration(value)
		if err != nil ||
			key != scriptrecord.ScriptSetBodyGenerationKey(
				source.EnvironmentID,
				source.ScriptSetGeneration,
				source.ScriptID,
				source.BodyGeneration,
			) ||
			generation.ScriptID != source.ScriptID ||
			generation.Generation != source.BodyGeneration ||
			generation.BodySHA256 != reference.SourceDigest {
			return errs.New(errs.KindValidationFailed, "Script body source evidence is invalid")
		}
	case ScriptSourceRunnerSnapshot:
		payload, snapshotID, err := decodeScriptRunnerSnapshotSource(key, value, reference)
		if err != nil || snapshotID != source.SnapshotID || payload.SnapshotId != source.SnapshotID {
			return errs.New(errs.KindValidationFailed, "Script runner snapshot source evidence is invalid")
		}
	case ScriptSourceService:
		if strings.HasPrefix(key, scriptRunnerSnapshotPrefix) {
			payload, _, err := decodeScriptRunnerSnapshotSource(key, value, reference)
			if err != nil || payload.ServiceId != source.ServiceID {
				return errs.New(errs.KindValidationFailed, "Script Service snapshot evidence is invalid")
			}
			return nil
		}
		record, err := decodeServiceRuntimeRecord(value)
		if err != nil || reference.SourceDigest != "" ||
			key != serviceRuntimeKey(source.ServiceID) || record.ServiceID != source.ServiceID ||
			record.EnvironmentID != reference.SourceOwnerID {
			return errs.New(errs.KindValidationFailed, "Script Service source evidence is invalid")
		}
	case ScriptSourceRelease:
		record, err := decodeReleaseRecord[domain.Intent](value, "release-intent")
		digest := sha256.Sum256(value)
		if err != nil || domain.ValidateIntent(record) != nil ||
			(key != releaseIntentStagingKey("", source.ReleaseID) &&
				!(strings.HasPrefix(key, releaseStagingPrefix) && strings.HasSuffix(key, "/"+source.ReleaseID))) ||
			record.ID != source.ReleaseID ||
			record.EnvironmentID != reference.SourceOwnerID || hex.EncodeToString(digest[:]) != reference.SourceDigest {
			return errs.New(errs.KindValidationFailed, "Script Release source evidence is invalid")
		}
	case ScriptSourceNetwork, ScriptSourceVolume:
		payload, _, err := decodeScriptRunnerSnapshotSource(key, value, reference)
		if err != nil || !runnerSnapshotContainsScriptSource(payload, source) {
			return errs.New(errs.KindValidationFailed, "Script runner snapshot membership evidence is invalid")
		}
	case ScriptSourceEntryValue:
		if key == plainEntryValueGenerationKey(source.EntryID, source.ValueGenerationID) {
			record, err := decodePlainEntryValueGeneration(value)
			if err != nil || record.EnvironmentID != reference.SourceOwnerID || record.EntryID != source.EntryID ||
				record.GenerationID != source.ValueGenerationID || record.PlaintextSHA256 != reference.SourceDigest {
				return errs.New(errs.KindValidationFailed, "Script Entry source evidence is invalid")
			}
		} else if key == secretEntryValueGenerationKey(source.EntryID, source.ValueGenerationID) {
			record, err := decodeSecretEntryValueGeneration(value)
			if err != nil || record.EnvironmentID != reference.SourceOwnerID || record.EntryID != source.EntryID ||
				record.GenerationID != source.ValueGenerationID || record.CiphertextSHA256 != reference.SourceDigest {
				return errs.New(errs.KindValidationFailed, "Script Entry source evidence is invalid")
			}
		} else {
			return errs.New(errs.KindValidationFailed, "Script Entry source key is invalid")
		}
	case ScriptSourceSecretValue:
		record, err := secretrecord.DecodeEncryptedValue(value)
		if err != nil || key != secretrecord.ValueKey(source.SecretID) || record.SecretID != source.SecretID ||
			record.CiphertextSHA256 != reference.SourceDigest {
			return errs.New(errs.KindValidationFailed, "Script Secret source evidence is invalid")
		}
	case ScriptSourceMaterialization:
		proof, err := decodeScriptMaterializationProof(value)
		found := false
		if err == nil {
			for _, member := range proof.Record().Members {
				if member.MaterializationID == source.MaterializationID {
					found = true
					break
				}
			}
		}
		if err != nil || !found ||
			key != scriptSourceMaterializationRecordKey(reference.SourceOwnerID, source.RenderGeneration) ||
			proof.EnvironmentID() != reference.SourceOwnerID ||
			proof.RenderGeneration() != source.RenderGeneration ||
			proof.CanonicalSHA256() != reference.SourceDigest {
			return errs.New(errs.KindValidationFailed, "Script materialization source evidence is invalid")
		}
	}
	return nil
}

func decodeScriptRunnerSnapshotSource(
	key string,
	value []byte,
	reference ScriptSourceReference,
) (*agentpb.ResolvedRunnerSnapshot, string, error) {
	snapshot, err := recordcodec.Decode[storedScriptRunnerSnapshot](value, "script-runner-snapshot")
	digest := sha256.Sum256(snapshot.Payload)
	var payload agentpb.ResolvedRunnerSnapshot
	if err != nil || snapshot.SnapshotID == "" || key != scriptRunnerSnapshotKey(snapshot.SnapshotID) ||
		snapshot.ExecutionID != reference.ScriptExecutionID || snapshot.SHA256 != reference.SourceDigest ||
		hex.EncodeToString(digest[:]) != reference.SourceDigest || proto.Unmarshal(snapshot.Payload, &payload) != nil ||
		payload.SnapshotId != snapshot.SnapshotID || payload.ScriptExecutionId != snapshot.ExecutionID ||
		!scriptRunnerSnapshotSourceOwnerMatches(&payload, reference) {
		return nil, "", errs.New(
			errs.KindValidationFailed,
			"Script runner snapshot source evidence is invalid",
		)
	}
	return &payload, snapshot.SnapshotID, nil
}

func scriptRunnerSnapshotSourceOwnerMatches(
	snapshot *agentpb.ResolvedRunnerSnapshot,
	reference ScriptSourceReference,
) bool {
	if reference.Source.Kind != ScriptSourceNetwork {
		return snapshot.EnvironmentId == reference.SourceOwnerID
	}
	for _, network := range snapshot.Networks {
		if network != nil && network.NetworkId == reference.Source.NetworkID {
			return network.OwnerEnvironmentId == reference.SourceOwnerID
		}
	}
	return false
}

func runnerSnapshotContainsScriptSource(
	snapshot *agentpb.ResolvedRunnerSnapshot,
	source ScriptSourceIdentity,
) bool {
	if source.Kind == ScriptSourceNetwork {
		for _, network := range snapshot.Networks {
			if network != nil && network.NetworkId == source.NetworkID {
				return true
			}
		}
		return false
	}
	for _, mount := range snapshot.Mounts {
		if mount != nil && mount.SourceId == source.VolumeID {
			return true
		}
	}
	return false
}

func decodeScriptMaterializationProof(encoded []byte) (coreproof.Proof, error) {
	const magic = "GPM1"
	const header = len(magic) + 4 + sha256.Size
	if len(encoded) < header || string(encoded[:len(magic)]) != magic {
		return coreproof.Proof{}, errs.New(
			errs.KindValidationFailed,
			"Script materialization source evidence is invalid",
		)
	}
	length := binary.BigEndian.Uint32(encoded[len(magic):])
	if length == 0 || int(length) != len(encoded)-header {
		return coreproof.Proof{}, errs.New(
			errs.KindValidationFailed,
			"Script materialization source evidence is invalid",
		)
	}
	payload := encoded[header:]
	digest := sha256.Sum256(payload)
	if subtle.ConstantTimeCompare(digest[:], encoded[len(magic)+4:header]) != 1 {
		return coreproof.Proof{}, errs.New(
			errs.KindValidationFailed,
			"Script materialization source evidence is invalid",
		)
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var record coreproof.Record
	if err := decoder.Decode(&record); err != nil {
		return coreproof.Proof{}, errs.New(
			errs.KindValidationFailed,
			"Script materialization source evidence is invalid",
		)
	}
	var trailing struct{}
	if err := decoder.Decode(&trailing); err != io.EOF {
		return coreproof.Proof{}, errs.New(
			errs.KindValidationFailed,
			"Script materialization source evidence is invalid",
		)
	}
	proof, err := coreproof.Restore(record)
	if err != nil {
		return coreproof.Proof{}, errs.New(
			errs.KindValidationFailed,
			"Script materialization source evidence is invalid",
		)
	}
	canonical, err := json.Marshal(proof.Record())
	if err != nil || !bytes.Equal(canonical, payload) {
		return coreproof.Proof{}, errs.New(
			errs.KindValidationFailed,
			"Script materialization source evidence is invalid",
		)
	}
	return proof, nil
}

func scriptSourcePreparationKey(operationID string) string { return ref.PreparationKey(operationID) }
func scriptSourceRootKey(operationID string) string        { return ref.RootKey(operationID) }
func scriptSourceForwardReferenceKey(reference ScriptSourceReference) string {
	return ref.ForwardKey(reference)
}
func scriptSourceReverseReferenceKey(reference ScriptSourceReference) string {
	return ref.ReverseKey(reference)
}
func scriptSourceCountKey(source ScriptSourceIdentity) string { return ref.CountKey(source) }
func scriptSourceSuffix(source ScriptSourceIdentity) string   { return ref.SourceSuffix(source) }
func scriptSourceMaterializationRecordKey(environmentID string, renderGeneration uint64) string {
	return scriptSourceMaterializationPrefix + environmentID + "/" + fmt.Sprint(renderGeneration)
}

func decodeScriptSourceCount(value []byte) (ScriptSourceCount, error) {
	return recordcodec.Decode[ScriptSourceCount](value, "script-source-count")
}
func decodeScriptSourcePreparation(value []byte) (ScriptSourcePreparation, error) {
	return recordcodec.Decode[ScriptSourcePreparation](value, "script-source-preparation")
}
func decodeScriptOperationSourceRoot(value []byte) (ScriptOperationSourceRoot, error) {
	root, err := recordcodec.Decode[ScriptOperationSourceRoot](value, "script-operation-source-root")
	if err != nil || ids.Validate(ids.KindOperation, root.OperationID) != nil || root.MembershipCount == 0 ||
		!validLowerSHA256(root.MembershipSHA256) || root.ReleaseCursor > root.MembershipCount {
		return ScriptOperationSourceRoot{}, errs.New(
			errs.KindInternal,
			"Script operation source root is corrupt",
		)
	}
	if root.Phase == ScriptOperationSourceActive {
		if root.ReleasePath != ScriptSourceReleaseAbsent || root.ReleaseCursor != 0 ||
			(root.RetryDisposition != ScriptRetryDispositionUndecided &&
				root.RetryDisposition != ScriptRetryDispositionAvailable &&
				root.RetryDisposition != ScriptRetryDispositionTransferred) {
			return ScriptOperationSourceRoot{}, errs.New(
				errs.KindInternal,
				"Script operation source root is corrupt",
			)
		}
		return root, nil
	}
	if root.Phase != ScriptOperationSourceReleasing ||
		(root.ReleasePath == ScriptSourceReleaseNormal &&
			root.RetryDisposition != ScriptRetryDispositionForbidden &&
			root.RetryDisposition != ScriptRetryDispositionAbandoned) ||
		(root.ReleasePath == ScriptSourceReleaseRetryExpiry &&
			root.RetryDisposition != ScriptRetryDispositionExpired) ||
		(root.ReleasePath != ScriptSourceReleaseNormal &&
			root.ReleasePath != ScriptSourceReleaseRetryExpiry) {
		return ScriptOperationSourceRoot{}, errs.New(
			errs.KindInternal,
			"Script operation source root is corrupt",
		)
	}
	return root, nil
}
