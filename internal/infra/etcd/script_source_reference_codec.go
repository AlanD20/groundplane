package etcd

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	entryvalues "github.com/AlanD20/groundplane/internal/infra/etcd/entryvalues"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	releases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	secretrecord "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
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

func validateScriptSourceReference(reference ref.Reference) error {
	if ids.Validate(ids.KindOperation, reference.OperationID) != nil ||
		!validRawScriptExecutionID(reference.ScriptExecutionID) ||
		validateScriptSourceIdentity(reference.Source) != nil ||
		reference.SourceOwnerID == "" {
		return errs.New(errs.KindValidationFailed, "Script source reference is invalid")
	}
	if reference.Source.Kind == ref.SourceSecretValue {
		if reference.SourceOwnerID != scriptSourcePlatformOwner &&
			ids.Validate(ids.KindProject, reference.SourceOwnerID) != nil {
			return errs.New(errs.KindValidationFailed, "Script Secret source owner is invalid")
		}
	} else if ids.Validate(ids.KindEnvironment, reference.SourceOwnerID) != nil {
		return errs.New(errs.KindValidationFailed, "Script source owner is invalid")
	}
	if reference.Source.Kind == ref.SourceBody && reference.SourceOwnerID != reference.Source.EnvironmentID {
		return errs.New(errs.KindValidationFailed, "Script body source owner is invalid")
	}
	requiresDigest := reference.Source.Kind == ref.SourceBody ||
		(reference.Source.Kind == ref.SourceService && reference.SourceDigest != "") ||
		reference.Source.Kind == ref.SourceRunnerSnapshot ||
		reference.Source.Kind == ref.SourceRelease ||
		reference.Source.Kind == ref.SourceNetwork ||
		reference.Source.Kind == ref.SourceVolume ||
		reference.Source.Kind == ref.SourceEntryValue ||
		reference.Source.Kind == ref.SourceSecretValue ||
		reference.Source.Kind == ref.SourceMaterialization
	if (requiresDigest && !validLowerSHA256(reference.SourceDigest)) ||
		(!requiresDigest && reference.SourceDigest != "") {
		return errs.New(errs.KindValidationFailed, "Script source digest is invalid")
	}
	return nil
}

func validateScriptSourceIdentity(source ref.SourceIdentity) error {
	valid := false
	copy := ref.SourceIdentity{Kind: source.Kind}
	switch source.Kind {
	case ref.SourceBody:
		valid = ids.Validate(ids.KindEnvironment, source.EnvironmentID) == nil &&
			scriptrecord.ValidateScriptSetGeneration(
				scriptrecord.SetGenerationRecord{
					EnvironmentID: source.EnvironmentID,
					GenerationID:  source.ScriptSetGeneration,
				},
			) == nil &&
			ids.Validate(ids.KindScript, source.ScriptID) == nil && source.BodyGeneration > 0
		copy.EnvironmentID, copy.ScriptSetGeneration, copy.ScriptID, copy.BodyGeneration = source.EnvironmentID, source.ScriptSetGeneration, source.ScriptID, source.BodyGeneration
	case ref.SourceRunnerSnapshot:
		valid, copy.SnapshotID = validRawScriptExecutionID(source.SnapshotID), source.SnapshotID
	case ref.SourceService:
		valid, copy.ServiceID = ids.Validate(ids.KindService, source.ServiceID) == nil, source.ServiceID
	case ref.SourceRelease:
		valid, copy.ReleaseID = ids.Validate(ids.KindDeployment, source.ReleaseID) == nil, source.ReleaseID
	case ref.SourceNetwork:
		valid, copy.NetworkID = ids.Validate(ids.KindNetwork, source.NetworkID) == nil, source.NetworkID
	case ref.SourceVolume:
		valid, copy.VolumeID = ids.Validate(ids.KindVolume, source.VolumeID) == nil, source.VolumeID
	case ref.SourceEntryValue:
		valid = ids.Validate(ids.KindEnvEntry, source.EntryID) == nil &&
			ids.Validate(ids.KindConfig, source.ValueGenerationID) == nil
		copy.EntryID, copy.ValueGenerationID = source.EntryID, source.ValueGenerationID
	case ref.SourceSecretValue:
		valid = ids.Validate(ids.KindSecret, source.SecretID) == nil && source.ValueGenerationID == source.SecretID
		copy.SecretID, copy.ValueGenerationID = source.SecretID, source.ValueGenerationID
	case ref.SourceMaterialization:
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

func validateScriptSourceRecord(key string, value []byte, reference ref.Reference) error {
	source := reference.Source
	switch source.Kind {
	case ref.SourceBody:
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
	case ref.SourceRunnerSnapshot:
		payload, snapshotID, err := decodeScriptRunnerSnapshotSource(key, value, reference)
		if err != nil || snapshotID != source.SnapshotID || payload.SnapshotId != source.SnapshotID {
			return errs.New(errs.KindValidationFailed, "Script runner snapshot source evidence is invalid")
		}
	case ref.SourceService:
		if strings.HasPrefix(key, scriptRunnerSnapshotPrefix) {
			payload, _, err := decodeScriptRunnerSnapshotSource(key, value, reference)
			if err != nil || payload.ServiceId != source.ServiceID {
				return errs.New(errs.KindValidationFailed, "Script Service snapshot evidence is invalid")
			}
			return nil
		}
		record, err := servicerecord.DecodeServiceRuntimeRecord(value)
		if err != nil || reference.SourceDigest != "" ||
			key != servicerecord.ServiceRuntimeKey(source.ServiceID) || record.ServiceID != source.ServiceID ||
			record.EnvironmentID != reference.SourceOwnerID {
			return errs.New(errs.KindValidationFailed, "Script Service source evidence is invalid")
		}
	case ref.SourceRelease:
		record, err := releases.DecodeReleaseRecord[domain.Intent](value, "release-intent")
		digest := sha256.Sum256(value)
		if err != nil || domain.ValidateIntent(record) != nil ||
			(key != releases.ReleaseIntentStagingKey("", source.ReleaseID) &&
				!(strings.HasPrefix(key, releases.ReleaseStagingPrefix) && strings.HasSuffix(key, "/"+source.ReleaseID))) ||
			record.ID != source.ReleaseID ||
			record.EnvironmentID != reference.SourceOwnerID || hex.EncodeToString(digest[:]) != reference.SourceDigest {
			return errs.New(errs.KindValidationFailed, "Script Release source evidence is invalid")
		}
	case ref.SourceNetwork, ref.SourceVolume:
		payload, _, err := decodeScriptRunnerSnapshotSource(key, value, reference)
		if err != nil || !runnerSnapshotContainsScriptSource(payload, source) {
			return errs.New(errs.KindValidationFailed, "Script runner snapshot membership evidence is invalid")
		}
	case ref.SourceEntryValue:
		if key == entryvalues.PlainKey(source.EntryID, source.ValueGenerationID) {
			record, err := entryvalues.DecodePlain(value)
			if err != nil || record.EnvironmentID != reference.SourceOwnerID || record.EntryID != source.EntryID ||
				record.GenerationID != source.ValueGenerationID || record.PlaintextSHA256 != reference.SourceDigest {
				return errs.New(errs.KindValidationFailed, "Script Entry source evidence is invalid")
			}
		} else if key == entryvalues.SecretKey(source.EntryID, source.ValueGenerationID) {
			record, err := entryvalues.DecodeSecret(value)
			if err != nil || record.EnvironmentID != reference.SourceOwnerID || record.EntryID != source.EntryID ||
				record.GenerationID != source.ValueGenerationID || record.CiphertextSHA256 != reference.SourceDigest {
				return errs.New(errs.KindValidationFailed, "Script Entry source evidence is invalid")
			}
		} else {
			return errs.New(errs.KindValidationFailed, "Script Entry source key is invalid")
		}
	case ref.SourceSecretValue:
		record, err := secretrecord.DecodeEncryptedValue(value)
		if err != nil || key != secretrecord.ValueKey(source.SecretID) || record.SecretID != source.SecretID ||
			record.CiphertextSHA256 != reference.SourceDigest {
			return errs.New(errs.KindValidationFailed, "Script Secret source evidence is invalid")
		}
	case ref.SourceMaterialization:
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
	reference ref.Reference,
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
	reference ref.Reference,
) bool {
	if reference.Source.Kind != ref.SourceNetwork {
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
	source ref.SourceIdentity,
) bool {
	if source.Kind == ref.SourceNetwork {
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
func scriptSourceForwardReferenceKey(reference ref.Reference) string {
	return ref.ForwardKey(reference)
}
func scriptSourceReverseReferenceKey(reference ref.Reference) string {
	return ref.ReverseKey(reference)
}
func scriptSourceCountKey(source ref.SourceIdentity) string { return ref.CountKey(source) }
func scriptSourceSuffix(source ref.SourceIdentity) string   { return ref.SourceSuffix(source) }
func scriptSourceMaterializationRecordKey(environmentID string, renderGeneration uint64) string {
	return scriptSourceMaterializationPrefix + environmentID + "/" + fmt.Sprint(renderGeneration)
}

func decodeScriptSourceCount(value []byte) (ref.Count, error) {
	return recordcodec.Decode[ref.Count](value, "script-source-count")
}
func decodeScriptSourcePreparation(value []byte) (ref.Preparation, error) {
	return recordcodec.Decode[ref.Preparation](value, "script-source-preparation")
}
func decodeScriptOperationSourceRoot(value []byte) (ref.OperationSourceRoot, error) {
	root, err := recordcodec.Decode[ref.OperationSourceRoot](value, "script-operation-source-root")
	if err != nil || ids.Validate(ids.KindOperation, root.OperationID) != nil || root.MembershipCount == 0 ||
		!validLowerSHA256(root.MembershipSHA256) || root.ReleaseCursor > root.MembershipCount {
		return ref.OperationSourceRoot{}, errs.New(
			errs.KindInternal,
			"Script operation source root is corrupt",
		)
	}
	if root.Phase == ScriptOperationSourceActive {
		if root.ReleasePath != ScriptSourceReleaseAbsent || root.ReleaseCursor != 0 ||
			(root.RetryDisposition != ref.RetryDispositionUndecided &&
				root.RetryDisposition != ref.RetryDispositionAvailable &&
				root.RetryDisposition != ref.RetryDispositionTransferred) {
			return ref.OperationSourceRoot{}, errs.New(
				errs.KindInternal,
				"Script operation source root is corrupt",
			)
		}
		return root, nil
	}
	if root.Phase != ScriptOperationSourceReleasing ||
		(root.ReleasePath == ScriptSourceReleaseNormal &&
			root.RetryDisposition != ref.RetryDispositionForbidden &&
			root.RetryDisposition != ref.RetryDispositionAbandoned) ||
		(root.ReleasePath == ScriptSourceReleaseRetryExpiry &&
			root.RetryDisposition != ref.RetryDispositionExpired) ||
		(root.ReleasePath != ScriptSourceReleaseNormal &&
			root.ReleasePath != ScriptSourceReleaseRetryExpiry) {
		return ref.OperationSourceRoot{}, errs.New(
			errs.KindInternal,
			"Script operation source root is corrupt",
		)
	}
	return root, nil
}
