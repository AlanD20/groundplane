package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"slices"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// manualScriptSourceMembers captures removal authority from the same fixed
// revision as the runner plan. Only the execution-owned immutable snapshot is
// prepared later; its exact revision is supplied by snapshot preparation.
func (repository *ScriptRepository) manualScriptSourceMembers(
	ctx context.Context,
	sources ScriptExecutionSources,
	execution ScriptExecutionRecord,
	snapshotRevision int64,
) ([]ScriptSourcePreparationMember, error) {
	if ctx == nil || repository == nil || repository.store == nil || sources.Revision <= 0 || snapshotRevision <= 0 {
		return nil, errs.New(errs.KindValidationFailed, "manual Script source preparation is invalid")
	}
	var snapshot agentpb.ResolvedRunnerSnapshot
	digest := sha256.Sum256(execution.Snapshot)
	if proto.Unmarshal(execution.Snapshot, &snapshot) != nil ||
		hex.EncodeToString(digest[:]) != execution.SnapshotSHA256 ||
		snapshot.SnapshotId != execution.SnapshotID || snapshot.ScriptExecutionId != execution.ID ||
		snapshot.EnvironmentId != execution.EnvironmentID || snapshot.ServiceId != execution.ServiceID ||
		snapshot.ReleaseId != execution.ReleaseID {
		return nil, errs.New(errs.KindValidationFailed, "manual Script snapshot does not match its execution")
	}
	base := ScriptSourceReference{
		OperationID: execution.OperationID, ScriptExecutionID: execution.ID, SourceOwnerID: execution.EnvironmentID,
	}
	body := base
	body.Source = ScriptSourceIdentity{
		Kind: ScriptSourceBody, EnvironmentID: execution.EnvironmentID,
		ScriptSetGeneration: sources.Script.Record.ScriptSetGeneration,
		ScriptID:            execution.ScriptID, BodyGeneration: execution.ScriptGeneration,
	}
	body.SourceModRevision, body.SourceDigest = sources.BodyGeneration.Revision, execution.BodySHA256
	members := []ScriptSourcePreparationMember{manualScriptExistingMember(body, scriptSetBodyGenerationKey(
		execution.EnvironmentID, body.Source.ScriptSetGeneration, execution.ScriptID, execution.ScriptGeneration,
	))}
	snapshotKey := scriptRunnerSnapshotKey(execution.SnapshotID)
	service := base
	service.Source = ScriptSourceIdentity{Kind: ScriptSourceService, ServiceID: execution.ServiceID}
	service.SourceModRevision, service.SourceDigest = snapshotRevision, execution.SnapshotSHA256
	members = append(members, manualScriptExistingMember(service, snapshotKey))
	releaseKey := releaseIntentStagingKey("", execution.ReleaseID)
	releaseValue, err := scriptExecutionValueAt(ctx, repository.store, releaseKey, sources.Revision)
	if err != nil {
		return nil, err
	}
	releaseDigest := sha256.Sum256(releaseValue.Value)
	release := base
	release.Source = ScriptSourceIdentity{Kind: ScriptSourceRelease, ReleaseID: execution.ReleaseID}
	release.SourceModRevision, release.SourceDigest = releaseValue.ModRevision, hex.EncodeToString(releaseDigest[:])
	members = append(members, manualScriptExistingMember(release, releaseKey))

	snapshotReference := base
	snapshotReference.Source = ScriptSourceIdentity{Kind: ScriptSourceRunnerSnapshot, SnapshotID: execution.SnapshotID}
	snapshotReference.SourceModRevision, snapshotReference.SourceDigest = snapshotRevision, execution.SnapshotSHA256
	members = append(members, manualScriptExistingMember(snapshotReference, snapshotKey))
	seen := make(map[ScriptSourceIdentity]struct{})
	for _, network := range snapshot.Networks {
		if network == nil {
			return nil, errs.New(errs.KindValidationFailed, "manual Script Network source is invalid")
		}
		reference := snapshotReference
		reference.Source = ScriptSourceIdentity{Kind: ScriptSourceNetwork, NetworkID: network.NetworkId}
		reference.SourceOwnerID = network.OwnerEnvironmentId
		if _, duplicate := seen[reference.Source]; !duplicate {
			members = append(members, manualScriptExistingMember(reference, snapshotKey))
			seen[reference.Source] = struct{}{}
		}
	}
	for _, mount := range snapshot.Mounts {
		if mount == nil {
			return nil, errs.New(errs.KindValidationFailed, "manual Script Volume source is invalid")
		}
		reference := snapshotReference
		reference.Source = ScriptSourceIdentity{Kind: ScriptSourceVolume, VolumeID: mount.SourceId}
		if _, duplicate := seen[reference.Source]; !duplicate {
			members = append(members, manualScriptExistingMember(reference, snapshotKey))
			seen[reference.Source] = struct{}{}
		}
	}
	entries, err := repository.manualScriptEntrySourceMembers(ctx, sources, base, snapshot.EntryBindings)
	if err != nil {
		return nil, err
	}
	return append(members, entries...), nil
}

func (repository *ScriptRepository) manualScriptEntrySourceMembers(
	ctx context.Context,
	sources ScriptExecutionSources,
	base ScriptSourceReference,
	bindings []*agentpb.ScriptRunnerEntryBinding,
) ([]ScriptSourcePreparationMember, error) {
	selected := make(map[string]EntryRecord)
	for _, record := range sources.DesiredProjection.Record.Entries {
		if !record.Entry.ExposesAll() && !slices.Contains(record.Entry.Exposure, sources.Service.Record.Desired.Name) {
			continue
		}
		if _, duplicate := selected[record.Entry.ID]; duplicate || record.EnvironmentID != base.SourceOwnerID {
			return nil, errs.New(errs.KindValidationFailed, "manual Script Entry source selection is invalid")
		}
		selected[record.Entry.ID] = record
	}
	if len(bindings) != len(selected) {
		return nil, errs.New(errs.KindValidationFailed, "manual Script Entry binding coverage is incomplete")
	}
	members := make([]ScriptSourcePreparationMember, 0, len(bindings))
	secrets := make(map[string]struct{})
	for _, binding := range bindings {
		if binding == nil {
			return nil, errs.New(errs.KindValidationFailed, "manual Script Entry binding is invalid")
		}
		record, found := selected[binding.EntryId]
		if !found || record.CurrentValueGenerationID != binding.ValueGenerationId ||
			record.Entry.Secret != binding.Secret {
			return nil, errs.New(errs.KindValidationFailed, "manual Script Entry binding does not match its source")
		}
		delete(selected, binding.EntryId)
		key := plainEntryValueGenerationKey(binding.EntryId, binding.ValueGenerationId)
		if binding.Secret {
			key = secretEntryValueGenerationKey(binding.EntryId, binding.ValueGenerationId)
		}
		value, err := scriptExecutionValueAt(ctx, repository.store, key, sources.Revision)
		if err != nil {
			return nil, err
		}
		reference := base
		reference.Source = ScriptSourceIdentity{
			Kind: ScriptSourceEntryValue, EntryID: binding.EntryId, ValueGenerationID: binding.ValueGenerationId,
		}
		reference.SourceModRevision = value.ModRevision
		if binding.Secret {
			generation, decodeErr := decodeSecretEntryValueGeneration(value.Value)
			if decodeErr != nil {
				return nil, decodeErr
			}
			reference.SourceDigest = generation.CiphertextSHA256
			clear(generation.Ciphertext)
		} else {
			generation, decodeErr := decodePlainEntryValueGeneration(value.Value)
			if decodeErr != nil {
				return nil, decodeErr
			}
			reference.SourceDigest = generation.PlaintextSHA256
			clear(generation.Content)
		}
		members = append(members, manualScriptExistingMember(reference, key))
		if record.Entry.Source.Kind != core.SourceSecretRef {
			continue
		}
		secretID := record.Entry.Source.SecretRef
		if _, duplicate := secrets[secretID]; duplicate {
			continue
		}
		secret, err := repository.manualScriptSecretSourceMember(ctx, sources, base, secretID)
		if err != nil {
			return nil, err
		}
		members = append(members, secret)
		secrets[secretID] = struct{}{}
	}
	return members, nil
}

func (repository *ScriptRepository) manualScriptSecretSourceMember(
	ctx context.Context,
	sources ScriptExecutionSources,
	base ScriptSourceReference,
	secretID string,
) (ScriptSourcePreparationMember, error) {
	metadata, err := scriptExecutionValueAt(ctx, repository.store, secretRecordKey(secretID), sources.Revision)
	if err != nil {
		return ScriptSourcePreparationMember{}, err
	}
	record, err := decodeSecretRecord(metadata.Value)
	if err != nil || record.Secret.ID != secretID ||
		(record.Secret.ProjectID != "" && record.Secret.ProjectID != sources.Project.Record.ID) {
		return ScriptSourcePreparationMember{}, errs.New(
			errs.KindValidationFailed,
			"manual Script Secret owner is invalid",
		)
	}
	value, err := scriptExecutionValueAt(ctx, repository.store, secretValueKey(secretID), sources.Revision)
	if err != nil {
		return ScriptSourcePreparationMember{}, err
	}
	secret, err := decodeSecretEncryptedValue(value.Value)
	if err != nil {
		return ScriptSourcePreparationMember{}, err
	}
	defer clear(secret.Ciphertext)
	reference := base
	reference.Source = ScriptSourceIdentity{
		Kind:              ScriptSourceSecretValue,
		SecretID:          secretID,
		ValueGenerationID: secretID,
	}
	reference.SourceOwnerID = scriptSourcePlatformOwner
	if record.Secret.ProjectID != "" {
		reference.SourceOwnerID = record.Secret.ProjectID
	}
	reference.SourceModRevision, reference.SourceDigest = value.ModRevision, secret.CiphertextSHA256
	return manualScriptExistingMember(reference, secretValueKey(secretID)), nil
}

func manualScriptExistingMember(reference ScriptSourceReference, key string) ScriptSourcePreparationMember {
	return ScriptSourcePreparationMember{
		Reference: reference,
		Evidence:  ScriptSourceEvidence{Existing: &ScriptExistingSourceEvidence{SourceKey: key}},
	}
}
