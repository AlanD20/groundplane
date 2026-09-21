package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	entryvalues "github.com/AlanD20/groundplane/internal/infra/etcd/entryvalues"
	releases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	scriptexecutions "github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	scriptsourceevidence "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	secretrecord "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	sourceref "github.com/AlanD20/groundplane/internal/infra/scriptsourcereference"
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
	execution scriptexecutions.ScriptExecutionRecord,
	snapshotRevision int64,
) ([]scriptsourceevidence.ScriptSourcePreparationMember, error) {
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
	base := sourceref.Reference{
		OperationID: execution.OperationID, ScriptExecutionID: execution.ID, SourceOwnerID: execution.EnvironmentID,
	}
	body := base
	body.Source = sourceref.SourceIdentity{
		Kind: sourceref.SourceBody, EnvironmentID: execution.EnvironmentID,
		ScriptSetGeneration: sources.Script.Record.ScriptSetGeneration,
		ScriptID:            execution.ScriptID, BodyGeneration: execution.ScriptGeneration,
	}
	body.SourceModRevision, body.SourceDigest = sources.BodyGeneration.Revision, execution.BodySHA256
	members := []scriptsourceevidence.ScriptSourcePreparationMember{manualScriptExistingMember(body, scriptrecord.ScriptSetBodyGenerationKey(
		execution.EnvironmentID, body.Source.ScriptSetGeneration, execution.ScriptID, execution.ScriptGeneration,
	))}
	snapshotKey := scriptexecutions.ScriptRunnerSnapshotKey(execution.SnapshotID)
	service := base
	service.Source = sourceref.SourceIdentity{Kind: sourceref.SourceService, ServiceID: execution.ServiceID}
	service.SourceModRevision, service.SourceDigest = snapshotRevision, execution.SnapshotSHA256
	members = append(members, manualScriptExistingMember(service, snapshotKey))
	releaseKey := releases.ReleaseIntentStagingKey("", execution.ReleaseID)
	releaseValue, err := scriptexecutions.ScriptExecutionValueAt(ctx, repository.store, releaseKey, sources.Revision)
	if err != nil {
		return nil, err
	}
	releaseDigest := sha256.Sum256(releaseValue.Value)
	release := base
	release.Source = sourceref.SourceIdentity{Kind: sourceref.SourceRelease, ReleaseID: execution.ReleaseID}
	release.SourceModRevision, release.SourceDigest = releaseValue.ModRevision, hex.EncodeToString(releaseDigest[:])
	members = append(members, manualScriptExistingMember(release, releaseKey))

	snapshotReference := base
	snapshotReference.Source = sourceref.SourceIdentity{Kind: sourceref.SourceRunnerSnapshot, SnapshotID: execution.SnapshotID}
	snapshotReference.SourceModRevision, snapshotReference.SourceDigest = snapshotRevision, execution.SnapshotSHA256
	members = append(members, manualScriptExistingMember(snapshotReference, snapshotKey))
	seen := make(map[sourceref.SourceIdentity]struct{})
	for _, network := range snapshot.Networks {
		if network == nil {
			return nil, errs.New(errs.KindValidationFailed, "manual Script Network source is invalid")
		}
		reference := snapshotReference
		reference.Source = sourceref.SourceIdentity{Kind: sourceref.SourceNetwork, NetworkID: network.NetworkId}
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
		reference.Source = sourceref.SourceIdentity{Kind: sourceref.SourceVolume, VolumeID: mount.SourceId}
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
	base sourceref.Reference,
	bindings []*agentpb.ScriptRunnerEntryBinding,
) ([]scriptsourceevidence.ScriptSourcePreparationMember, error) {
	execution := sources.Script.Record.Desired.Execution
	if execution != nil {
		if err := execution.Validate(); err != nil {
			return nil, err
		}
	}
	explicit := execution != nil && execution.Mode == core.ScriptExecutionExplicit
	selected := make(map[string]entryrecord.Record)
	for _, record := range sources.DesiredProjection.Record.Entries {
		if explicit && !slices.Contains(execution.EntryIDs, record.Entry.ID) {
			continue
		}
		if !record.Entry.ExposesAll() && !slices.Contains(record.Entry.Exposure, sources.Service.Record.Desired.Name) {
			if explicit {
				return nil, errs.New(
					errs.KindValidationFailed,
					"explicit Script Entry source is not exposed to its Service",
				)
			}
			continue
		}
		if _, duplicate := selected[record.Entry.ID]; duplicate || record.EnvironmentID != base.SourceOwnerID {
			return nil, errs.New(errs.KindValidationFailed, "manual Script Entry source selection is invalid")
		}
		selected[record.Entry.ID] = record
	}
	if len(bindings) != len(selected) || explicit && len(selected) != len(execution.EntryIDs) {
		return nil, errs.New(errs.KindValidationFailed, "manual Script Entry binding coverage is incomplete")
	}
	members := make([]scriptsourceevidence.ScriptSourcePreparationMember, 0, len(bindings))
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
		key := entryvalues.PlainKey(binding.EntryId, binding.ValueGenerationId)
		if binding.Secret {
			key = entryvalues.SecretKey(binding.EntryId, binding.ValueGenerationId)
		}
		value, err := scriptexecutions.ScriptExecutionValueAt(ctx, repository.store, key, sources.Revision)
		if err != nil {
			return nil, err
		}
		reference := base
		reference.Source = sourceref.SourceIdentity{
			Kind: sourceref.SourceEntryValue, EntryID: binding.EntryId, ValueGenerationID: binding.ValueGenerationId,
		}
		reference.SourceModRevision = value.ModRevision
		if binding.Secret {
			generation, decodeErr := entryvalues.DecodeSecret(value.Value)
			if decodeErr != nil {
				return nil, decodeErr
			}
			reference.SourceDigest = generation.CiphertextSHA256
			clear(generation.Ciphertext)
		} else {
			generation, decodeErr := entryvalues.DecodePlain(value.Value)
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
		secret, err := repository.manualScriptSecretSourceMember(ctx, sources, base, record.Entry.Source.SecretRef)
		if err != nil {
			return nil, err
		}
		secretID := secret.Reference.Source.SecretID
		if _, duplicate := secrets[secretID]; duplicate {
			continue
		}
		members = append(members, secret)
		secrets[secretID] = struct{}{}
	}
	return members, nil
}

func (repository *ScriptRepository) manualScriptSecretSourceMember(
	ctx context.Context,
	sources ScriptExecutionSources,
	base sourceref.Reference,
	reference string,
) (scriptsourceevidence.ScriptSourcePreparationMember, error) {
	secrets, err := newSecretRepository(repository.store)
	if err != nil {
		return scriptsourceevidence.ScriptSourcePreparationMember{}, err
	}
	resolved, err := secrets.ResolveSecretAtRevision(ctx, sources.Project.Record.ID, reference, sources.Revision)
	if err != nil {
		return scriptsourceevidence.ScriptSourcePreparationMember{}, err
	}
	record := resolved.Record
	secretID := record.Secret.ID
	value, err := scriptexecutions.ScriptExecutionValueAt(ctx, repository.store, secretrecord.ValueKey(secretID), sources.Revision)
	if err != nil {
		return scriptsourceevidence.ScriptSourcePreparationMember{}, err
	}
	secret, err := secretrecord.DecodeEncryptedValue(value.Value)
	if err != nil {
		return scriptsourceevidence.ScriptSourcePreparationMember{}, err
	}
	defer clear(secret.Ciphertext)
	member := base
	member.Source = sourceref.SourceIdentity{
		Kind:              sourceref.SourceSecretValue,
		SecretID:          secretID,
		ValueGenerationID: secretID,
	}
	member.SourceOwnerID = scriptsourceevidence.ScriptSourcePlatformOwner
	if record.Secret.ProjectID != "" {
		member.SourceOwnerID = record.Secret.ProjectID
	}
	member.SourceModRevision, member.SourceDigest = value.ModRevision, secret.CiphertextSHA256
	return manualScriptExistingMember(member, secretrecord.ValueKey(secretID)), nil
}

func manualScriptExistingMember(reference sourceref.Reference, key string) scriptsourceevidence.ScriptSourcePreparationMember {
	return scriptsourceevidence.ScriptSourcePreparationMember{
		Reference: reference,
		Evidence:  scriptsourceevidence.ScriptSourceEvidence{Existing: &scriptsourceevidence.ScriptExistingSourceEvidence{SourceKey: key}},
	}
}
