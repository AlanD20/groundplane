package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// BlueprintReleaseSourceMembers derives the immutable ADR 0062 authority for
// candidate-bound Script executions. Candidate intents already exist in the
// sealed staging revision; runner snapshots are the only source bytes created
// by the final owning transaction.
func (ledger *ReleaseLedger) BlueprintReleaseSourceMembers(
	ctx context.Context,
	manifest VersionedReleaseManifest,
	hooks []ReleaseHookExecutionPublication,
) ([]ScriptSourcePreparationMember, error) {
	if ctx == nil || ledger == nil || manifest.ReadRevision <= 0 {
		return nil, errs.New(errs.KindValidationFailed, "Blueprint Script source evidence is invalid")
	}
	members := make([]ScriptSourcePreparationMember, 0, len(hooks)*8)
	for _, hook := range hooks {
		sources, execution := hook.Sources, hook.Execution
		if sources.Revision != manifest.ReadRevision ||
			execution.OperationID != manifest.Record.OperationID {
			return nil, errs.New(
				errs.KindValidationFailed,
				"Blueprint Script source evidence diverges",
			)
		}
		var snapshot agentpb.ResolvedRunnerSnapshot
		if err := proto.Unmarshal(execution.Snapshot, &snapshot); err != nil ||
			snapshot.ScriptExecutionId != execution.ID ||
			snapshot.EnvironmentId != execution.EnvironmentID ||
			snapshot.ServiceId != execution.ServiceID ||
			snapshot.ReleaseId != execution.ReleaseID {
			return nil, errs.New(
				errs.KindValidationFailed,
				"Blueprint Script runner snapshot evidence is invalid",
			)
		}
		if err := validateScriptContextSources(sources, &snapshot); err != nil {
			return nil, err
		}
		base := ScriptSourceReference{
			OperationID:       execution.OperationID,
			ScriptExecutionID: execution.ID,
			SourceOwnerID:     execution.EnvironmentID,
		}
		body := base
		body.Source = ScriptSourceIdentity{
			Kind:                ScriptSourceBody,
			EnvironmentID:       execution.EnvironmentID,
			ScriptSetGeneration: sources.Script.Record.ScriptSetGeneration,
			ScriptID:            execution.ScriptID,
			BodyGeneration:      execution.ScriptGeneration,
		}
		body.SourceModRevision = sources.BodyGeneration.Revision
		body.SourceDigest = execution.BodySHA256
		members = append(members, ScriptSourcePreparationMember{
			Reference: body,
			Evidence: ScriptSourceEvidence{Existing: &ScriptExistingSourceEvidence{
				SourceKey: scriptSetBodyGenerationKey(
					execution.EnvironmentID,
					body.Source.ScriptSetGeneration,
					execution.ScriptID,
					execution.ScriptGeneration,
				),
			}},
		})

		serviceValue, err := EncodeServiceRuntimeRecordStorage(sources.Service.Record)
		if err != nil {
			return nil, err
		}
		service := base
		service.Source = ScriptSourceIdentity{
			Kind:      ScriptSourceService,
			ServiceID: execution.ServiceID,
		}
		serviceEvidence, err := blueprintStagedSourceEvidence(
			snapshot.ServiceSource,
			serviceRuntimeKey(execution.ServiceID),
			serviceValue,
			execution.EnvironmentID,
		)
		clear(serviceValue)
		if err != nil {
			return nil, err
		}
		members = append(members, serviceEvidence.withReference(service))

		releaseKey := releaseIntentStagingKey(
			manifest.Record.PublicationID,
			execution.ReleaseID,
		)
		releaseValue, err := scriptExecutionValueAt(
			ctx,
			ledger.store,
			releaseKey,
			manifest.ReadRevision,
		)
		if err != nil {
			return nil, err
		}
		releaseDigest := sha256.Sum256(releaseValue.Value)
		release := base
		release.Source = ScriptSourceIdentity{
			Kind:      ScriptSourceRelease,
			ReleaseID: execution.ReleaseID,
		}
		release.SourceModRevision = releaseValue.ModRevision
		release.SourceDigest = hex.EncodeToString(releaseDigest[:])
		members = append(members, ScriptSourcePreparationMember{
			Reference: release,
			Evidence: ScriptSourceEvidence{Existing: &ScriptExistingSourceEvidence{
				SourceKey: releaseKey,
			}},
		})

		snapshotValue, err := encodeBlueprintReleaseHookSnapshot(execution)
		if err != nil {
			return nil, err
		}
		snapshotReference := base
		snapshotReference.Source = ScriptSourceIdentity{
			Kind:       ScriptSourceRunnerSnapshot,
			SnapshotID: execution.SnapshotID,
		}
		snapshotReference.SourceDigest = execution.SnapshotSHA256
		snapshotReference.SourceModRevision = hook.SnapshotRevision
		clear(snapshotValue)
		if hook.SnapshotRevision <= 0 {
			return nil, errs.New(errs.KindValidationFailed, "Blueprint Script runner snapshot is not prepared")
		}
		members = append(
			members,
			ScriptSourcePreparationMember{
				Reference: snapshotReference,
				Evidence: ScriptSourceEvidence{
					Existing: &ScriptExistingSourceEvidence{SourceKey: scriptRunnerSnapshotKey(execution.SnapshotID)},
				},
			},
		)

		snapshotKey := scriptRunnerSnapshotKey(execution.SnapshotID)
		seenNetworks := make(map[string]struct{}, len(snapshot.Networks))
		for _, network := range snapshot.Networks {
			if network == nil {
				return nil, errs.New(
					errs.KindValidationFailed,
					"Blueprint Script Network source is invalid",
				)
			}
			if _, duplicate := seenNetworks[network.NetworkId]; duplicate {
				continue
			}
			seenNetworks[network.NetworkId] = struct{}{}
			reference := base
			reference.SourceOwnerID = network.OwnerEnvironmentId
			reference.Source = ScriptSourceIdentity{
				Kind:      ScriptSourceNetwork,
				NetworkID: network.NetworkId,
			}
			reference.SourceModRevision = hook.SnapshotRevision
			reference.SourceDigest = execution.SnapshotSHA256
			members = append(members, ScriptSourcePreparationMember{
				Reference: reference,
				Evidence: ScriptSourceEvidence{Existing: &ScriptExistingSourceEvidence{
					SourceKey: snapshotKey,
				}},
			})
		}
		seenVolumes := make(map[string]struct{}, len(snapshot.Mounts))
		for _, mount := range snapshot.Mounts {
			if mount == nil {
				return nil, errs.New(
					errs.KindValidationFailed,
					"Blueprint Script Volume source is invalid",
				)
			}
			if _, duplicate := seenVolumes[mount.SourceId]; duplicate {
				continue
			}
			seenVolumes[mount.SourceId] = struct{}{}
			reference := base
			reference.Source = ScriptSourceIdentity{
				Kind:     ScriptSourceVolume,
				VolumeID: mount.SourceId,
			}
			reference.SourceModRevision = hook.SnapshotRevision
			reference.SourceDigest = execution.SnapshotSHA256
			members = append(members, ScriptSourcePreparationMember{
				Reference: reference,
				Evidence: ScriptSourceEvidence{Existing: &ScriptExistingSourceEvidence{
					SourceKey: snapshotKey,
				}},
			})
		}

		for _, binding := range snapshot.EntryBindings {
			key := plainEntryValueGenerationKey(
				binding.EntryId,
				binding.ValueGenerationId,
			)
			sourceDigest := ""
			if binding.Secret {
				key = secretEntryValueGenerationKey(
					binding.EntryId,
					binding.ValueGenerationId,
				)
			}
			value, readErr := scriptExecutionValueAt(
				ctx,
				ledger.store,
				key,
				manifest.ReadRevision,
			)
			if readErr != nil {
				return nil, readErr
			}
			if binding.Secret {
				generation, decodeErr := decodeSecretEntryValueGeneration(value.Value)
				if decodeErr != nil {
					return nil, decodeErr
				}
				sourceDigest = generation.CiphertextSHA256
				clear(generation.Ciphertext)
			} else {
				generation, decodeErr := decodePlainEntryValueGeneration(value.Value)
				if decodeErr != nil {
					return nil, decodeErr
				}
				sourceDigest = generation.PlaintextSHA256
				clear(generation.Content)
			}
			reference := base
			reference.Source = ScriptSourceIdentity{
				Kind:              ScriptSourceEntryValue,
				EntryID:           binding.EntryId,
				ValueGenerationID: binding.ValueGenerationId,
			}
			reference.SourceModRevision = value.ModRevision
			reference.SourceDigest = sourceDigest
			members = append(members, ScriptSourcePreparationMember{
				Reference: reference,
				Evidence: ScriptSourceEvidence{Existing: &ScriptExistingSourceEvidence{
					SourceKey: key,
				}},
			})
		}
		for _, secret := range snapshot.SecretValues {
			key := secretValueKey(secret.ValueGenerationId)
			value, readErr := scriptExecutionValueAt(
				ctx,
				ledger.store,
				key,
				manifest.ReadRevision,
			)
			if readErr != nil {
				return nil, readErr
			}
			reference := base
			reference.Source = ScriptSourceIdentity{
				Kind:              ScriptSourceSecretValue,
				SecretID:          secret.ValueGenerationId,
				ValueGenerationID: secret.ValueGenerationId,
			}
			reference.SourceOwnerID = secret.OwnerId
			reference.SourceModRevision = value.ModRevision
			reference.SourceDigest = hex.EncodeToString(secret.Digest)
			members = append(members, ScriptSourcePreparationMember{
				Reference: reference,
				Evidence: ScriptSourceEvidence{Existing: &ScriptExistingSourceEvidence{
					SourceKey: key,
				}},
			})
		}
	}
	return members, nil
}
