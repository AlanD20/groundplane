package etcd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// GetBlueprintTaskRenderInput loads only candidate Release visibility state.
// Blueprint Tasks deliberately do not create a parallel ReleaseOperationHead.
func (ledger *ReleaseLedger) GetBlueprintTaskRenderInput(
	ctx context.Context,
	task TaskRecord,
) (ReleaseTaskRenderInput, error) {
	publicationID := task.Params[TaskReleasePublicationParam]
	if ctx == nil || ledger == nil || validatePublicationID(publicationID) != nil ||
		task.Type != TaskUpdate || ids.Validate(ids.KindPlan, task.PlanID) != nil {
		return ReleaseTaskRenderInput{}, errs.New(errs.KindValidationFailed, "Blueprint Release render input request is invalid")
	}
	keys := []string{releaseManifestStagingKey(publicationID), releasePublicationKey(publicationID)}
	loaded, err := ledger.store.GetMany(ctx, GetManyRequest{Keys: keys})
	if err != nil {
		return ReleaseTaskRenderInput{}, err
	}
	if loaded == nil || len(loaded.Values) != 2 || loaded.Values[0] == nil || loaded.Values[1] == nil {
		return ReleaseTaskRenderInput{}, corruptReleaseRecord()
	}
	manifest, err := decodeReleaseRecord[ReleaseStagedManifest](loaded.Values[0].Value, "release-staged-manifest")
	if err != nil || manifest.PublicationID != publicationID || manifest.OperationID != task.OperationID {
		return ReleaseTaskRenderInput{}, corruptReleaseRecord()
	}
	marker, err := decodeReleaseRecord[ReleasePublicationMarker](loaded.Values[1].Value, "release-publication")
	if err != nil || marker.PublicationID != publicationID || marker.OperationID != task.OperationID || marker.ManifestDigest != manifest.Digest {
		return ReleaseTaskRenderInput{}, corruptReleaseRecord()
	}
	memberKeys := make([]string, 0, len(manifest.Members)*2)
	for _, member := range manifest.Members {
		memberKeys = append(memberKeys,
			releaseIntentStagingKey(publicationID, member.ReleaseID),
			releaseRenderInputStagingKey(publicationID, member.ReleaseID),
		)
	}
	loadedMembers, err := ledger.store.GetMany(ctx, GetManyRequest{Keys: memberKeys, Revision: loaded.ReadRevision})
	if err != nil {
		return ReleaseTaskRenderInput{}, err
	}
	if loadedMembers == nil || loadedMembers.ReadRevision != loaded.ReadRevision || len(loadedMembers.Values) != len(memberKeys) {
		return ReleaseTaskRenderInput{}, corruptReleaseRecord()
	}
	result := ReleaseTaskRenderInput{PublicationID: publicationID, Members: make([]ReleaseTaskRenderMember, len(manifest.Members))}
	for index, reference := range manifest.Members {
		intentValue, renderValue := loadedMembers.Values[index*2], loadedMembers.Values[index*2+1]
		if intentValue == nil || renderValue == nil {
			return ReleaseTaskRenderInput{}, corruptReleaseRecord()
		}
		intent, decodeErr := decodeReleaseRecord[domain.Intent](intentValue.Value, "release-intent")
		if decodeErr != nil || domain.ValidateIntent(intent) != nil || intent.ID != reference.ReleaseID ||
			intent.ServiceID != reference.ServiceID || intent.OperationID != task.OperationID ||
			intent.OperationKind != domain.OperationBlueprintApply || intent.OriginatingTaskID != task.ID {
			return ReleaseTaskRenderInput{}, corruptReleaseRecord()
		}
		raw, decodeErr := decodeReleaseRecord[json.RawMessage](renderValue.Value, "release-render-input")
		if decodeErr != nil {
			return ReleaseTaskRenderInput{}, decodeErr
		}
		render, decodeErr := decodeReleaseRenderInput(raw)
		if decodeErr != nil || render.ReleaseID != intent.ID || render.PlanID != task.PlanID ||
			render.ArtifactID != intent.RenderInputID || render.ServiceID != intent.ServiceID ||
			render.Image != intent.Image || render.Strategy != intent.Strategy {
			return ReleaseTaskRenderInput{}, corruptReleaseRecord()
		}
		digest, _ := domain.Digest(raw)
		if digest != intent.RenderInputDigest || digest != reference.RenderDigest {
			return ReleaseTaskRenderInput{}, corruptReleaseRecord()
		}
		result.Members[index] = ReleaseTaskRenderMember{Intent: intent, Render: render}
	}
	return result, nil
}

// BlueprintReleasePublication is the opaque bounded fragment appended to the
// owning Environment desired-state transaction. Candidate records are staged
// before this fragment; the release marker makes them visible atomically with
// the one Blueprint Task and desired head.
type BlueprintReleasePublication struct {
	environmentID string
	operationID   string
	conditions    []Condition
	mutations     []Mutation
	sources       ScriptSourcePublicationFragment
	authority     *ScriptSourceReferenceAuthority
	members       []ScriptSourcePreparationMember
}

type blueprintReleasePublicationInput struct {
	EnvironmentID   string
	OperationID     string
	Conditions      []Condition
	Mutations       []Mutation
	SourceFragment  ScriptSourcePublicationFragment
	SourceAuthority *ScriptSourceReferenceAuthority
	SourceMembers   []ScriptSourcePreparationMember
}

func newBlueprintReleasePublication(input blueprintReleasePublicationInput) (BlueprintReleasePublication, error) {
	if ids.Validate(ids.KindEnvironment, input.EnvironmentID) != nil || ids.Validate(ids.KindOperation, input.OperationID) != nil {
		return BlueprintReleasePublication{}, errs.New(errs.KindValidationFailed, "Blueprint Release publication identity is invalid")
	}
	publication := BlueprintReleasePublication{
		environmentID: input.EnvironmentID, operationID: input.OperationID,
		conditions: append(append([]Condition(nil), input.Conditions...), input.SourceFragment.conditions...),
		mutations:  append(cloneBlueprintReleaseMutations(input.Mutations), cloneBlueprintReleaseMutations(input.SourceFragment.mutations)...),
		sources:    input.SourceFragment, authority: input.SourceAuthority,
		members: cloneScriptSourcePreparationMembers(input.SourceMembers),
	}
	return publication, nil
}

func (publication BlueprintReleasePublication) IsZero() bool {
	return publication.environmentID == "" && publication.operationID == "" &&
		len(publication.conditions) == 0 && len(publication.mutations) == 0
}

func (publication BlueprintReleasePublication) validate(environmentID string, task TaskRecord) error {
	if publication.IsZero() {
		if task.Params[TaskReleasePublicationParam] != "" {
			return errs.New(errs.KindValidationFailed, "Blueprint Task has no candidate Release publication")
		}
		return nil
	}
	if publication.environmentID != environmentID || publication.operationID != task.OperationID ||
		validatePublicationID(task.Params[TaskReleasePublicationParam]) != nil ||
		len(publication.conditions) == 0 || len(publication.mutations) == 0 {
		return errs.New(errs.KindValidationFailed, "Blueprint Release publication does not match its Task")
	}
	return nil
}

func (publication BlueprintReleasePublication) classify(values []*KeyValue) error {
	if len(values) != len(publication.conditions) {
		return errs.New(errs.KindInternal, "Blueprint Release compare evidence is incomplete")
	}
	return errs.New(errs.KindStateConflict, "Blueprint candidate Release publication changed")
}

// Abandon removes exactly the prepared Script source membership. Candidate
// Release staging is immutable garbage-collectable evidence and is never
// guessed at or rewritten during cleanup.
func (publication BlueprintReleasePublication) Abandon(ctx context.Context) error {
	if publication.authority == nil || len(publication.members) == 0 {
		return nil
	}
	if ctx == nil || ctx.Err() != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
	}
	return publication.authority.Abandon(ctx, publication.operationID, publication.members)
}

func (publication *BlueprintReleasePublication) Clear() {
	if publication == nil {
		return
	}
	clearMutations(publication.mutations)
	publication.sources.Clear()
	clearScriptSourcePreparationMembers(publication.members)
	*publication = BlueprintReleasePublication{}
}

type PreparedBlueprintReleaseHooks struct {
	key               string
	revision          int64
	snapshotRevisions map[string]int64
}

func blueprintReleaseHookStageKey(operationID string) string {
	return "/v1/preparations/blueprint-release-hooks/" + operationID
}

type blueprintReleaseHookStage struct {
	Schema      int
	OperationID string
	TaskID      string
	Digest      string
	Count       int
}

func (ledger *ReleaseLedger) PrepareBlueprintReleaseHooks(ctx context.Context, task TaskRecord, hooks []ReleaseHookExecutionPublication) (PreparedBlueprintReleaseHooks, error) {
	if len(hooks) == 0 {
		return PreparedBlueprintReleaseHooks{}, nil
	}
	fragment, err := prepareBlueprintReleaseHookPublicationFragment(task, hooks)
	if err != nil {
		return PreparedBlueprintReleaseHooks{}, err
	}
	defer clearReleaseHookPublicationFragment(fragment)
	hash := sha256.New()
	for _, mutation := range fragment.mutations {
		hash.Write([]byte(mutation.Key))
		hash.Write([]byte{0})
		hash.Write(mutation.Value)
	}
	stage := blueprintReleaseHookStage{Schema: 1, OperationID: task.OperationID, TaskID: task.ID, Digest: hex.EncodeToString(hash.Sum(nil)), Count: len(hooks)}
	value, err := json.Marshal(stage)
	if err != nil {
		return PreparedBlueprintReleaseHooks{}, errs.Wrap(errs.KindInternal, err)
	}
	key := blueprintReleaseHookStageKey(task.OperationID)
	read, err := ledger.store.Get(ctx, key)
	if err != nil {
		return PreparedBlueprintReleaseHooks{}, err
	}
	var revision int64
	if read == nil || read.Entry == nil {
		result, txErr := ledger.store.Transact(ctx, []Condition{{Key: key}}, []Mutation{{Type: MutationPut, Key: key, Value: value}})
		if txErr != nil {
			return PreparedBlueprintReleaseHooks{}, txErr
		}
		if result.Succeeded {
			revision = result.Revision
		} else {
			read, err = ledger.store.Get(ctx, key)
			if err != nil {
				return PreparedBlueprintReleaseHooks{}, err
			}
		}
	}
	if revision == 0 {
		if read == nil || read.Entry == nil || !bytes.Equal(read.Entry.Value, value) {
			return PreparedBlueprintReleaseHooks{}, errs.New(errs.KindStateConflict, "Blueprint hook preparation is occupied")
		}
		revision = read.Entry.ModRevision
	}
	prepared := PreparedBlueprintReleaseHooks{key: key, revision: revision, snapshotRevisions: make(map[string]int64, len(hooks))}
	for i := 0; i < len(fragment.mutations); i += 2 {
		pair := fragment.mutations[i : i+2]
		loaded, loadErr := ledger.store.GetMany(ctx, GetManyRequest{Keys: []string{pair[0].Key, pair[1].Key}})
		if loadErr != nil {
			return PreparedBlueprintReleaseHooks{}, loadErr
		}
		if loaded == nil || len(loaded.Values) != 2 {
			return PreparedBlueprintReleaseHooks{}, errs.New(errs.KindInternal, "Blueprint hook preparation evidence is incomplete")
		}
		if loaded.Values[0] == nil && loaded.Values[1] == nil {
			result, txErr := ledger.store.Transact(ctx, []Condition{{Key: key, ModRevision: revision}, {Key: pair[0].Key}, {Key: pair[1].Key}}, pair)
			if txErr != nil {
				return PreparedBlueprintReleaseHooks{}, txErr
			}
			if !result.Succeeded {
				loaded, loadErr = ledger.store.GetMany(ctx, GetManyRequest{Keys: []string{pair[0].Key, pair[1].Key}})
				if loadErr != nil {
					return PreparedBlueprintReleaseHooks{}, loadErr
				}
			} else {
				prepared.snapshotRevisions[hooks[i/2].Execution.SnapshotID] = result.Revision
				continue
			}
		}
		if loaded.Values[0] == nil || loaded.Values[1] == nil || !bytes.Equal(loaded.Values[0].Value, pair[0].Value) || !bytes.Equal(loaded.Values[1].Value, pair[1].Value) {
			return PreparedBlueprintReleaseHooks{}, errs.New(errs.KindStateConflict, "Blueprint hook preparation changed")
		}
		prepared.snapshotRevisions[hooks[i/2].Execution.SnapshotID] = loaded.Values[1].ModRevision
	}
	return prepared, nil
}

func (prepared PreparedBlueprintReleaseHooks) SnapshotRevision(id string) int64 {
	return prepared.snapshotRevisions[id]
}

type BlueprintReleasePublicationEvidence struct {
	Manifest                   VersionedReleaseManifest
	EnvironmentID              string
	Task                       TaskRecord
	CandidateReleaseDescriptor executionplan.CandidateReleaseDescriptor
	Hooks                      []ReleaseHookExecutionPublication
	PublishedAt                time.Time
	SourcePrepared             PreparedSourceSet
	SourceMembers              []ScriptSourcePreparationMember
	HookPrepared               PreparedBlueprintReleaseHooks
}

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
		members = append(members, ScriptSourcePreparationMember{Reference: snapshotReference, Evidence: ScriptSourceEvidence{Existing: &ScriptExistingSourceEvidence{SourceKey: scriptRunnerSnapshotKey(execution.SnapshotID)}}})

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

type blueprintStagedMember struct {
	Evidence  ScriptSourceEvidence
	Reference ScriptSourceReference
}

func (member blueprintStagedMember) withReference(
	reference ScriptSourceReference,
) ScriptSourcePreparationMember {
	reference.SourceModRevision = 0
	return ScriptSourcePreparationMember{
		Reference: reference,
		Evidence:  member.Evidence,
	}
}

func blueprintStagedSourceEvidence(
	authority *agentpb.ScriptSourceAuthority,
	key string,
	value []byte,
	ownerID string,
) (blueprintStagedMember, error) {
	if authority == nil || authority.Existing != nil || authority.Staged == nil {
		return blueprintStagedMember{}, errs.New(
			errs.KindValidationFailed,
			"Blueprint Script source must use staged authority",
		)
	}
	return blueprintStagedSourceEvidenceFromStage(
		authority.Staged,
		key,
		value,
		ownerID,
	)
}

func blueprintStagedSourceEvidenceFromStage(
	authority *agentpb.ScriptStagedSourceAuthority,
	key string,
	value []byte,
	ownerID string,
) (blueprintStagedMember, error) {
	digest := sha256.Sum256(value)
	if authority == nil || authority.EnvironmentId != ownerID ||
		authority.FixedReadRevision > uint64(^uint64(0)>>1) ||
		!bytes.Equal(authority.CanonicalValueSha256, digest[:]) {
		return blueprintStagedMember{}, errs.New(
			errs.KindValidationFailed,
			"Blueprint Script staged source digest changed",
		)
	}
	var canonical [sha256.Size]byte
	copy(canonical[:], authority.CanonicalValueSha256)
	return blueprintStagedMember{Evidence: ScriptSourceEvidence{
		Staged: &ScriptStagedSourceEvidence{
			SourceKey: key,
			Stage: ScriptCandidateSourceStage{
				EnvironmentID:        authority.EnvironmentId,
				RevisionID:           authority.RevisionId,
				RenderGeneration:     authority.RenderGeneration,
				FixedReadRevision:    int64(authority.FixedReadRevision),
				CanonicalValueSHA256: canonical,
			},
			Value: append([]byte(nil), value...),
		},
	}}, nil
}

// PrepareBlueprintReleasePublication seals the release marker, indexes,
// Script checkpoints, and prepared source root into one opaque fragment.
func (ledger *ReleaseLedger) PrepareBlueprintReleasePublication(
	ctx context.Context,
	authority *ScriptSourceReferenceAuthority,
	evidence BlueprintReleasePublicationEvidence,
) (BlueprintReleasePublication, error) {
	if ctx == nil || ledger == nil || ledger.store == nil || evidence.Manifest.Revision <= 0 ||
		evidence.Manifest.Record.OperationID != evidence.Task.OperationID ||
		evidence.Manifest.Record.PublicationID != evidence.Task.Params[TaskReleasePublicationParam] ||
		evidence.Task.Owner.EnvironmentID != evidence.EnvironmentID || evidence.Task.Type != TaskUpdate ||
		evidence.PublishedAt.IsZero() || evidence.PublishedAt.Location() != time.UTC {
		return BlueprintReleasePublication{}, errs.New(errs.KindValidationFailed, "Blueprint Release publication evidence is invalid")
	}
	if _, err := validateReleaseCandidateDescriptor(evidence.CandidateReleaseDescriptor, evidence.Task, evidence.Manifest.Record); err != nil {
		return BlueprintReleasePublication{}, err
	}
	conditions := []Condition{
		{Key: releaseManifestStagingKey(evidence.Manifest.Record.PublicationID), ModRevision: evidence.Manifest.Revision},
		{Key: releasePublicationKey(evidence.Manifest.Record.PublicationID)},
	}
	publicationValue, err := encodeReleaseRecord("release-publication", ReleasePublicationMarker{
		PublicationID:              evidence.Manifest.Record.PublicationID,
		OperationID:                evidence.Manifest.Record.OperationID,
		ManifestDigest:             evidence.Manifest.Record.Digest,
		CandidateReleaseDescriptor: executionplan.CloneCandidateReleaseDescriptor(evidence.CandidateReleaseDescriptor),
		PublishedAt:                evidence.PublishedAt,
	})
	if err != nil {
		return BlueprintReleasePublication{}, err
	}
	mutations := []Mutation{{Type: MutationPut, Key: releasePublicationKey(evidence.Manifest.Record.PublicationID), Value: publicationValue}}
	for _, member := range evidence.Manifest.Record.Members {
		environmentValue, encodeErr := json.Marshal(releaseEnvironmentIndexValue{
			Schema: 1, ServiceID: member.ServiceID, PublicationID: evidence.Manifest.Record.PublicationID,
		})
		if encodeErr != nil {
			clearMutations(mutations)
			return BlueprintReleasePublication{}, errs.Wrap(errs.KindInternal, encodeErr)
		}
		serviceValue, encodeErr := json.Marshal(releaseServiceIndexValue{Schema: 1, PublicationID: evidence.Manifest.Record.PublicationID})
		if encodeErr != nil {
			clear(environmentValue)
			clearMutations(mutations)
			return BlueprintReleasePublication{}, errs.Wrap(errs.KindInternal, encodeErr)
		}
		mutations = append(mutations,
			Mutation{Type: MutationPut, Key: releaseEnvironmentIndexKey(evidence.EnvironmentID, member.ReleaseID), Value: environmentValue},
			Mutation{Type: MutationPut, Key: releaseServiceIndexKey(evidence.EnvironmentID, member.ServiceID, member.ReleaseID), Value: serviceValue},
		)
	}
	if len(evidence.Hooks) != 0 {
		if evidence.HookPrepared.key != blueprintReleaseHookStageKey(evidence.Task.OperationID) || evidence.HookPrepared.revision <= 0 {
			clearMutations(mutations)
			return BlueprintReleasePublication{}, errs.New(errs.KindValidationFailed, "Blueprint hook preparation is incomplete")
		}
		conditions = append(conditions, Condition{Key: evidence.HookPrepared.key, ModRevision: evidence.HookPrepared.revision})
		mutations = append(mutations, Mutation{Type: MutationDelete, Key: evidence.HookPrepared.key})
	}
	var sourceFragment ScriptSourcePublicationFragment
	if !evidence.SourcePrepared.IsZero() {
		if authority == nil || len(evidence.SourceMembers) == 0 {
			clearMutations(mutations)
			return BlueprintReleasePublication{}, errs.New(errs.KindValidationFailed, "Blueprint Script source publication is incomplete")
		}
		sourceFragment, err = authority.FinalPublicationFragment(ctx, evidence.SourcePrepared)
		if err != nil {
			clearMutations(mutations)
			return BlueprintReleasePublication{}, err
		}
	}
	publication, err := newBlueprintReleasePublication(blueprintReleasePublicationInput{
		EnvironmentID: evidence.EnvironmentID, OperationID: evidence.Task.OperationID,
		Conditions: conditions, Mutations: mutations, SourceFragment: sourceFragment,
		SourceAuthority: authority, SourceMembers: evidence.SourceMembers,
	})
	clearMutations(mutations)
	if err != nil {
		sourceFragment.Clear()
		return BlueprintReleasePublication{}, err
	}
	return publication, nil
}

func prepareBlueprintReleaseHookPublicationFragment(
	task TaskRecord,
	hooks []ReleaseHookExecutionPublication,
) (releaseHookPublicationFragment, error) {
	fragment := releaseHookPublicationFragment{}
	for _, hook := range hooks {
		execution := hook.Execution
		if execution.ScriptSetGeneration == "" {
			execution.ScriptSetGeneration = hook.Sources.Script.Record.ScriptSetGeneration
		}
		if validateScriptExecutionRecord(execution) != nil || execution.State != ScriptExecutionNotStarted ||
			!execution.ActiveReference || execution.CurrentTaskID != task.ID || execution.OperationID != task.OperationID ||
			execution.PlanHash != task.PlanHash || task.Params[ReleaseHookStepExecutionParam(execution.StepID)] != execution.ID {
			clearReleaseHookPublicationFragment(fragment)
			return releaseHookPublicationFragment{}, errs.New(errs.KindValidationFailed, "Blueprint release hook execution evidence is invalid")
		}
		executionValue, err := encodeEnvelope("script-execution", execution)
		if err != nil {
			clearReleaseHookPublicationFragment(fragment)
			return releaseHookPublicationFragment{}, err
		}
		snapshotValue, err := encodeBlueprintReleaseHookSnapshot(execution)
		if err != nil {
			clear(executionValue)
			clearReleaseHookPublicationFragment(fragment)
			return releaseHookPublicationFragment{}, err
		}
		fragment.conditions = append(fragment.conditions,
			Condition{Key: scriptExecutionKey(execution.ID)},
			Condition{Key: scriptRunnerSnapshotKey(execution.SnapshotID)},
		)
		fragment.mutations = append(fragment.mutations,
			Mutation{Type: MutationPut, Key: scriptExecutionKey(execution.ID), Value: executionValue},
			Mutation{Type: MutationPut, Key: scriptRunnerSnapshotKey(execution.SnapshotID), Value: snapshotValue},
		)
	}
	return fragment, nil
}

func encodeBlueprintReleaseHookSnapshot(execution ScriptExecutionRecord) ([]byte, error) {
	return encodeEnvelope("script-runner-snapshot", storedScriptRunnerSnapshot{
		ExecutionID: execution.ID, SnapshotID: execution.SnapshotID,
		SHA256: execution.SnapshotSHA256, Payload: execution.Snapshot,
	})
}

func cloneBlueprintReleaseMutations(values []Mutation) []Mutation {
	result := make([]Mutation, len(values))
	for index, value := range values {
		result[index] = value
		result[index].Value = append([]byte(nil), value.Value...)
	}
	return result
}

func cloneScriptSourcePreparationMembers(values []ScriptSourcePreparationMember) []ScriptSourcePreparationMember {
	result := make([]ScriptSourcePreparationMember, len(values))
	for index, value := range values {
		result[index] = value
		if value.Evidence.Existing != nil {
			existing := *value.Evidence.Existing
			result[index].Evidence.Existing = &existing
		}
		if value.Evidence.Staged != nil {
			staged := *value.Evidence.Staged
			staged.Value = append([]byte(nil), staged.Value...)
			result[index].Evidence.Staged = &staged
		}
	}
	return result
}

func clearScriptSourcePreparationMembers(values []ScriptSourcePreparationMember) {
	for index := range values {
		if values[index].Evidence.Staged != nil {
			clear(values[index].Evidence.Staged.Value)
		}
	}
}

var _ = domain.OperationBlueprintApply
