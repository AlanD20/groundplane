package etcd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	releases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	sourceref "github.com/AlanD20/groundplane/internal/infra/scriptsourcereference"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// GetBlueprintTaskRenderInput loads only candidate Release visibility state.
// Blueprint Tasks deliberately do not create a parallel ReleaseOperationHead.
func (ledger *ReleaseLedger) GetBlueprintTaskRenderInput(
	ctx context.Context,
	task TaskRecord,
) (ReleaseTaskRenderInput, error) {
	publicationID := task.Params[TaskReleasePublicationParam]
	if ctx == nil || ledger == nil || releases.ValidatePublicationID(publicationID) != nil ||
		task.Type != taskjournal.TaskUpdate || ids.Validate(ids.KindPlan, task.PlanID) != nil {
		return ReleaseTaskRenderInput{}, errs.New(
			errs.KindValidationFailed,
			"Blueprint Release render input request is invalid",
		)
	}
	keys := []string{releases.ReleaseManifestStagingKey(publicationID), releases.ReleasePublicationKey(publicationID)}
	loaded, err := ledger.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys})
	if err != nil {
		return ReleaseTaskRenderInput{}, err
	}
	if loaded == nil || len(loaded.Values) != 2 || loaded.Values[0] == nil || loaded.Values[1] == nil {
		return ReleaseTaskRenderInput{}, releases.CorruptReleaseRecord()
	}
	manifest, err := releases.DecodeReleaseRecord[releases.ReleaseStagedManifest](loaded.Values[0].Value, "release-staged-manifest")
	if err != nil || manifest.PublicationID != publicationID || manifest.OperationID != task.OperationID {
		return ReleaseTaskRenderInput{}, releases.CorruptReleaseRecord()
	}
	marker, err := releases.DecodeReleaseRecord[releases.ReleasePublicationMarker](loaded.Values[1].Value, "release-publication")
	if err != nil || marker.PublicationID != publicationID || marker.OperationID != task.OperationID ||
		marker.ManifestDigest != manifest.Digest {
		return ReleaseTaskRenderInput{}, releases.CorruptReleaseRecord()
	}
	memberKeys := make([]string, 0, len(manifest.Members)*2)
	for _, member := range manifest.Members {
		memberKeys = append(memberKeys,
			releases.ReleaseIntentStagingKey(publicationID, member.ReleaseID),
			releases.ReleaseRenderInputStagingKey(publicationID, member.ReleaseID),
		)
	}
	loadedMembers, err := ledger.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: memberKeys, Revision: loaded.ReadRevision})
	if err != nil {
		return ReleaseTaskRenderInput{}, err
	}
	if loadedMembers == nil || loadedMembers.ReadRevision != loaded.ReadRevision ||
		len(loadedMembers.Values) != len(memberKeys) {
		return ReleaseTaskRenderInput{}, releases.CorruptReleaseRecord()
	}
	result := ReleaseTaskRenderInput{
		PublicationID: publicationID,
		Members:       make([]ReleaseTaskRenderMember, len(manifest.Members)),
	}
	for index, reference := range manifest.Members {
		intentValue, renderValue := loadedMembers.Values[index*2], loadedMembers.Values[index*2+1]
		if intentValue == nil || renderValue == nil {
			return ReleaseTaskRenderInput{}, releases.CorruptReleaseRecord()
		}
		intent, decodeErr := releases.DecodeReleaseRecord[domain.Intent](intentValue.Value, "release-intent")
		if decodeErr != nil || domain.ValidateIntent(intent) != nil || intent.ID != reference.ReleaseID ||
			intent.ServiceID != reference.ServiceID || intent.OperationKind != domain.OperationBlueprintApply ||
			intent.OperationID != task.OperationID || intent.OriginatingTaskID != task.Params[blueprints.EnvironmentDesiredRevisionParam] {
			return ReleaseTaskRenderInput{}, releases.CorruptReleaseRecord()
		}
		raw, decodeErr := releases.DecodeReleaseRecord[json.RawMessage](renderValue.Value, "release-render-input")
		if decodeErr != nil {
			return ReleaseTaskRenderInput{}, decodeErr
		}
		render, decodeErr := decodeReleaseRenderInput(raw)
		if decodeErr != nil || render.ReleaseID != intent.ID || render.PlanID != task.PlanID ||
			render.ArtifactID != intent.RenderInputID || render.ServiceID != intent.ServiceID ||
			render.CandidateWorkload != intent.CandidateWorkload || render.Strategy != intent.Strategy {
			return ReleaseTaskRenderInput{}, releases.CorruptReleaseRecord()
		}
		digest, _ := domain.Digest(raw)
		if digest != intent.RenderInputDigest || digest != reference.RenderDigest {
			return ReleaseTaskRenderInput{}, releases.CorruptReleaseRecord()
		}
		result.Members[index] = ReleaseTaskRenderMember{Intent: intent, Render: render}
	}
	result.NativePredecessors, err = resolveBlueprintNativePredecessors(marker.NativePredecessors, result.Members)
	if err != nil {
		return ReleaseTaskRenderInput{}, err
	}
	return result, validateBlueprintNativeRenderInput(result, marker, task)
}

// BlueprintReleasePublication is the opaque bounded fragment appended to the
// owning Environment desired-state transaction. Candidate records are staged
// before this fragment; the release marker makes them visible atomically with
// the one Blueprint Task and desired head.
type BlueprintReleasePublication struct {
	environmentID string
	operationID   string
	conditions    []etcdstore.Condition
	mutations     []etcdstore.Mutation
	sources       ScriptSourcePublicationFragment
	authority     *ScriptSourceReferenceAuthority
	members       []ScriptSourcePreparationMember
	retained      *blueprintRuntimeRetention
}

type blueprintReleasePublicationInput struct {
	EnvironmentID   string
	OperationID     string
	Conditions      []etcdstore.Condition
	Mutations       []etcdstore.Mutation
	SourceFragment  ScriptSourcePublicationFragment
	SourceAuthority *ScriptSourceReferenceAuthority
	SourceMembers   []ScriptSourcePreparationMember
}

func newBlueprintReleasePublication(input blueprintReleasePublicationInput) (BlueprintReleasePublication, error) {
	if ids.Validate(ids.KindEnvironment, input.EnvironmentID) != nil ||
		ids.Validate(ids.KindOperation, input.OperationID) != nil {
		return BlueprintReleasePublication{}, errs.New(
			errs.KindValidationFailed,
			"Blueprint Release publication identity is invalid",
		)
	}
	publication := BlueprintReleasePublication{
		environmentID: input.EnvironmentID, operationID: input.OperationID,
		conditions: append(append([]etcdstore.Condition(nil), input.Conditions...), input.SourceFragment.conditions...),
		mutations: append(
			cloneBlueprintReleaseMutations(input.Mutations),
			cloneBlueprintReleaseMutations(input.SourceFragment.mutations)...),
		sources: input.SourceFragment, authority: input.SourceAuthority,
		members: cloneScriptSourcePreparationMembers(input.SourceMembers),
	}
	return publication, nil
}

func (publication BlueprintReleasePublication) IsZero() bool {
	return publication.environmentID == "" && publication.operationID == "" &&
		len(publication.conditions) == 0 && len(publication.mutations) == 0 && publication.retained == nil
}

func (publication BlueprintReleasePublication) validate(environmentID string, task TaskRecord) error {
	if publication.retained != nil {
		if publication.environmentID != environmentID || publication.operationID != task.OperationID ||
			len(
				publication.retained.conditions,
			) < 2 || publication.retained.conditions[0] != (etcdstore.Condition{Key: projectionrecord.EnvironmentComposeProjectionStorageKey(environmentID), ModRevision: publication.retained.sourceRevision}) ||
			publication.retained.sourceRevision < 0 || publication.retained.sourceReadRevision <= 0 || publication.retained.sourceReadRevision < publication.retained.sourceRevision {
			return errs.New(errs.KindValidationFailed, "Blueprint retained runtime publication does not match its Task")
		}
		for _, condition := range publication.retained.conditions {
			if !slices.Contains(publication.conditions, condition) {
				return errs.New(errs.KindValidationFailed, "Blueprint retained runtime source comparison is absent")
			}
		}
		if task.Params[TaskReleasePublicationParam] == "" {
			if len(publication.mutations) != 0 ||
				!slices.Equal(publication.conditions, publication.retained.conditions) ||
				publication.authority != nil ||
				len(publication.members) != 0 ||
				len(publication.sources.conditions) != 0 ||
				len(publication.sources.mutations) != 0 {
				return errs.New(
					errs.KindValidationFailed,
					"Blueprint retained runtime publication does not match its Task",
				)
			}
			return nil
		}
	}
	if publication.IsZero() {
		if task.Params[TaskReleasePublicationParam] != "" {
			return errs.New(errs.KindValidationFailed, "Blueprint Task has no candidate Release publication")
		}
		return nil
	}
	if publication.environmentID != environmentID || publication.operationID != task.OperationID ||
		releases.ValidatePublicationID(task.Params[TaskReleasePublicationParam]) != nil ||
		len(publication.conditions) == 0 || len(publication.mutations) == 0 {
		return errs.New(errs.KindValidationFailed, "Blueprint Release publication does not match its Task")
	}
	return nil
}

func (publication BlueprintReleasePublication) classify(values []*etcdstore.KeyValue) error {
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

func (ledger *ReleaseLedger) PrepareBlueprintReleaseHooks(
	ctx context.Context,
	task TaskRecord,
	hooks []ReleaseHookExecutionPublication,
) (PreparedBlueprintReleaseHooks, error) {
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
	stage := blueprintReleaseHookStage{
		Schema:      1,
		OperationID: task.OperationID,
		TaskID:      task.ID,
		Digest:      hex.EncodeToString(hash.Sum(nil)),
		Count:       len(hooks),
	}
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
		result, txErr := ledger.store.Transact(
			ctx,
			[]etcdstore.Condition{{Key: key}},
			[]etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: key, Value: value}},
		)
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
			return PreparedBlueprintReleaseHooks{}, errs.New(
				errs.KindStateConflict,
				"Blueprint hook preparation is occupied",
			)
		}
		revision = read.Entry.ModRevision
	}
	prepared := PreparedBlueprintReleaseHooks{
		key:               key,
		revision:          revision,
		snapshotRevisions: make(map[string]int64, len(hooks)),
	}
	for i := 0; i < len(fragment.mutations); i += 2 {
		pair := fragment.mutations[i : i+2]
		loaded, loadErr := ledger.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{pair[0].Key, pair[1].Key}})
		if loadErr != nil {
			return PreparedBlueprintReleaseHooks{}, loadErr
		}
		if loaded == nil || len(loaded.Values) != 2 {
			return PreparedBlueprintReleaseHooks{}, errs.New(
				errs.KindInternal,
				"Blueprint hook preparation evidence is incomplete",
			)
		}
		if loaded.Values[0] == nil && loaded.Values[1] == nil {
			result, txErr := ledger.store.Transact(
				ctx,
				[]etcdstore.Condition{{Key: key, ModRevision: revision}, {Key: pair[0].Key}, {Key: pair[1].Key}},
				pair,
			)
			if txErr != nil {
				return PreparedBlueprintReleaseHooks{}, txErr
			}
			if !result.Succeeded {
				loaded, loadErr = ledger.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{pair[0].Key, pair[1].Key}})
				if loadErr != nil {
					return PreparedBlueprintReleaseHooks{}, loadErr
				}
			} else {
				prepared.snapshotRevisions[hooks[i/2].Execution.SnapshotID] = result.Revision
				continue
			}
		}
		if loaded.Values[0] == nil || loaded.Values[1] == nil || !bytes.Equal(loaded.Values[0].Value, pair[0].Value) ||
			!bytes.Equal(loaded.Values[1].Value, pair[1].Value) {
			return PreparedBlueprintReleaseHooks{}, errs.New(
				errs.KindStateConflict,
				"Blueprint hook preparation changed",
			)
		}
		prepared.snapshotRevisions[hooks[i/2].Execution.SnapshotID] = loaded.Values[1].ModRevision
	}
	return prepared, nil
}

func (prepared PreparedBlueprintReleaseHooks) SnapshotRevision(id string) int64 {
	return prepared.snapshotRevisions[id]
}

type blueprintStagedMember struct {
	Evidence  ScriptSourceEvidence
	Reference sourceref.Reference
}

func (member blueprintStagedMember) withReference(
	reference sourceref.Reference,
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
		if validateStoredScriptContext(hook.Sources, execution) != nil ||
			validateScriptExecutionRecord(execution) != nil || execution.State != ScriptExecutionNotStarted ||
			!execution.ActiveReference || execution.CurrentTaskID != task.ID || execution.OperationID != task.OperationID ||
			execution.PlanHash != task.PlanHash || task.Params[ReleaseHookStepExecutionParam(execution.StepID)] != execution.ID {
			clearReleaseHookPublicationFragment(fragment)
			return releaseHookPublicationFragment{}, errs.New(
				errs.KindValidationFailed,
				"Blueprint release hook execution evidence is invalid",
			)
		}
		executionValue, err := recordcodec.Encode("script-execution", execution)
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
			etcdstore.Condition{Key: scriptExecutionKey(execution.ID)},
			etcdstore.Condition{Key: scriptRunnerSnapshotKey(execution.SnapshotID)},
		)
		fragment.mutations = append(fragment.mutations,
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: scriptExecutionKey(execution.ID), Value: executionValue},
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: scriptRunnerSnapshotKey(execution.SnapshotID), Value: snapshotValue},
		)
	}
	return fragment, nil
}

func encodeBlueprintReleaseHookSnapshot(execution ScriptExecutionRecord) ([]byte, error) {
	return recordcodec.Encode("script-runner-snapshot", storedScriptRunnerSnapshot{
		ExecutionID: execution.ID, SnapshotID: execution.SnapshotID,
		SHA256: execution.SnapshotSHA256, Payload: execution.Snapshot,
	})
}

func cloneBlueprintReleaseMutations(values []etcdstore.Mutation) []etcdstore.Mutation {
	result := make([]etcdstore.Mutation, len(values))
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
