package etcd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	releases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	scriptexecutions "github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	scriptsourceevidence "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	scriptsourcepublication "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcepublication"
	scriptsourcequeries "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcequeries"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
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
	publicationID := task.Params[releaserender.TaskReleasePublicationParam]
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
	manifest, err := releases.DecodeReleaseRecord[releases.ReleaseStagedManifest](
		loaded.Values[0].Value,
		"release-staged-manifest",
	)
	if err != nil || manifest.PublicationID != publicationID || manifest.OperationID != task.OperationID {
		return ReleaseTaskRenderInput{}, releases.CorruptReleaseRecord()
	}
	marker, err := releases.DecodeReleaseRecord[releases.ReleasePublicationMarker](
		loaded.Values[1].Value,
		"release-publication",
	)
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
	loadedMembers, err := ledger.store.GetMany(
		ctx,
		etcdstore.GetManyRequest{Keys: memberKeys, Revision: loaded.ReadRevision},
	)
	if err != nil {
		return ReleaseTaskRenderInput{}, err
	}
	if loadedMembers == nil || loadedMembers.ReadRevision != loaded.ReadRevision ||
		len(loadedMembers.Values) != len(memberKeys) {
		return ReleaseTaskRenderInput{}, releases.CorruptReleaseRecord()
	}
	result := ReleaseTaskRenderInput{
		PublicationID: publicationID,
		Members:       make([]releaserender.ReleaseTaskRenderMember, len(manifest.Members)),
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
		render, decodeErr := releaserender.DecodeReleaseRenderInput(raw)
		if decodeErr != nil || render.ReleaseID != intent.ID || render.PlanID != task.PlanID ||
			render.ArtifactID != intent.RenderInputID || render.ServiceID != intent.ServiceID ||
			render.CandidateWorkload != intent.CandidateWorkload || render.Strategy != intent.Strategy {
			return ReleaseTaskRenderInput{}, releases.CorruptReleaseRecord()
		}
		digest, _ := domain.Digest(raw)
		if digest != intent.RenderInputDigest || digest != reference.RenderDigest {
			return ReleaseTaskRenderInput{}, releases.CorruptReleaseRecord()
		}
		result.Members[index] = releaserender.ReleaseTaskRenderMember{Intent: intent, Render: render}
	}
	result.NativePredecessors, err = resolveBlueprintNativePredecessors(marker.NativePredecessors, result.Members)
	if err != nil {
		return ReleaseTaskRenderInput{}, err
	}
	return result, validateBlueprintNativeRenderInput(result, marker, task)
}

// ReleaseWorkPublication is the opaque bounded fragment appended to the
// owning Environment desired-state transaction. Candidate records are staged
// before this fragment; the release marker makes them visible atomically with
// the one Blueprint Task and desired head.
type ReleaseWorkPublication struct {
	taskID        string
	workItemID    string
	taskRevision  int64
	itemRevision  int64
	input         workexecution.Input
	environmentID string
	operationID   string
	conditions    []etcdstore.Condition
	mutations     []etcdstore.Mutation
	sources       scriptsourcepublication.ScriptSourcePublicationFragment
	authority     *scriptsourcepublication.Authority
	members       []scriptsourceevidence.ScriptSourcePreparationMember
	requirements  *BlueprintRequirementGate
}

type releaseWorkPublicationInput struct {
	Work            workqueue.View
	Input           workexecution.Input
	EnvironmentID   string
	OperationID     string
	Conditions      []etcdstore.Condition
	Mutations       []etcdstore.Mutation
	SourceFragment  scriptsourcepublication.ScriptSourcePublicationFragment
	SourceAuthority *scriptsourcepublication.Authority
	SourceMembers   []scriptsourceevidence.ScriptSourcePreparationMember
}

func newReleaseWorkPublication(input releaseWorkPublicationInput) (ReleaseWorkPublication, error) {
	if ids.Validate(ids.KindEnvironment, input.EnvironmentID) != nil ||
		ids.Validate(ids.KindOperation, input.OperationID) != nil || input.Input.Validate() != nil ||
		input.Work.Task.Task.Validate() != nil || input.Work.Item.Item.Validate() != nil ||
		input.Work.Task.Task.Scope.EnvironmentID != input.EnvironmentID ||
		input.Work.Item.Item.TaskID != input.Work.Task.Task.ID ||
		input.Work.Task.Revision <= 0 || input.Work.Item.Revision <= 0 {
		return ReleaseWorkPublication{}, errs.New(
			errs.KindValidationFailed,
			"Blueprint Release publication identity is invalid",
		)
	}
	publication := ReleaseWorkPublication{
		taskID: input.Work.Task.Task.ID, workItemID: input.Work.Item.Item.ID,
		taskRevision: input.Work.Task.Revision, itemRevision: input.Work.Item.Revision,
		input:         input.Input,
		environmentID: input.EnvironmentID, operationID: input.OperationID,
		conditions: append(
			append([]etcdstore.Condition(nil), input.Conditions...),
			input.SourceFragment.Conditions()...),
		mutations: append(
			cloneBlueprintReleaseMutations(input.Mutations),
			cloneBlueprintReleaseMutations(input.SourceFragment.Mutations())...),
		sources: input.SourceFragment, authority: input.SourceAuthority,
		members: cloneScriptSourcePreparationMembers(input.SourceMembers),
	}
	return publication, nil
}

func (publication ReleaseWorkPublication) IsZero() bool {
	return publication.taskID == "" && publication.workItemID == "" && publication.input == (workexecution.Input{}) &&
		publication.environmentID == "" && publication.operationID == "" &&
		len(publication.conditions) == 0 && len(publication.mutations) == 0 &&
		publication.requirements == nil
}

// Change returns owned mutations for one atomic native work publication. The
// coordinator joins desired-state/epoch and resolved resource comparisons before
// committing; it cannot publish this fragment for another item or revision.
func (publication ReleaseWorkPublication) Change(work workqueue.View, dependencies []workqueue.ItemSnapshot) (workqueue.Change, error) {
	task, item := work.Task.Task, work.Item.Item
	if publication.IsZero() || task.Validate() != nil || item.Validate() != nil ||
		!releaserender.IsReleaseAction(task.Action) || item.Operation != workexecution.Release ||
		item.TaskID != task.ID || task.ID != publication.taskID || item.ID != publication.workItemID ||
		task.Scope.EnvironmentID != publication.environmentID || publication.input.Validate() != nil ||
		work.Task.Revision != publication.taskRevision || work.Item.Revision != publication.itemRevision ||
		work.ReadRevision < work.Task.Revision || work.ReadRevision < work.Item.Revision ||
		publication.requirements != nil ||
		task.Stop != nil || task.FinishedAt != nil || item.AttemptNumber != 0 || item.ForwardRevoked ||
		len(publication.conditions) == 0 || len(publication.mutations) == 0 {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint Release publication no longer matches its waiting work")
	}
	for _, required := range []etcdstore.Condition{
		{Key: workqueue.TaskKey(task.ID), ModRevision: work.Task.Revision},
		{Key: workqueue.ItemKey(item.ID), ModRevision: work.Item.Revision},
	} {
		if !slices.Contains(publication.conditions, required) {
			return workqueue.Change{}, errs.New(errs.KindValidationFailed, "Blueprint Release publication is missing its work comparison")
		}
	}
	guards, err := workqueue.DependencyConditions(item, dependencies)
	if err != nil {
		return workqueue.Change{}, err
	}
	for _, dependency := range dependencies {
		if dependency.Revision > work.ReadRevision {
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint prerequisite is newer than its captured work snapshot")
		}
	}
	return workqueue.Change{
		Conditions: append(slices.Clone(publication.conditions), guards...),
		Mutations:  cloneBlueprintReleaseMutations(publication.mutations),
	}, nil
}

func (publication ReleaseWorkPublication) classify(values []*etcdstore.KeyValue) error {
	if len(values) != len(publication.conditions) {
		return errs.New(errs.KindInternal, "Blueprint Release compare evidence is incomplete")
	}
	return errs.New(errs.KindStateConflict, "Blueprint candidate Release publication changed")
}

// Abandon removes exactly the prepared Script source membership. Candidate
// Release staging is immutable garbage-collectable evidence and is never
// guessed at or rewritten during cleanup.
func (publication ReleaseWorkPublication) Abandon(ctx context.Context) error {
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

func (publication *ReleaseWorkPublication) Clear() {
	if publication == nil {
		return
	}
	etcdstore.ZeroMutationBytes(publication.mutations)
	publication.sources.Clear()
	clearScriptSourcePreparationMembers(publication.members)
	*publication = ReleaseWorkPublication{}
}

// WithRequirementGate binds the exact resolved prerequisites to the private
// Service child publication. The gate and its Attach revision comparisons are
// committed in the same transaction as the child Task and Release authority.
func (publication ReleaseWorkPublication) WithRequirementGate(
	gate BlueprintRequirementGate,
	task TaskRecord,
	projection projectionrecord.EnvironmentComposeProjection,
) (ReleaseWorkPublication, error) {
	required := len(projection.BlueprintRequirements.Resolved) != 0
	if task.Params[taskjournal.TaskBlueprintParentParam] == "" {
		return ReleaseWorkPublication{}, errs.New(
			errs.KindValidationFailed, "Blueprint Release requirement gate Task is not a child",
		)
	}
	prepared, err := prepareBlueprintRequirementGatePublication(gate, task, projection, required)
	if err != nil {
		return ReleaseWorkPublication{}, err
	}
	if publication.IsZero() || publication.requirements != nil {
		prepared.clear()
		return ReleaseWorkPublication{}, errs.New(
			errs.KindValidationFailed, "Blueprint Release requirement gate publication is invalid",
		)
	}
	publication.conditions = append(publication.conditions, prepared.conditions...)
	publication.mutations = append(publication.mutations, prepared.mutations...)
	if required {
		sealed := gate.Clone()
		publication.requirements = &sealed
	}
	return publication, nil
}

type PreparedReleaseWorkHooks struct {
	key               string
	revision          int64
	snapshotRevisions map[string]int64
	stage             blueprintReleaseHookStage
}

func blueprintReleaseHookStageKey(workItemID string) string {
	return "/v1/preparations/blueprint-release-hooks/" + workItemID
}

type blueprintReleaseHookStage struct {
	Schema      int
	OperationID string
	TaskID      string
	WorkItemID  string
	Input       workexecution.Input
	Digest      string
	Count       int
	Snapshots   []blueprintHookSnapshot
}

type blueprintHookSnapshot struct {
	ExecutionID string
	SnapshotID  string
	SHA256      string
}

func (ledger *ReleaseLedger) PrepareReleaseWorkHooks(
	ctx context.Context,
	work workqueue.View,
	evidence ReleaseHookWorkPublication,
) (PreparedReleaseWorkHooks, error) {
	if ctx == nil || ledger == nil || ledger.store == nil {
		return PreparedReleaseWorkHooks{}, errs.New(errs.KindInternal, "Blueprint hook storage is unavailable")
	}
	if work.Task.Revision <= 0 || work.Item.Revision <= 0 || work.Task.Task.ID != evidence.Task.ID ||
		work.Item.Item.ID != evidence.Item.ID || work.Item.Item.TaskID != evidence.Task.ID ||
		work.Task.Task.Stop != nil || work.Task.Task.FinishedAt != nil || work.Task.Task.CollectionStartedAt != nil ||
		work.Item.Item.Phase == workexecution.Settled || work.Item.Item.AttemptNumber != 0 {
		return PreparedReleaseWorkHooks{}, errs.New(errs.KindStateConflict, "Blueprint hook preparation no longer owns unstarted work")
	}
	for _, hook := range evidence.Hooks {
		if hook.Execution.SourceMembershipCount != 0 || hook.Execution.SourceMembershipSHA256 != "" || hook.SnapshotRevision != 0 {
			return PreparedReleaseWorkHooks{}, errs.New(errs.KindValidationFailed, "Blueprint hook preparation requires unpublished source inputs")
		}
	}
	fragment, err := prepareReleaseWorkHookPublicationFragment(evidence)
	if err != nil {
		return PreparedReleaseWorkHooks{}, err
	}
	defer clearReleaseHookPublicationFragment(fragment)
	if len(evidence.Hooks) == 0 {
		return PreparedReleaseWorkHooks{}, nil
	}
	hash := sha256.New()
	for _, mutation := range fragment.mutations {
		hash.Write([]byte(mutation.Key))
		hash.Write([]byte{0})
		hash.Write(mutation.Value)
	}
	stage := blueprintReleaseHookStage{
		Schema:      1,
		OperationID: evidence.OperationID,
		TaskID:      evidence.Task.ID,
		WorkItemID:  evidence.Item.ID,
		Input:       *evidence.Item.Input,
		Digest:      hex.EncodeToString(hash.Sum(nil)),
		Count:       len(evidence.Hooks),
	}
	for index, hook := range evidence.Hooks {
		digest := sha256.Sum256(fragment.mutations[index*2+1].Value)
		stage.Snapshots = append(stage.Snapshots, blueprintHookSnapshot{
			ExecutionID: hook.Execution.ID, SnapshotID: hook.Execution.SnapshotID, SHA256: hex.EncodeToString(digest[:]),
		})
	}
	value, err := json.Marshal(stage)
	if err != nil {
		return PreparedReleaseWorkHooks{}, errs.Wrap(errs.KindInternal, err)
	}
	key := blueprintReleaseHookStageKey(evidence.Item.ID)
	read, err := ledger.store.Get(ctx, key)
	if err != nil {
		return PreparedReleaseWorkHooks{}, err
	}
	var revision int64
	if read == nil || read.Entry == nil {
		result, txErr := ledger.store.Transact(
			ctx,
			[]etcdstore.Condition{{Key: key},
				{Key: workqueue.TaskKey(work.Task.Task.ID), ModRevision: work.Task.Revision},
				{Key: workqueue.ItemKey(work.Item.Item.ID), ModRevision: work.Item.Revision}},
			[]etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: key, Value: value}},
		)
		if txErr != nil {
			return PreparedReleaseWorkHooks{}, txErr
		}
		if result.Succeeded {
			revision = result.Revision
		} else {
			read, err = ledger.store.Get(ctx, key)
			if err != nil {
				return PreparedReleaseWorkHooks{}, err
			}
		}
	}
	if revision == 0 {
		if read == nil || read.Entry == nil || !bytes.Equal(read.Entry.Value, value) {
			return PreparedReleaseWorkHooks{}, errs.New(
				errs.KindStateConflict,
				"Blueprint hook preparation is occupied",
			)
		}
		revision = read.Entry.ModRevision
	}
	prepared := PreparedReleaseWorkHooks{
		key:               key,
		revision:          revision,
		snapshotRevisions: make(map[string]int64, len(evidence.Hooks)),
		stage:             stage,
	}
	// Only immutable snapshots are staged. Executions become visible together
	// with the native work input and retained source root in final publication.
	for i := 1; i < len(fragment.mutations); i += 2 {
		snapshot := fragment.mutations[i]
		loaded, loadErr := ledger.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{snapshot.Key}})
		if loadErr != nil {
			return PreparedReleaseWorkHooks{}, loadErr
		}
		if loaded == nil || len(loaded.Values) != 1 {
			return PreparedReleaseWorkHooks{}, errs.New(
				errs.KindInternal,
				"Blueprint hook preparation evidence is incomplete",
			)
		}
		if loaded.Values[0] == nil {
			result, txErr := ledger.store.Transact(
				ctx,
				[]etcdstore.Condition{{Key: key, ModRevision: revision}, {Key: snapshot.Key},
					{Key: workqueue.TaskKey(work.Task.Task.ID), ModRevision: work.Task.Revision},
					{Key: workqueue.ItemKey(work.Item.Item.ID), ModRevision: work.Item.Revision}},
				[]etcdstore.Mutation{snapshot},
			)
			if txErr != nil {
				return PreparedReleaseWorkHooks{}, txErr
			}
			if !result.Succeeded {
				loaded, loadErr = ledger.store.GetMany(
					ctx,
					etcdstore.GetManyRequest{Keys: []string{snapshot.Key}},
				)
				if loadErr != nil {
					return PreparedReleaseWorkHooks{}, loadErr
				}
			} else {
				prepared.snapshotRevisions[evidence.Hooks[i/2].Execution.SnapshotID] = result.Revision
				continue
			}
		}
		if loaded == nil || len(loaded.Values) != 1 || loaded.Values[0] == nil ||
			loaded.Values[0].Key != snapshot.Key || loaded.Values[0].ModRevision <= 0 ||
			!bytes.Equal(loaded.Values[0].Value, snapshot.Value) {
			return PreparedReleaseWorkHooks{}, errs.New(
				errs.KindStateConflict,
				"Blueprint hook preparation changed",
			)
		}
		prepared.snapshotRevisions[evidence.Hooks[i/2].Execution.SnapshotID] = loaded.Values[0].ModRevision
	}
	return prepared, nil
}

func (prepared PreparedReleaseWorkHooks) SnapshotRevision(id string) int64 {
	return prepared.snapshotRevisions[id]
}

type releaseHookSourceMember struct {
	Evidence scriptsourceevidence.ScriptSourceEvidence
}

func (member releaseHookSourceMember) withReference(
	reference sourceref.Reference,
) scriptsourceevidence.ScriptSourcePreparationMember {
	reference.SourceModRevision = 0
	return scriptsourceevidence.ScriptSourcePreparationMember{
		Reference: reference,
		Evidence:  member.Evidence,
	}
}

func releaseHookSourceEvidence(
	authority *agentpb.ScriptSourceAuthority,
	key string,
	value []byte,
	ownerID string,
) (releaseHookSourceMember, error) {
	if authority == nil || authority.Existing != nil || authority.Staged == nil {
		return releaseHookSourceMember{}, errs.New(
			errs.KindValidationFailed,
			"Release hook staged source requires its captured candidate authority",
		)
	}
	return releaseHookStagedSourceEvidence(
		authority.Staged,
		key,
		value,
		ownerID,
	)
}

func releaseHookStagedSourceEvidence(
	authority *agentpb.ScriptStagedSourceAuthority,
	key string,
	value []byte,
	ownerID string,
) (releaseHookSourceMember, error) {
	digest := sha256.Sum256(value)
	if authority == nil || authority.EnvironmentId != ownerID ||
		authority.FixedReadRevision > uint64(^uint64(0)>>1) ||
		!bytes.Equal(authority.CanonicalValueSha256, digest[:]) {
		return releaseHookSourceMember{}, errs.New(
			errs.KindValidationFailed,
			"Blueprint Script staged source digest changed",
		)
	}
	var canonical [sha256.Size]byte
	copy(canonical[:], authority.CanonicalValueSha256)
	return releaseHookSourceMember{Evidence: scriptsourceevidence.ScriptSourceEvidence{
		Staged: &scriptsourceevidence.ScriptStagedSourceEvidence{
			SourceKey: key,
			Stage: scriptsourceevidence.ScriptCandidateSourceStage{
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

func prepareReleaseWorkHookPublicationFragment(
	evidence ReleaseHookWorkPublication,
) (releaseHookPublicationFragment, error) {
	fragment := releaseHookPublicationFragment{}
	if evidence.Task.Validate() != nil || evidence.Item.Validate() != nil ||
		!releaserender.IsReleaseAction(evidence.Task.Action) || evidence.Item.Operation != workexecution.Release ||
		evidence.Item.Placement.Executor != workexecution.Agent || len(evidence.Hooks) > 16 ||
		evidence.Item.TaskID != evidence.Task.ID || evidence.Item.Input == nil ||
		evidence.Item.AttemptNumber != 0 || evidence.Task.Stop != nil || evidence.Item.ForwardRevoked ||
		evidence.Task.FinishedAt != nil || evidence.ReadRevision <= 0 ||
		ids.Validate(ids.KindOperation, evidence.OperationID) != nil {
		return fragment, errs.New(errs.KindValidationFailed, "Blueprint hooks require unstarted native work")
	}
	executionIDs := make(map[string]bool, len(evidence.Hooks))
	snapshotIDs := make(map[string]bool, len(evidence.Hooks))
	for _, hook := range evidence.Hooks {
		execution := hook.Execution
		if scriptsourcequeries.ValidateStoredScriptContext(hook.Sources, execution) != nil ||
			scriptexecutions.ValidateScriptExecutionRecord(
				execution,
			) != nil || execution.State != scriptexecutions.ScriptExecutionNotStarted ||
			!execution.ActiveReference || execution.CurrentTaskID != evidence.Task.ID || execution.OperationID != evidence.OperationID ||
			execution.WorkItemID != evidence.Item.ID || execution.WorkInput == nil || *execution.WorkInput != *evidence.Item.Input ||
			execution.ServiceID != evidence.Item.TargetID || execution.EnvironmentID != evidence.Task.Scope.EnvironmentID ||
			hook.Sources.Project.Record.ID != evidence.Task.Scope.ProjectID ||
			hook.Sources.Project.Record.TenantID != evidence.Task.Scope.TenantID ||
			executionIDs[execution.ID] || snapshotIDs[execution.SnapshotID] ||
			execution.ScriptSetGeneration != hook.Sources.Script.Record.ScriptSetGeneration ||
			hook.Sources.Revision != evidence.ReadRevision {
			clearReleaseHookPublicationFragment(fragment)
			return releaseHookPublicationFragment{}, errs.New(
				errs.KindValidationFailed,
				"Blueprint release hook execution evidence is invalid",
			)
		}
		executionIDs[execution.ID] = true
		snapshotIDs[execution.SnapshotID] = true
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
			etcdstore.Condition{Key: scriptexecutions.ScriptExecutionKey(execution.ID)},
			etcdstore.Condition{Key: scriptexecutions.ScriptRunnerSnapshotKey(execution.SnapshotID)},
		)
		fragment.mutations = append(
			fragment.mutations,
			etcdstore.Mutation{
				Type:  etcdstore.MutationPut,
				Key:   scriptexecutions.ScriptExecutionKey(execution.ID),
				Value: executionValue,
			},
			etcdstore.Mutation{
				Type:  etcdstore.MutationPut,
				Key:   scriptexecutions.ScriptRunnerSnapshotKey(execution.SnapshotID),
				Value: snapshotValue,
			},
		)
	}
	return fragment, nil
}

func encodeBlueprintReleaseHookSnapshot(execution scriptexecutions.ScriptExecutionRecord) ([]byte, error) {
	return recordcodec.Encode("script-runner-snapshot", scriptsourceevidence.StoredScriptRunnerSnapshot{
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

func cloneScriptSourcePreparationMembers(
	values []scriptsourceevidence.ScriptSourcePreparationMember,
) []scriptsourceevidence.ScriptSourcePreparationMember {
	result := make([]scriptsourceevidence.ScriptSourcePreparationMember, len(values))
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

func clearScriptSourcePreparationMembers(values []scriptsourceevidence.ScriptSourcePreparationMember) {
	for index := range values {
		if values[index].Evidence.Staged != nil {
			clear(values[index].Evidence.Staged.Value)
		}
	}
}

var _ = domain.OperationBlueprintApply
