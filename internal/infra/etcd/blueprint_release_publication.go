package etcd

import (
	"context"
	"encoding/json"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	releases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	scriptsourceevidence "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	scriptsourcepublication "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcepublication"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"

	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
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
func (publication ReleaseWorkPublication) Change(
	work workqueue.View,
	dependencies []workqueue.ItemSnapshot,
) (workqueue.Change, error) {
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
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Blueprint Release publication no longer matches its waiting work",
		)
	}
	for _, required := range []etcdstore.Condition{
		{Key: workqueue.TaskKey(task.ID), ModRevision: work.Task.Revision},
		{Key: workqueue.ItemKey(item.ID), ModRevision: work.Item.Revision},
	} {
		if !slices.Contains(publication.conditions, required) {
			return workqueue.Change{}, errs.New(
				errs.KindValidationFailed,
				"Blueprint Release publication is missing its work comparison",
			)
		}
	}
	guards, err := workqueue.DependencyConditions(item, dependencies)
	if err != nil {
		return workqueue.Change{}, err
	}
	for _, dependency := range dependencies {
		if dependency.Revision > work.ReadRevision {
			return workqueue.Change{}, errs.New(
				errs.KindStateConflict,
				"Blueprint prerequisite is newer than its captured work snapshot",
			)
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
