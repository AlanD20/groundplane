package etcd

import (
	"context"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	scriptsourceevidence "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	scriptsourcepublication "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcepublication"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

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
		len(publication.conditions) == 0 && len(publication.mutations) == 0
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
