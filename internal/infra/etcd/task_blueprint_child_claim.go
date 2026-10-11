package etcd

import (
	"context"
	"math"

	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// prepareBlueprintChildClaim binds Agent assignment to the latest head and the
// exact pending unit. An overtaken child remains queued for coordinator
// cancellation, but cannot obtain an Agent assignment.
func (repository *TaskRepository) prepareBlueprintChildClaim(
	ctx context.Context, task TaskRecord,
) ([]etcdstore.Condition, []etcdstore.Mutation, bool, error) {
	if !taskjournal.IsBlueprintChild(task.Params) {
		return nil, nil, true, nil
	}
	parentID := task.Params[taskjournal.TaskBlueprintParentParam]
	ledger, err := blueprintunits.NewRepository(repository.store)
	if err != nil {
		return nil, nil, false, err
	}
	snapshot, err := ledger.Load(ctx, task.Owner.EnvironmentID)
	if err != nil {
		return nil, nil, false, err
	}
	if snapshot.HeadTaskID != parentID {
		return nil, nil, false, nil
	}
	var execution *blueprintunits.ExecutionRecord
	for index := range snapshot.Executions {
		candidate := &snapshot.Executions[index].Record
		if candidate.PlanID == task.PlanID {
			execution = candidate
			break
		}
	}
	if execution == nil || execution.ParentTaskID != parentID || execution.TaskID != task.ID ||
		execution.EnvironmentID != task.Owner.EnvironmentID || execution.State != blueprintunits.Pending {
		return nil, nil, false, errs.New(errs.KindStateConflict, "Blueprint child has no pending execution authority")
	}
	parentKey := taskjournal.TaskStorageKey(parentID)
	parentClaimKey := taskjournal.BlueprintParentClaimKey(parentID)
	parents, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{parentKey, parentClaimKey}, Revision: snapshot.ReadRevision,
	})
	if err != nil {
		return nil, nil, false, err
	}
	if parents == nil || len(parents.Values) != 2 || parents.Values[0] == nil || parents.Values[1] == nil {
		return nil, nil, false, errs.New(errs.KindStateConflict, "Blueprint parent is not claimed")
	}
	parent, err := DecodeTaskRecord(parents.Values[0].Value)
	claimID, claimErr := idempotencyrecord.DecodeTaskReference(parents.Values[1].Value)
	if err != nil || claimErr != nil || validateBlueprintParentClaimTask(parent) != nil ||
		parent.ID != parentID || parent.Owner.EnvironmentID != task.Owner.EnvironmentID ||
		parent.Status != taskjournal.TaskStatusRunning || claimID != parentID {
		return nil, nil, false, errs.New(errs.KindInternal, "Blueprint parent claim is inconsistent")
	}
	next := *execution
	next.State = blueprintunits.Running
	next.Epoch = 1
	plan, err := blueprintunits.PrepareMutation(snapshot, nil, []blueprintunits.ExecutionChange{{
		PlanID: task.PlanID, Next: &next,
	}})
	if err != nil {
		return nil, nil, false, err
	}
	conditions := plan.Conditions()
	conditions = append(conditions,
		etcdstore.Condition{Key: parentKey, ModRevision: parents.Values[0].ModRevision},
		etcdstore.Condition{Key: parentClaimKey, ModRevision: parents.Values[1].ModRevision},
	)
	return conditions, plan.Mutations(), true, nil
}

// prepareBlueprintChildAssignmentEpoch joins assignment replacement to its
// resource claim. Recovery keeps the original claim even after supersession;
// a forward reconnect still requires the parent's current desired head.
func (repository *TaskRepository) prepareBlueprintChildAssignmentEpoch(
	ctx context.Context, task TaskRecord, epoch uint32, recovery bool,
) (blueprintunits.MutationPlan, error) {
	if !taskjournal.IsBlueprintChild(task.Params) {
		return blueprintunits.MutationPlan{}, nil
	}
	if epoch == 0 || epoch == math.MaxUint32 {
		return blueprintunits.MutationPlan{}, errs.New(
			errs.KindStateConflict,
			"Blueprint child assignment epoch is exhausted",
		)
	}
	ledger, err := blueprintunits.NewRepository(repository.store)
	if err != nil {
		return blueprintunits.MutationPlan{}, err
	}
	snapshot, err := ledger.Load(ctx, task.Owner.EnvironmentID)
	if err != nil {
		return blueprintunits.MutationPlan{}, err
	}
	for _, execution := range snapshot.Executions {
		current := execution.Record
		if current.PlanID != task.PlanID {
			continue
		}
		if current.TaskID != task.ID || current.ParentTaskID != task.Params[taskjournal.TaskBlueprintParentParam] ||
			current.Epoch != int64(
				epoch,
			) || current.State != blueprintunits.Running && current.State != blueprintunits.Draining ||
			!recovery && (current.State != blueprintunits.Running || snapshot.HeadTaskID != current.ParentTaskID) {
			return blueprintunits.MutationPlan{}, errs.New(
				errs.KindStateConflict,
				"Blueprint child assignment no longer matches its resource claim",
			)
		}
		current.Epoch++
		if recovery {
			current.State = blueprintunits.Draining
		}
		return blueprintunits.PrepareMutation(
			snapshot,
			nil,
			[]blueprintunits.ExecutionChange{{PlanID: task.PlanID, Next: &current}},
		)
	}
	return blueprintunits.MutationPlan{}, errs.New(
		errs.KindStateConflict,
		"Blueprint child resource claim is unavailable",
	)
}

// blueprintChildHeadAtRevision is only a queue-scan hint. The assignment
// transaction uses prepareBlueprintChildClaim's head and epoch compares.
func (repository *TaskRepository) blueprintChildHeadAtRevision(
	ctx context.Context, task TaskRecord, revision int64,
) (bool, error) {
	if !taskjournal.IsBlueprintChild(task.Params) {
		return true, nil
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{blueprints.EnvironmentBlueprintHeadKey(task.Owner.EnvironmentID)}, Revision: revision,
	})
	if err != nil {
		return false, err
	}
	if read == nil || len(read.Values) != 1 || read.Values[0] == nil {
		return false, errs.New(errs.KindStateConflict, "Blueprint child head is unavailable")
	}
	headID, err := idempotencyrecord.DecodeTaskReference(read.Values[0].Value)
	if err != nil {
		return false, err
	}
	return headID == task.Params[taskjournal.TaskBlueprintParentParam], nil
}
