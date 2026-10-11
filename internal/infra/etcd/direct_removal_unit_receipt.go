package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// prepareDirectServiceRemovalReceipt runs only after the direct removal's exact
// intent and completed effect have been checked by terminal acknowledgement.
// The returned ledger update commits with the original Task and definition
// retirement, so a later Apply cannot repeat an already completed removal.
func (repository *TaskRepository) prepareDirectServiceRemovalReceipt(
	ctx context.Context, task TaskRecord, assignment taskassignments.TaskAssignmentRecord,
	intent environmentchanges.ServiceRemovalIntent, revision int64,
) (blueprintunits.MutationPlan, error) {
	if intent.Authored != nil || validateServiceRemovalTaskOwner(task, intent) != nil ||
		taskjournal.IsBlueprintChild(task.Params) || assignment.TaskID != task.ID ||
		assignment.Executor != taskjournal.TaskExecutorAgent || assignment.ExecutionEpoch == 0 {
		return blueprintunits.MutationPlan{}, errs.New(
			errs.KindInternal,
			"direct Service removal receipt owner is invalid",
		)
	}
	return repository.prepareDirectRemovalReceipt(ctx, task, assignment, intent.EnvironmentID,
		blueprintunits.ResourceKey{Kind: ids.KindService, ID: intent.ServiceID}, revision)
}

// The owning lifecycle validates completed effects and appends this plan to its
// same metadata-retirement transaction; this helper cannot execute a removal.
func (repository *TaskRepository) prepareDirectRemovalReceipt(
	ctx context.Context, task TaskRecord, assignment taskassignments.TaskAssignmentRecord,
	environmentID string, target blueprintunits.ResourceKey, revision int64,
) (blueprintunits.MutationPlan, error) {
	if taskjournal.IsBlueprintChild(task.Params) || task.Target != target.ID ||
		assignment.TaskID != task.ID || assignment.Executor != task.Executor ||
		(assignment.Executor != taskjournal.TaskExecutorAgent &&
			!(assignment.Executor == taskjournal.TaskExecutorController && target.Kind == ids.KindNetwork)) ||
		assignment.ExecutionEpoch == 0 {
		return blueprintunits.MutationPlan{}, errs.New(errs.KindInternal, "direct removal receipt owner is invalid")
	}
	ledger, err := blueprintunits.NewRepository(repository.store)
	if err != nil {
		return blueprintunits.MutationPlan{}, err
	}
	snapshot, err := ledger.LoadAtRevision(ctx, environmentID, revision)
	if err != nil {
		return blueprintunits.MutationPlan{}, err
	}
	return blueprintunits.PrepareNativeRemovalReceipt(snapshot, blueprintunits.NativeRemoval{
		Target: target, OperationID: task.OperationID, TaskID: task.ID, PlanID: task.PlanID,
		AssignmentID: assignment.AssignmentID, ExecutionEpoch: assignment.ExecutionEpoch,
	})
}

// Removal fragments can capture the same desired head or DNS authority. Join
// identical evidence once, rejecting disagreement rather than dropping a fence.
func appendRemovalComparisons(
	existing, incoming []etcdstore.Condition,
) ([]etcdstore.Condition, error) {
	seen := make(map[string]etcdstore.Condition, len(existing))
	for _, condition := range existing {
		seen[condition.Key] = condition
	}
	for _, condition := range incoming {
		if previous, found := seen[condition.Key]; found {
			if previous != condition {
				return nil, errs.New(errs.KindStateConflict, "removal lifecycle and receipt source revisions disagree")
			}
			continue
		}
		seen[condition.Key] = condition
		existing = append(existing, condition)
	}
	return existing, nil
}
