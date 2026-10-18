package etcd

import (
	"context"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// A first Running receipt grants one step, not the remainder of a Task. Join
// these comparisons to that event's transaction so publication or Abort and
// the start have one durable order. Denial never manufactures a Running event.
func (repository *TaskRepository) prepareBlueprintStepStart(
	ctx context.Context, task TaskRecord, assignment taskassignments.TaskAssignmentRecord,
	input taskjournal.TaskEventInput, revision int64, at time.Time,
) (taskjournal.StepStartDecision, []keyvalue.Condition, error) {
	if input.State != taskjournal.TaskEventStateRunning || input.Identity.Ordinal != 1 {
		return taskjournal.StepStartNotRequested, nil, nil
	}
	if !taskjournal.IsBlueprintChild(task.Params) ||
		assignment.ExecutionMode == taskassignments.TaskExecutionModeRecoveryOnly {
		return taskjournal.StepStartAllowed, nil, nil
	}
	recovery, recoveryConditions, err := repository.blueprintSettlementStep(
		ctx,
		task,
		assignment,
		input.Identity.StepID,
		revision,
	)
	if err != nil || recovery {
		return taskjournal.StepStartAllowed, recoveryConditions, err
	}
	ledger, err := blueprintunits.NewRepository(repository.store)
	if err != nil {
		return taskjournal.StepStartNotRequested, nil, err
	}
	snapshot, err := ledger.LoadAtRevision(ctx, task.Owner.EnvironmentID, revision)
	if err != nil {
		return taskjournal.StepStartNotRequested, nil, err
	}
	var owner *keyvalue.Versioned[blueprintunits.ExecutionRecord]
	for index := range snapshot.Executions {
		if snapshot.Executions[index].Record.PlanID == task.PlanID {
			owner = &snapshot.Executions[index]
			break
		}
	}
	if owner == nil || owner.Record.TaskID != task.ID ||
		owner.Record.ParentTaskID != task.Params[taskjournal.TaskBlueprintParentParam] ||
		owner.Record.Epoch != int64(assignment.ExecutionEpoch) {
		return taskjournal.StepStartNotRequested, nil, errs.New(
			errs.KindStateConflict,
			"Blueprint step has no matching execution claim",
		)
	}
	if owner.Record.State != blueprintunits.Running {
		return taskjournal.StepStartStop, nil, nil
	}
	keys := []string{
		taskjournal.TaskStorageKey(owner.Record.ParentTaskID),
		taskjournal.BlueprintParentAbortKey(owner.Record.ParentTaskID),
	}
	if snapshot.HeadTaskID != owner.Record.ParentTaskID {
		keys = append(keys, taskjournal.BlueprintParentAbortKey(snapshot.HeadTaskID))
	}
	parents, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return taskjournal.StepStartNotRequested, nil, err
	}
	if parents == nil || parents.ReadRevision != revision || len(parents.Values) != len(keys) ||
		parents.Values[0] == nil {
		return taskjournal.StepStartNotRequested, nil, errs.New(
			errs.KindInternal,
			"Blueprint step parent evidence is incomplete",
		)
	}
	defer keyvalue.ClearValues(parents.Values)
	parent, err := DecodeTaskRecord(parents.Values[0].Value)
	if err != nil || validateBlueprintParentClaimTask(parent) != nil || parent.ID != owner.Record.ParentTaskID ||
		parent.StartedAt == nil {
		return taskjournal.StepStartNotRequested, nil, errs.New(
			errs.KindInternal,
			"Blueprint step parent evidence is invalid",
		)
	}
	if parent.Status != taskjournal.TaskStatusRunning || parents.Values[1] != nil ||
		len(parents.Values) > 2 && parents.Values[2] != nil ||
		!at.Before(parent.StartedAt.Add(time.Duration(parent.TimeoutSeconds)*time.Second)) {
		return taskjournal.StepStartStop, nil, nil
	}
	if snapshot.Desired == nil || snapshot.Desired.Record.ParentTaskID != snapshot.HeadTaskID {
		return taskjournal.StepStartWait, nil, nil
	}
	selection, err := blueprintunits.Select(snapshot)
	if err != nil {
		return taskjournal.StepStartNotRequested, nil, err
	}
	if !slices.Contains(selection.ContinueRunning, task.PlanID) {
		return taskjournal.StepStartStop, nil, nil
	}
	conditions := []keyvalue.Condition{
		{Key: blueprints.EnvironmentBlueprintHeadKey(snapshot.EnvironmentID), ModRevision: snapshot.HeadRevision},
		{Key: blueprintunits.EpochKey(snapshot.EnvironmentID), ModRevision: snapshot.EpochRevision},
		{Key: blueprintunits.DesiredPlanKey(snapshot.EnvironmentID), ModRevision: snapshot.Desired.Revision},
		{Key: blueprintunits.ExecutionKey(snapshot.EnvironmentID, task.PlanID), ModRevision: owner.Revision},
		{Key: keys[0], ModRevision: parents.Values[0].ModRevision},
		{Key: keys[1]},
	}
	if len(keys) > 2 {
		conditions = append(conditions, keyvalue.Condition{Key: keys[2]})
	}
	return taskjournal.StepStartAllowed, conditions, nil
}

// Supersession cannot revoke the captured restoration obligation. Only step
// ids in the assignment's validated recovery descriptor bypass forward gating.
func (repository *TaskRepository) blueprintSettlementStep(
	ctx context.Context,
	task TaskRecord,
	assignment taskassignments.TaskAssignmentRecord,
	stepID string,
	revision int64,
) (bool, []keyvalue.Condition, error) {
	if assignment.RestorationAuthority != nil {
		procedure, conditions, err := repository.releaseRecoveryProcedureAtRevision(ctx, task, revision)
		if err != nil {
			return false, nil, err
		}
		steps, err := releaseRestorationStepIDs(procedure, assignment.RestorationAuthority.Candidates)
		return slices.Contains(steps, stepID), conditions, err
	}
	if assignment.ServiceAbsenceProcedureSHA256 != "" {
		procedure, _, conditions, found, err := repository.serviceAbsenceProcedureAtRevision(ctx, task, revision)
		if err != nil || !found {
			if err == nil {
				err = taskassignments.CorruptTaskAssignment()
			}
			return false, nil, err
		}
		return slices.Contains(executionplan.ServiceAbsenceRecoveryStepIDs(procedure), stepID), conditions, nil
	}
	return false, nil, nil
}
