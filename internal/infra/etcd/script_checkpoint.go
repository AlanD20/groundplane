package etcd

import (
	"bytes"
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	scriptexecutions "github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type scriptCheckpointAnchor struct {
	record     scriptexecutions.ScriptExecutionRecord
	revision   int64
	read       int64
	conditions []etcdstore.Condition
}

func (repository *ScriptRepository) GetScriptExecution(
	ctx context.Context,
	executionID string,
) (etcdstore.Versioned[scriptexecutions.ScriptExecutionRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[scriptexecutions.ScriptExecutionRecord]{}, err
	}
	if repository == nil || repository.store == nil || !scriptexecutions.ValidRawScriptExecutionID(executionID) {
		return etcdstore.Versioned[scriptexecutions.ScriptExecutionRecord]{}, errs.New(
			errs.KindValidationFailed,
			"Script execution request is invalid",
		)
	}
	result, err := repository.store.Get(ctx, scriptexecutions.ScriptExecutionKey(executionID))
	if err != nil {
		return etcdstore.Versioned[scriptexecutions.ScriptExecutionRecord]{}, err
	}
	if result == nil || result.Entry == nil {
		return etcdstore.Versioned[scriptexecutions.ScriptExecutionRecord]{}, errs.New(
			errs.KindStateConflict,
			"Script execution record is missing",
		)
	}
	defer clear(result.Entry.Value)
	record, err := recordcodec.Decode[scriptexecutions.ScriptExecutionRecord](result.Entry.Value, "script-execution")
	if err != nil || scriptexecutions.ValidateScriptExecutionRecord(record) != nil || record.ID != executionID {
		return etcdstore.Versioned[scriptexecutions.ScriptExecutionRecord]{}, errs.New(errs.KindInternal, "Script execution record is corrupt")
	}
	return etcdstore.Versioned[scriptexecutions.ScriptExecutionRecord]{
		Record: record, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, nil
}

func (repository *ScriptRepository) CheckpointScriptExecution(
	ctx context.Context,
	input scriptexecutions.ScriptCheckpointInput,
) (etcdstore.Versioned[scriptexecutions.ScriptExecutionRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[scriptexecutions.ScriptExecutionRecord]{}, err
	}
	if repository == nil || repository.store == nil || scriptexecutions.ValidateScriptCheckpointInput(input) != nil {
		return etcdstore.Versioned[scriptexecutions.ScriptExecutionRecord]{}, errs.New(
			errs.KindValidationFailed,
			"Script checkpoint input is invalid",
		)
	}
	for attempt := 0; attempt < 2; attempt++ {
		anchor, err := repository.loadScriptCheckpointAnchor(ctx, input)
		if err != nil {
			return etcdstore.Versioned[scriptexecutions.ScriptExecutionRecord]{}, err
		}
		if anchor.record.State == input.State && anchor.record.LastCheckpointSHA256 == input.PayloadSHA256 {
			return etcdstore.Versioned[scriptexecutions.ScriptExecutionRecord]{
				Record: anchor.record, Revision: anchor.revision, ReadRevision: anchor.read,
			}, nil
		}
		next, err := scriptexecutions.AdvanceScriptExecutionRecord(anchor.record, input)
		if err != nil {
			return etcdstore.Versioned[scriptexecutions.ScriptExecutionRecord]{}, err
		}
		value, err := recordcodec.Encode("script-execution", next)
		if err != nil {
			return etcdstore.Versioned[scriptexecutions.ScriptExecutionRecord]{}, err
		}
		transaction, err := repository.store.Transact(ctx, anchor.conditions, []etcdstore.Mutation{{
			Type: etcdstore.MutationPut, Key: scriptexecutions.ScriptExecutionKey(input.ExecutionID), Value: value,
		}})
		clear(value)
		etcdstore.ClearValues(transaction.FailureReads)
		if err != nil {
			return etcdstore.Versioned[scriptexecutions.ScriptExecutionRecord]{}, err
		}
		if transaction.Succeeded {
			return etcdstore.Versioned[scriptexecutions.ScriptExecutionRecord]{
				Record: next, Revision: transaction.Revision, ReadRevision: transaction.Revision,
			}, nil
		}
	}
	return etcdstore.Versioned[scriptexecutions.ScriptExecutionRecord]{}, errs.New(
		errs.KindStateConflict,
		"Script checkpoint changed concurrently",
	)
}

func (repository *ScriptRepository) loadScriptCheckpointAnchor(
	ctx context.Context,
	input scriptexecutions.ScriptCheckpointInput,
) (scriptCheckpointAnchor, error) {
	primary, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		scriptexecutions.ScriptExecutionKey(input.ExecutionID), taskjournal.TaskStorageKey(input.TaskID), taskjournal.TaskAssignmentIndexKey(input.TaskID),
	}})
	if err != nil {
		return scriptCheckpointAnchor{}, err
	}
	if primary == nil || len(primary.Values) != 3 || primary.Values[0] == nil ||
		primary.Values[1] == nil || primary.Values[2] == nil {
		etcdstore.ClearValues(primary.Values)
		return scriptCheckpointAnchor{}, errs.New(errs.KindStateConflict, "Script checkpoint assignment is unavailable")
	}
	defer etcdstore.ClearValues(primary.Values)
	execution, executionErr := recordcodec.Decode[scriptexecutions.ScriptExecutionRecord](primary.Values[0].Value, "script-execution")
	task, taskErr := decodeTaskRecord(primary.Values[1].Value)
	assignment, assignmentErr := taskassignments.DecodeTaskAssignment(primary.Values[2].Value)
	if executionErr != nil || taskErr != nil || assignmentErr != nil ||
		scriptexecutions.ValidateScriptExecutionRecord(execution) != nil {
		return scriptCheckpointAnchor{}, errs.New(errs.KindInternal, "Script checkpoint durable state is corrupt")
	}
	if execution.ID != input.ExecutionID || execution.CurrentTaskID != input.TaskID ||
		execution.OperationID != input.OperationID || execution.StepID != input.StepID || execution.PlanHash != input.PlanHash ||
		(execution.AssignmentID != "" && execution.AssignmentID != input.AssignmentID) ||
		task.ID != input.TaskID || task.OperationID != input.OperationID ||
		task.Status != taskjournal.TaskStatusRunning || task.Executor != taskjournal.TaskExecutorAgent || task.PlanHash != input.PlanHash ||
		!taskOwnsScriptExecution(task, execution) || assignment.TaskID != input.TaskID ||
		assignment.AssignmentID != input.AssignmentID || assignment.Executor != taskjournal.TaskExecutorAgent ||
		assignment.AgentID != input.AgentID || assignment.AgentGeneration != input.AgentGeneration {
		return scriptCheckpointAnchor{}, errs.New(
			errs.KindStateConflict,
			"Script checkpoint does not own the running assignment",
		)
	}
	var blueprintConditions []etcdstore.Condition
	if task.Type == taskjournal.TaskUpdate {
		blueprintConditions, err = repository.blueprintScriptExecutionAuthority(
			ctx, task, execution, primary.ReadRevision,
		)
		if err != nil {
			return scriptCheckpointAnchor{}, err
		}
	}
	if task.Type == taskjournal.TaskScript {
		blueprintConditions, err = repository.manualScriptExecutionAuthority(ctx, task, execution, primary.ReadRevision)
		if err != nil {
			return scriptCheckpointAnchor{}, err
		}
	}
	claimKey := taskjournal.TaskExecutionClaimKey(taskjournal.TaskExecutorAgent, input.AgentID, input.TaskID)
	timeoutKey := taskjournal.TaskTimeoutIndexKey(input.TaskID, assignment.Deadline)
	claim, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{claimKey, timeoutKey}, Revision: primary.ReadRevision,
	})
	if err != nil {
		return scriptCheckpointAnchor{}, err
	}
	if claim == nil || len(claim.Values) != 2 || claim.Values[0] == nil || claim.Values[1] == nil {
		etcdstore.ClearValues(claim.Values)
		return scriptCheckpointAnchor{}, errs.New(errs.KindStateConflict, "Script checkpoint execution claim changed")
	}
	defer etcdstore.ClearValues(claim.Values)
	if primary.Values[2].ModRevision != claim.Values[0].ModRevision ||
		primary.Values[2].ModRevision != claim.Values[1].ModRevision ||
		!bytes.Equal(primary.Values[2].Value, claim.Values[0].Value) ||
		!bytes.Equal(primary.Values[2].Value, claim.Values[1].Value) {
		return scriptCheckpointAnchor{}, errs.New(errs.KindInternal, "Script checkpoint assignment copies diverged")
	}
	conditions := []etcdstore.Condition{
		{Key: scriptexecutions.ScriptExecutionKey(input.ExecutionID), ModRevision: primary.Values[0].ModRevision},
		{Key: taskjournal.TaskStorageKey(input.TaskID), ModRevision: primary.Values[1].ModRevision},
		{Key: taskjournal.TaskAssignmentIndexKey(input.TaskID), ModRevision: primary.Values[2].ModRevision},
		{Key: claimKey, ModRevision: claim.Values[0].ModRevision},
		{Key: timeoutKey, ModRevision: claim.Values[1].ModRevision},
	}
	conditions = append(conditions, blueprintConditions...)
	return scriptCheckpointAnchor{
		record: execution, revision: primary.Values[0].ModRevision, read: primary.ReadRevision,
		conditions: conditions,
	}, nil
}

func taskOwnsScriptExecution(task TaskRecord, execution scriptexecutions.ScriptExecutionRecord) bool {
	switch task.Type {
	case taskjournal.TaskScript:
		return task.Target == execution.ScriptID && len(task.Steps) == 1 &&
			task.Steps[0].ID == execution.StepID &&
			task.Params[scriptexecutions.ScriptExecutionIDParam] == execution.ID
	case taskjournal.TaskDeploy, taskjournal.TaskRollback:
		if task.Owner.EnvironmentID != execution.EnvironmentID ||
			task.Params[releaserender.TaskReleasePublicationParam] == "" {
			return false
		}
		for _, step := range task.Steps {
			if step.ID == execution.StepID {
				return true
			}
		}
	case taskjournal.TaskUpdate:
		return blueprintScriptTaskShape(task) &&
			task.Owner.EnvironmentID == execution.EnvironmentID &&
			task.Params[releaserender.ReleaseHookStepExecutionParam(execution.StepID)] == execution.ID
	}
	return false
}
