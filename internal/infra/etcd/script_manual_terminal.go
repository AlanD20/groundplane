package etcd

import (
	"context"
	"errors"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	scriptexecutions "github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	sourceref "github.com/AlanD20/groundplane/internal/infra/scriptsourcereference"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// prepareManualScriptTerminalRelease keeps the Task assigned until its exact
// source set has drained. The original report is persisted with closure so a
// reconnect resumes cleanup without redispatching an already closed execution.
func (repository *TaskRepository) prepareManualScriptTerminalRelease(
	ctx context.Context,
	task TaskRecord,
	taskValue *etcdstore.KeyValue,
	assignment taskassignments.TaskAssignmentRecord,
	assignmentValue, assignmentIndexValue *etcdstore.KeyValue,
	status taskjournal.TaskStatus,
	terminalAt *time.Time,
	revision int64,
	result taskjournal.TaskResultRecord,
) (scriptTerminalSourceRelease, bool, error) {
	scripts := &ScriptRepository{store: repository.store}
	execution, executionValue, err := scripts.manualScriptExecutionAtRevision(ctx, task, revision)
	if err != nil {
		return scriptTerminalSourceRelease{}, false, err
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			scriptSourceRootKey(task.OperationID),
			taskjournal.TaskActiveOperationKey(task.OperationID),
		}, Revision: revision,
	})
	if err != nil {
		return scriptTerminalSourceRelease{}, false, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 2 ||
		read.Values[0] == nil || read.Values[1] == nil || taskValue == nil ||
		assignmentValue == nil || assignmentIndexValue == nil || terminalAt == nil {
		return scriptTerminalSourceRelease{}, false, taskassignments.CorruptTaskAssignment()
	}
	root, err := decodeScriptOperationSourceRoot(read.Values[0].Value)
	if err != nil || !manualScriptRootMatches(execution, root) {
		return scriptTerminalSourceRelease{}, false, taskassignments.CorruptTaskAssignment()
	}
	activeTaskID, err := idempotencyrecord.DecodeTaskReference(read.Values[1].Value)
	if err != nil || activeTaskID != task.ID {
		return scriptTerminalSourceRelease{}, false, taskassignments.CorruptTaskAssignment()
	}
	if execution.State == scriptexecutions.ScriptExecutionNotStarted && !result.ReconciliationRequired &&
		(status == taskjournal.TaskStatusFailed || status == taskjournal.TaskStatusTimedOut) {
		change, err := repository.prepareManualScriptRetryAvailability(ctx, task, execution, executionValue,
			read.Values[0], status, terminalAt)
		return change, false, err
	}
	preparedAbort := false
	if root.Phase == ScriptOperationSourceActive && execution.State == scriptexecutions.ScriptExecutionNotStarted &&
		status == taskjournal.TaskStatusAborted && !result.ReconciliationRequired {
		execution, err = abortAssignedManualScriptBeforeStart(execution, *terminalAt)
		if err != nil {
			return scriptTerminalSourceRelease{}, false, err
		}
		preparedAbort = true
	}
	agentCleanupProven := execution.State == scriptexecutions.ScriptExecutionCleanupProven &&
		execution.AssignmentID == assignment.AssignmentID && execution.ControllerCleanup == "" && !execution.ReconciliationRequired
	controllerAbortProven := status == taskjournal.TaskStatusAborted && manualScriptAssignedAbortMatches(execution)
	if result.ReconciliationRequired || (!agentCleanupProven && !controllerAbortProven) {
		return scriptTerminalSourceRelease{}, false, errs.New(
			errs.KindStateConflict,
			"manual Script cleanup is not proven",
		)
	}
	lifecycleKey, lifecycleValue, _, err := repository.assignmentLifecycleIndexAtRevision(ctx,
		assignment, assignmentValue, revision)
	if err != nil {
		return scriptTerminalSourceRelease{}, false, err
	}
	current := TaskAssignment{
		Task:       etcdstore.Versioned[TaskRecord]{Record: task, Revision: taskValue.ModRevision, ReadRevision: revision},
		Assignment: etcdstore.Versioned[taskassignments.TaskAssignmentRecord]{Record: assignment, Revision: assignmentValue.ModRevision},
	}
	report, reportCondition, reportMutation, err := repository.prepareScriptClosingReport(ctx,
		current, status, result, *terminalAt, root.Phase == ScriptOperationSourceActive)
	if err != nil {
		return scriptTerminalSourceRelease{}, false, err
	}
	defer clear(reportMutation.Value)
	*terminalAt = report.ObservedAt
	executionCondition := etcdstore.Condition{Key: executionValue.Key, ModRevision: executionValue.ModRevision}
	guards := []etcdstore.Condition{
		{Key: taskjournal.TaskStorageKey(task.ID), ModRevision: taskValue.ModRevision},
		{
			Key:         taskjournal.TaskExecutionClaimKey(assignment.Executor, assignment.AgentID, task.ID),
			ModRevision: assignmentValue.ModRevision,
		},
		{Key: taskjournal.TaskAssignmentIndexKey(task.ID), ModRevision: assignmentIndexValue.ModRevision},
		{Key: lifecycleKey, ModRevision: lifecycleValue.ModRevision},
		{Key: taskjournal.TaskActiveOperationKey(task.OperationID), ModRevision: read.Values[1].ModRevision},
		executionCondition, reportCondition,
	}
	authority, err := newScriptSourceReferenceAuthority(repository.store)
	if err != nil {
		return scriptTerminalSourceRelease{}, false, err
	}
	disposition := sourceref.RetryDispositionForbidden
	if status == taskjournal.TaskStatusAborted {
		disposition = sourceref.RetryDispositionAbandoned
	}
	if root.Phase == ScriptOperationSourceActive {
		if (!preparedAbort && (!execution.ActiveReference || !terminalAt.After(execution.UpdatedAt))) ||
			root.ReleasePath != ScriptSourceReleaseAbsent ||
			(root.RetryDisposition != sourceref.RetryDispositionUndecided && root.RetryDisposition != sourceref.RetryDispositionTransferred) {
			return scriptTerminalSourceRelease{}, false, taskassignments.CorruptTaskAssignment()
		}
		release, err := authority.PrepareNormalRelease(ctx, task.OperationID, disposition)
		if err != nil {
			return scriptTerminalSourceRelease{}, false, err
		}
		defer release.Clear()
		execution.ActiveReference, execution.UpdatedAt = false, *terminalAt
		if err := scriptexecutions.ValidateScriptExecutionRecord(execution); err != nil {
			return scriptTerminalSourceRelease{}, false, err
		}
		encoded, err := recordcodec.Encode("script-execution", execution)
		if err != nil {
			return scriptTerminalSourceRelease{}, false, err
		}
		defer clear(encoded)
		mutations := append(cloneBlueprintCandidateMutations(release.mutations), reportMutation,
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: executionValue.Key, Value: encoded})
		defer clearMutationValues(mutations)
		transaction, err := repository.store.Transact(ctx, append(guards, release.conditions...), mutations)
		if err != nil {
			return scriptTerminalSourceRelease{}, false, err
		}
		etcdstore.ClearValues(transaction.FailureReads)
		return scriptTerminalSourceRelease{}, true, nil
	}
	if root.Phase != ScriptOperationSourceReleasing || root.ReleasePath != ScriptSourceReleaseNormal ||
		root.RetryDisposition != disposition || execution.ActiveReference || !execution.UpdatedAt.Equal(*terminalAt) {
		return scriptTerminalSourceRelease{}, false, taskassignments.CorruptTaskAssignment()
	}
	processed, drained, err := authority.ReleaseNext(ctx, task.OperationID, guards)
	if err != nil {
		if errors.Is(err, errs.New(errs.KindStateConflict, "")) {
			return scriptTerminalSourceRelease{}, true, nil
		}
		return scriptTerminalSourceRelease{}, false, err
	}
	if processed {
		return scriptTerminalSourceRelease{}, true, nil
	}
	if !drained {
		return scriptTerminalSourceRelease{}, false, taskassignments.CorruptTaskAssignment()
	}
	final, err := authority.PrepareReleaseFinalization(ctx, task.OperationID)
	if err != nil {
		return scriptTerminalSourceRelease{}, false, err
	}
	defer final.Clear()
	return scriptTerminalSourceRelease{
		conditions: append(append([]etcdstore.Condition(nil), final.conditions...), executionCondition, reportCondition),
		mutations:  append(cloneBlueprintCandidateMutations(final.mutations), reportMutation),
	}, false, nil
}
