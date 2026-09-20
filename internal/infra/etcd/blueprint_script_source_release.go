package etcd

import (
	"context"
	"errors"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type scriptTerminalSourceRelease struct {
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
	advance    *blueprintTerminalSourceAdvance
}

func (change *scriptTerminalSourceRelease) clear() {
	if change == nil {
		return
	}
	clearMutationValues(change.mutations)
	*change = scriptTerminalSourceRelease{}
}

// prepareTerminalScriptSourceRelease advances the existing bounded
// source-reference release protocol without surrendering Task assignment
// authority. Only its final fragment joins the Task terminal transaction.
func (repository *TaskRepository) prepareTerminalScriptSourceRelease(
	ctx context.Context,
	task TaskRecord,
	taskValue *etcdstore.KeyValue,
	assignment TaskAssignmentRecord,
	assignmentValue *etcdstore.KeyValue,
	assignmentIndexValue *etcdstore.KeyValue,
	recovery releaseRecoveryAcknowledgement,
	terminalStatus TaskStatus,
	terminalAt *time.Time,
	revision int64,
	submittedStatus TaskStatus,
	submittedResult TaskResultRecord,
	advanceNow bool,
) (scriptTerminalSourceRelease, bool, error) {
	if task.Type == TaskScript {
		return repository.prepareManualScriptTerminalRelease(ctx, task, taskValue, assignment,
			assignmentValue, assignmentIndexValue, terminalStatus, terminalAt, revision, submittedResult)
	}
	steps, err := releaseHookExecutionSteps(task)
	if err != nil || len(steps) == 0 {
		return scriptTerminalSourceRelease{}, false, err
	}
	if taskValue == nil || assignmentValue == nil || assignmentIndexValue == nil || revision <= 0 ||
		(assignment.ExecutionMode == TaskExecutionModeRecoveryOnly && (!recovery.final || recovery.value == nil)) {
		return scriptTerminalSourceRelease{}, false, corruptTaskAssignment()
	}
	lifecycleKey, lifecycleValue, _, err := repository.assignmentLifecycleIndexAtRevision(
		ctx,
		assignment,
		assignmentValue,
		revision,
	)
	if err != nil {
		return scriptTerminalSourceRelease{}, false, err
	}
	environmentID, materializes, err := taskEnvironmentWriter(task)
	if err != nil {
		return scriptTerminalSourceRelease{}, false, err
	}
	if !materializes {
		environmentID = task.Owner.EnvironmentID
	}
	if environmentID == "" || environmentID != task.Owner.EnvironmentID {
		return scriptTerminalSourceRelease{}, false, corruptTaskAssignment()
	}
	rootKey := scriptSourceRootKey(task.OperationID)
	keys := []string{
		rootKey,
		environmentMutationEpochKey(environmentID),
		taskActiveOperationKey(task.OperationID),
	}
	writerIndex := -1
	if materializes {
		writerIndex = len(keys)
		keys = append(keys, taskMaterializationWriterKey(environmentID))
	}
	executionOffset := len(keys)
	for _, step := range steps {
		keys = append(keys, scriptExecutionKey(step.executionID))
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return scriptTerminalSourceRelease{}, false, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
		return scriptTerminalSourceRelease{}, false, corruptReleaseRecord()
	}
	for _, value := range read.Values {
		if value == nil {
			return scriptTerminalSourceRelease{}, false, corruptReleaseRecord()
		}
	}
	root, err := decodeScriptOperationSourceRoot(read.Values[0].Value)
	if err != nil || root.OperationID != task.OperationID {
		return scriptTerminalSourceRelease{}, false, corruptReleaseRecord()
	}
	activeTaskID, err := decodeTaskReference(read.Values[2].Value)
	if err != nil || activeTaskID != task.ID {
		return scriptTerminalSourceRelease{}, false, corruptTaskAssignment()
	}
	if materializes {
		writer, decodeErr := decodeTaskMaterializationWriter(read.Values[writerIndex].Value)
		if decodeErr != nil || validateTaskMaterializationWriterForTask(writer, task, environmentID) != nil {
			return scriptTerminalSourceRelease{}, false, corruptTaskMaterializationWriter()
		}
	}
	executions := make([]ScriptExecutionRecord, len(steps))
	for index, step := range steps {
		value := read.Values[index+executionOffset]
		record, decodeErr := decodeEnvelope[ScriptExecutionRecord](value.Value, "script-execution")
		if decodeErr != nil || validateScriptExecutionRecord(record) != nil || !taskOwnsScriptExecution(task, record) ||
			record.ID != step.executionID || record.StepID != step.stepID || record.CurrentTaskID != task.ID ||
			record.OperationID != task.OperationID || record.PlanHash != task.PlanHash {
			return scriptTerminalSourceRelease{}, false, corruptReleaseRecord()
		}
		executions[index] = record
		if root.Phase == ScriptOperationSourceActive && terminalStatus == TaskStatusCompleted &&
			(record.State != ScriptExecutionCleanupProven || record.AssignmentID != assignment.AssignmentID || record.ReconciliationRequired) {
			return scriptTerminalSourceRelease{}, false, errs.New(
				errs.KindStateConflict,
				"Blueprint Script cleanup is not proven",
			)
		}
	}
	guards := []etcdstore.Condition{
		{Key: taskKey(task.ID), ModRevision: taskValue.ModRevision},
		{
			Key:         taskExecutionClaimKey(assignment.Executor, assignment.AgentID, task.ID),
			ModRevision: assignmentValue.ModRevision,
		},
		{Key: taskAssignmentIndexKey(task.ID), ModRevision: assignmentIndexValue.ModRevision},
		{Key: lifecycleKey, ModRevision: lifecycleValue.ModRevision},
		{Key: environmentMutationEpochKey(environmentID), ModRevision: read.Values[1].ModRevision},
		{Key: taskActiveOperationKey(task.OperationID), ModRevision: read.Values[2].ModRevision},
	}
	if recovery.final {
		guards = append(guards, recovery.conditions...)
	}
	if materializes {
		guards = append(guards, etcdstore.Condition{
			Key: taskMaterializationWriterKey(environmentID), ModRevision: read.Values[writerIndex].ModRevision,
		})
	}
	executionGuards := make([]etcdstore.Condition, 0, len(steps))
	for index := range steps {
		executionGuards = append(executionGuards, etcdstore.Condition{
			Key: read.Values[index+executionOffset].Key, ModRevision: read.Values[index+executionOffset].ModRevision,
		})
	}
	guards = append(guards, executionGuards...)
	var pending *blueprintTerminalSourceAdvance
	if task.Type == TaskUpdate && !advanceNow {
		pending = &blueprintTerminalSourceAdvance{
			task: task, taskValue: taskValue, assignment: assignment, assignmentValue: assignmentValue,
			assignmentIndexValue: assignmentIndexValue, recovery: recovery, terminalStatus: terminalStatus,
			terminalAt: *terminalAt, revision: revision, submittedStatus: submittedStatus, submittedResult: submittedResult,
		}
	}
	var closingCondition etcdstore.Condition
	var closingMutation etcdstore.Mutation
	if task.Type == TaskUpdate {
		current := TaskAssignment{
			Task:       Versioned[TaskRecord]{Record: task, Revision: taskValue.ModRevision, ReadRevision: revision},
			Assignment: Versioned[TaskAssignmentRecord]{Record: assignment, Revision: assignmentValue.ModRevision},
		}
		report, condition, mutation, reportErr := repository.prepareScriptClosingReport(
			ctx, current, submittedStatus, submittedResult, *terminalAt, root.Phase == ScriptOperationSourceActive,
		)
		if reportErr != nil {
			return scriptTerminalSourceRelease{}, false, reportErr
		}
		closingCondition, closingMutation, *terminalAt = condition, mutation, report.ObservedAt
		if pending != nil {
			pending.terminalAt = report.ObservedAt
		}
		defer clear(closingMutation.Value)
		guards = append(guards, closingCondition)
	}
	if root.Phase == ScriptOperationSourceActive {
		if root.ReleasePath != ScriptSourceReleaseAbsent ||
			(root.RetryDisposition != ScriptRetryDispositionUndecided && root.RetryDisposition != ScriptRetryDispositionTransferred) {
			return scriptTerminalSourceRelease{}, false, corruptReleaseRecord()
		}
		release, releaseErr := repository.beginBlueprintTerminalScriptSourceRelease(
			ctx, task, executions, read.Values[executionOffset:], *terminalAt,
		)
		if releaseErr != nil {
			return scriptTerminalSourceRelease{}, false, releaseErr
		}
		defer release.clear()
		if pending != nil {
			return pending.projection(executionGuards), false, nil
		}
		if task.Type == TaskUpdate {
			release.mutations = append(release.mutations, closingMutation)
		}
		transaction, transactErr := repository.store.Transact(
			ctx,
			append(guards, release.conditions...),
			release.mutations,
		)
		if transactErr != nil {
			return scriptTerminalSourceRelease{}, false, transactErr
		}
		clearKeyValues(transaction.FailureReads)
		return scriptTerminalSourceRelease{}, true, nil
	}
	if root.Phase != ScriptOperationSourceReleasing || root.ReleasePath != ScriptSourceReleaseNormal ||
		root.RetryDisposition != ScriptRetryDispositionForbidden {
		return scriptTerminalSourceRelease{}, false, corruptReleaseRecord()
	}
	for _, execution := range executions {
		if !releaseRecoveryClosedScriptExecutionMatches(execution, assignment.AssignmentID) {
			return scriptTerminalSourceRelease{}, false, corruptReleaseRecord()
		}
	}
	if pending != nil && root.ReleaseCursor != root.MembershipCount {
		return pending.projection(executionGuards), false, nil
	}
	authority, err := newScriptSourceReferenceAuthority(repository.store)
	if err != nil {
		return scriptTerminalSourceRelease{}, false, err
	}
	if pending == nil {
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
			return scriptTerminalSourceRelease{}, false, corruptReleaseRecord()
		}
	}
	final, err := authority.PrepareReleaseFinalization(ctx, task.OperationID)
	if err != nil {
		return scriptTerminalSourceRelease{}, false, err
	}
	defer final.Clear()
	// The terminal transaction already compares Task, assignment, lifecycle,
	// Environment epoch, writer, and recovery authority. Add only source and
	// execution evidence here; duplicate compares are rejected by its compiler.
	change := scriptTerminalSourceRelease{
		conditions: append(append([]etcdstore.Condition(nil), final.conditions...), executionGuards...),
		mutations:  cloneBlueprintCandidateMutations(final.mutations),
	}
	if task.Type == TaskUpdate {
		change.conditions = append(change.conditions, closingCondition)
		change.mutations = append(change.mutations, closingMutation)
	}
	return change, false, nil
}

func (repository *TaskRepository) beginBlueprintTerminalScriptSourceRelease(
	ctx context.Context,
	task TaskRecord,
	executions []ScriptExecutionRecord,
	values []*etcdstore.KeyValue,
	terminalAt time.Time,
) (scriptTerminalSourceRelease, error) {
	authority, err := newScriptSourceReferenceAuthority(repository.store)
	if err != nil {
		return scriptTerminalSourceRelease{}, err
	}
	release, err := authority.PrepareNormalRelease(ctx, task.OperationID, ScriptRetryDispositionForbidden)
	if err != nil {
		return scriptTerminalSourceRelease{}, err
	}
	conditions := append([]etcdstore.Condition(nil), release.conditions...)
	mutations := cloneBlueprintCandidateMutations(release.mutations)
	release.Clear()
	for index, execution := range executions {
		if !terminalAt.After(execution.UpdatedAt) {
			clearMutationValues(mutations)
			return scriptTerminalSourceRelease{}, errs.New(
				errs.KindStateConflict, "recovery parent Script execution timestamp changed",
			)
		}
		next := execution
		switch execution.State {
		case ScriptExecutionNotStarted:
			if execution.AssignmentID != "" || execution.StartAuthorized || !execution.ActiveReference {
				clearMutationValues(mutations)
				return scriptTerminalSourceRelease{}, errs.New(
					errs.KindStateConflict, "recovery parent Script execution may already have started",
				)
			}
			outcome := ScriptOutcomeEvidence{Reason: ScriptOutcomeParentFailureBeforeStart, ObservedAt: terminalAt}
			cleanup := ScriptCleanupEvidence{ContainerAbsent: true, BodyAbsent: true, ExecutionDirectoryAbsent: true}
			digest, digestErr := scriptControllerCleanupSHA256(outcome, cleanup)
			if digestErr != nil {
				clearMutationValues(mutations)
				return scriptTerminalSourceRelease{}, digestErr
			}
			next.State = ScriptExecutionCleanupProven
			next.Outcome = &outcome
			next.Cleanup = &cleanup
			next.ControllerCleanup = ScriptControllerCleanupReleaseRecoveryParentFailure
			next.LastCheckpointSHA256 = digest
		case ScriptExecutionCleanupProven:
			if execution.AssignmentID == "" || execution.ControllerCleanup != "" || execution.ReconciliationRequired {
				clearMutationValues(mutations)
				return scriptTerminalSourceRelease{}, errs.New(
					errs.KindStateConflict, "recovery parent Script cleanup authority changed",
				)
			}
		default:
			clearMutationValues(mutations)
			return scriptTerminalSourceRelease{}, errs.New(
				errs.KindStateConflict, "recovery parent Script execution may already have started",
			)
		}
		next.ActiveReference = false
		next.UpdatedAt = terminalAt
		if validateScriptExecutionRecord(next) != nil ||
			!releaseRecoveryClosedScriptExecutionMatches(next, execution.AssignmentID) {
			clearMutationValues(mutations)
			return scriptTerminalSourceRelease{}, corruptReleaseRecord()
		}
		encoded, encodeErr := encodeEnvelope("script-execution", next)
		if encodeErr != nil {
			clearMutationValues(mutations)
			return scriptTerminalSourceRelease{}, encodeErr
		}
		conditions = append(conditions, etcdstore.Condition{Key: values[index].Key, ModRevision: values[index].ModRevision})
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: values[index].Key, Value: encoded})
	}
	return scriptTerminalSourceRelease{conditions: conditions, mutations: mutations}, nil
}

func releaseRecoveryParentFailureExecutionMatches(record ScriptExecutionRecord) bool {
	if record.State != ScriptExecutionCleanupProven || record.AssignmentID != "" || record.StartAuthorized ||
		record.BodyPrepared != nil || record.ContainerCreated != nil || record.ActiveReference ||
		record.ReconciliationRequired || record.Outcome == nil || record.Cleanup == nil ||
		record.ControllerCleanup != ScriptControllerCleanupReleaseRecoveryParentFailure ||
		record.Outcome.Reason != ScriptOutcomeParentFailureBeforeStart || record.Outcome.ExitCode != nil ||
		record.Outcome.OutputTruncated || record.Outcome.ObservedAt.IsZero() ||
		!record.Cleanup.ContainerAbsent || !record.Cleanup.BodyAbsent || !record.Cleanup.ExecutionDirectoryAbsent ||
		record.Cleanup.ContainerID != "" || record.Cleanup.BodyDevice != 0 || record.Cleanup.BodyInode != 0 ||
		record.Cleanup.BodyLeaf != "" || !record.UpdatedAt.Equal(record.Outcome.ObservedAt) {
		return false
	}
	digest, err := scriptControllerCleanupSHA256(*record.Outcome, *record.Cleanup)
	return err == nil && record.LastCheckpointSHA256 == digest
}

func releaseRecoveryClosedScriptExecutionMatches(record ScriptExecutionRecord, assignmentID string) bool {
	if releaseRecoveryParentFailureExecutionMatches(record) {
		return true
	}
	return record.State == ScriptExecutionCleanupProven && record.AssignmentID == assignmentID &&
		record.ControllerCleanup == "" && !record.ActiveReference && !record.ReconciliationRequired &&
		validateScriptExecutionRecord(record) == nil
}
