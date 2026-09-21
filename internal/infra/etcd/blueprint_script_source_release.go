package etcd

import (
	"context"
	"errors"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	releases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	scriptexecutions "github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	scriptsourceevidence "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	scriptsourcepublication "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcepublication"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	sourceref "github.com/AlanD20/groundplane/internal/infra/scriptsourcereference"
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
	etcdstore.ClearMutationValues(change.mutations)
	*change = scriptTerminalSourceRelease{}
}

// prepareTerminalScriptSourceRelease advances the existing bounded
// source-reference release protocol without surrendering Task assignment
// authority. Only its final fragment joins the Task terminal transaction.
func (repository *TaskRepository) prepareTerminalScriptSourceRelease(
	ctx context.Context,
	task TaskRecord,
	taskValue *etcdstore.KeyValue,
	assignment taskassignments.TaskAssignmentRecord,
	assignmentValue *etcdstore.KeyValue,
	assignmentIndexValue *etcdstore.KeyValue,
	recovery releaseRecoveryAcknowledgement,
	terminalStatus taskjournal.TaskStatus,
	terminalAt *time.Time,
	revision int64,
	submittedStatus taskjournal.TaskStatus,
	submittedResult taskjournal.TaskResultRecord,
	advanceNow bool,
) (scriptTerminalSourceRelease, bool, error) {
	if task.Type == taskjournal.TaskScript {
		return repository.prepareManualScriptTerminalRelease(ctx, task, taskValue, assignment,
			assignmentValue, assignmentIndexValue, terminalStatus, terminalAt, revision, submittedResult)
	}
	steps, err := releaseHookExecutionSteps(task)
	if err != nil || len(steps) == 0 {
		return scriptTerminalSourceRelease{}, false, err
	}
	if taskValue == nil || assignmentValue == nil || assignmentIndexValue == nil || revision <= 0 ||
		(assignment.ExecutionMode == taskassignments.TaskExecutionModeRecoveryOnly && (!recovery.final || recovery.value == nil)) {
		return scriptTerminalSourceRelease{}, false, taskassignments.CorruptTaskAssignment()
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
		return scriptTerminalSourceRelease{}, false, taskassignments.CorruptTaskAssignment()
	}
	rootKey := scriptsourceevidence.ScriptSourceRootKey(task.OperationID)
	keys := []string{
		rootKey,
		hierarchyrecord.EnvironmentMutationEpochKey(environmentID),
		taskjournal.TaskActiveOperationKey(task.OperationID),
	}
	writerIndex := -1
	if materializes {
		writerIndex = len(keys)
		keys = append(keys, taskjournal.TaskMaterializationWriterKey(environmentID))
	}
	executionOffset := len(keys)
	for _, step := range steps {
		keys = append(keys, scriptexecutions.ScriptExecutionKey(step.executionID))
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return scriptTerminalSourceRelease{}, false, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
		return scriptTerminalSourceRelease{}, false, releases.CorruptReleaseRecord()
	}
	for _, value := range read.Values {
		if value == nil {
			return scriptTerminalSourceRelease{}, false, releases.CorruptReleaseRecord()
		}
	}
	root, err := scriptsourceevidence.DecodeScriptOperationSourceRoot(read.Values[0].Value)
	if err != nil || root.OperationID != task.OperationID {
		return scriptTerminalSourceRelease{}, false, releases.CorruptReleaseRecord()
	}
	activeTaskID, err := idempotencyrecord.DecodeTaskReference(read.Values[2].Value)
	if err != nil || activeTaskID != task.ID {
		return scriptTerminalSourceRelease{}, false, taskassignments.CorruptTaskAssignment()
	}
	if materializes {
		writer, decodeErr := decodeTaskMaterializationWriter(read.Values[writerIndex].Value)
		if decodeErr != nil || validateTaskMaterializationWriterForTask(writer, task, environmentID) != nil {
			return scriptTerminalSourceRelease{}, false, corruptTaskMaterializationWriter()
		}
	}
	executions := make([]scriptexecutions.ScriptExecutionRecord, len(steps))
	for index, step := range steps {
		value := read.Values[index+executionOffset]
		record, decodeErr := recordcodec.Decode[scriptexecutions.ScriptExecutionRecord](value.Value, "script-execution")
		if decodeErr != nil || scriptexecutions.ValidateScriptExecutionRecord(record) != nil ||
			!taskOwnsScriptExecution(task, record) ||
			record.ID != step.executionID ||
			record.StepID != step.stepID ||
			record.CurrentTaskID != task.ID ||
			record.OperationID != task.OperationID ||
			record.PlanHash != task.PlanHash {
			return scriptTerminalSourceRelease{}, false, releases.CorruptReleaseRecord()
		}
		executions[index] = record
		if root.Phase == scriptsourceevidence.ScriptOperationSourceActive &&
			terminalStatus == taskjournal.TaskStatusCompleted &&
			(record.State != scriptexecutions.ScriptExecutionCleanupProven || record.AssignmentID != assignment.AssignmentID || record.ReconciliationRequired) {
			return scriptTerminalSourceRelease{}, false, errs.New(
				errs.KindStateConflict,
				"Blueprint Script cleanup is not proven",
			)
		}
	}
	guards := []etcdstore.Condition{
		{Key: taskjournal.TaskStorageKey(task.ID), ModRevision: taskValue.ModRevision},
		{
			Key:         taskjournal.TaskExecutionClaimKey(assignment.Executor, assignment.AgentID, task.ID),
			ModRevision: assignmentValue.ModRevision,
		},
		{Key: taskjournal.TaskAssignmentIndexKey(task.ID), ModRevision: assignmentIndexValue.ModRevision},
		{Key: lifecycleKey, ModRevision: lifecycleValue.ModRevision},
		{Key: hierarchyrecord.EnvironmentMutationEpochKey(environmentID), ModRevision: read.Values[1].ModRevision},
		{Key: taskjournal.TaskActiveOperationKey(task.OperationID), ModRevision: read.Values[2].ModRevision},
	}
	if recovery.final {
		guards = append(guards, recovery.conditions...)
	}
	if materializes {
		guards = append(guards, etcdstore.Condition{
			Key: taskjournal.TaskMaterializationWriterKey(
				environmentID,
			), ModRevision: read.Values[writerIndex].ModRevision,
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
	if task.Type == taskjournal.TaskUpdate && !advanceNow {
		pending = &blueprintTerminalSourceAdvance{
			task: task, taskValue: taskValue, assignment: assignment, assignmentValue: assignmentValue,
			assignmentIndexValue: assignmentIndexValue, recovery: recovery, terminalStatus: terminalStatus,
			terminalAt: *terminalAt, revision: revision, submittedStatus: submittedStatus, submittedResult: submittedResult,
		}
	}
	var closingCondition etcdstore.Condition
	var closingMutation etcdstore.Mutation
	if task.Type == taskjournal.TaskUpdate {
		current := TaskAssignment{
			Task: etcdstore.Versioned[TaskRecord]{
				Record:       task,
				Revision:     taskValue.ModRevision,
				ReadRevision: revision,
			},
			Assignment: etcdstore.Versioned[taskassignments.TaskAssignmentRecord]{
				Record:   assignment,
				Revision: assignmentValue.ModRevision,
			},
		}
		report, condition, mutation, reportErr := repository.prepareScriptClosingReport(
			ctx,
			current,
			submittedStatus,
			submittedResult,
			*terminalAt,
			root.Phase == scriptsourceevidence.ScriptOperationSourceActive,
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
	if root.Phase == scriptsourceevidence.ScriptOperationSourceActive {
		if root.ReleasePath != scriptsourceevidence.ScriptSourceReleaseAbsent ||
			(root.RetryDisposition != sourceref.RetryDispositionUndecided && root.RetryDisposition != sourceref.RetryDispositionTransferred) {
			return scriptTerminalSourceRelease{}, false, releases.CorruptReleaseRecord()
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
		if task.Type == taskjournal.TaskUpdate {
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
		etcdstore.ClearValues(transaction.FailureReads)
		return scriptTerminalSourceRelease{}, true, nil
	}
	if root.Phase != scriptsourceevidence.ScriptOperationSourceReleasing ||
		root.ReleasePath != scriptsourceevidence.ScriptSourceReleaseNormal ||
		root.RetryDisposition != sourceref.RetryDispositionForbidden {
		return scriptTerminalSourceRelease{}, false, releases.CorruptReleaseRecord()
	}
	for _, execution := range executions {
		if !releaseRecoveryClosedScriptExecutionMatches(execution, assignment.AssignmentID) {
			return scriptTerminalSourceRelease{}, false, releases.CorruptReleaseRecord()
		}
	}
	if pending != nil && root.ReleaseCursor != root.MembershipCount {
		return pending.projection(executionGuards), false, nil
	}
	authority, err := scriptsourcepublication.NewAuthority(repository.store)
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
			return scriptTerminalSourceRelease{}, false, releases.CorruptReleaseRecord()
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
		conditions: append(append([]etcdstore.Condition(nil), final.Conditions()...), executionGuards...),
		mutations:  cloneBlueprintCandidateMutations(final.Mutations()),
	}
	if task.Type == taskjournal.TaskUpdate {
		change.conditions = append(change.conditions, closingCondition)
		change.mutations = append(change.mutations, closingMutation)
	}
	return change, false, nil
}

func (repository *TaskRepository) beginBlueprintTerminalScriptSourceRelease(
	ctx context.Context,
	task TaskRecord,
	executions []scriptexecutions.ScriptExecutionRecord,
	values []*etcdstore.KeyValue,
	terminalAt time.Time,
) (scriptTerminalSourceRelease, error) {
	authority, err := scriptsourcepublication.NewAuthority(repository.store)
	if err != nil {
		return scriptTerminalSourceRelease{}, err
	}
	release, err := authority.PrepareNormalRelease(ctx, task.OperationID, sourceref.RetryDispositionForbidden)
	if err != nil {
		return scriptTerminalSourceRelease{}, err
	}
	conditions := append([]etcdstore.Condition(nil), release.Conditions()...)
	mutations := cloneBlueprintCandidateMutations(release.Mutations())
	release.Clear()
	for index, execution := range executions {
		if !terminalAt.After(execution.UpdatedAt) {
			etcdstore.ClearMutationValues(mutations)
			return scriptTerminalSourceRelease{}, errs.New(
				errs.KindStateConflict, "recovery parent Script execution timestamp changed",
			)
		}
		next := execution
		switch execution.State {
		case scriptexecutions.ScriptExecutionNotStarted:
			if execution.AssignmentID != "" || execution.StartAuthorized || !execution.ActiveReference {
				etcdstore.ClearMutationValues(mutations)
				return scriptTerminalSourceRelease{}, errs.New(
					errs.KindStateConflict, "recovery parent Script execution may already have started",
				)
			}
			outcome := scriptexecutions.ScriptOutcomeEvidence{
				Reason:     scriptexecutions.ScriptOutcomeParentFailureBeforeStart,
				ObservedAt: terminalAt,
			}
			cleanup := scriptexecutions.ScriptCleanupEvidence{
				ContainerAbsent:          true,
				BodyAbsent:               true,
				ExecutionDirectoryAbsent: true,
			}
			digest, digestErr := scriptControllerCleanupSHA256(outcome, cleanup)
			if digestErr != nil {
				etcdstore.ClearMutationValues(mutations)
				return scriptTerminalSourceRelease{}, digestErr
			}
			next.State = scriptexecutions.ScriptExecutionCleanupProven
			next.Outcome = &outcome
			next.Cleanup = &cleanup
			next.ControllerCleanup = scriptexecutions.ScriptControllerCleanupReleaseRecoveryParentFailure
			next.LastCheckpointSHA256 = digest
		case scriptexecutions.ScriptExecutionCleanupProven:
			if execution.AssignmentID == "" || execution.ControllerCleanup != "" || execution.ReconciliationRequired {
				etcdstore.ClearMutationValues(mutations)
				return scriptTerminalSourceRelease{}, errs.New(
					errs.KindStateConflict, "recovery parent Script cleanup authority changed",
				)
			}
		default:
			etcdstore.ClearMutationValues(mutations)
			return scriptTerminalSourceRelease{}, errs.New(
				errs.KindStateConflict, "recovery parent Script execution may already have started",
			)
		}
		next.ActiveReference = false
		next.UpdatedAt = terminalAt
		if scriptexecutions.ValidateScriptExecutionRecord(next) != nil ||
			!releaseRecoveryClosedScriptExecutionMatches(next, execution.AssignmentID) {
			etcdstore.ClearMutationValues(mutations)
			return scriptTerminalSourceRelease{}, releases.CorruptReleaseRecord()
		}
		encoded, encodeErr := recordcodec.Encode("script-execution", next)
		if encodeErr != nil {
			etcdstore.ClearMutationValues(mutations)
			return scriptTerminalSourceRelease{}, encodeErr
		}
		conditions = append(
			conditions,
			etcdstore.Condition{Key: values[index].Key, ModRevision: values[index].ModRevision},
		)
		mutations = append(
			mutations,
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: values[index].Key, Value: encoded},
		)
	}
	return scriptTerminalSourceRelease{conditions: conditions, mutations: mutations}, nil
}

func releaseRecoveryParentFailureExecutionMatches(record scriptexecutions.ScriptExecutionRecord) bool {
	if record.State != scriptexecutions.ScriptExecutionCleanupProven || record.AssignmentID != "" || record.StartAuthorized ||
		record.BodyPrepared != nil || record.ContainerCreated != nil || record.ActiveReference ||
		record.ReconciliationRequired || record.Outcome == nil || record.Cleanup == nil ||
		record.ControllerCleanup != scriptexecutions.ScriptControllerCleanupReleaseRecoveryParentFailure ||
		record.Outcome.Reason != scriptexecutions.ScriptOutcomeParentFailureBeforeStart || record.Outcome.ExitCode != nil ||
		record.Outcome.OutputTruncated || record.Outcome.ObservedAt.IsZero() ||
		!record.Cleanup.ContainerAbsent || !record.Cleanup.BodyAbsent ||
		!record.Cleanup.ExecutionDirectoryAbsent ||
		record.Cleanup.ContainerID != "" ||
		record.Cleanup.BodyDevice != 0 ||
		record.Cleanup.BodyInode != 0 ||
		record.Cleanup.BodyLeaf != "" ||
		!record.UpdatedAt.Equal(record.Outcome.ObservedAt) {
		return false
	}
	digest, err := scriptControllerCleanupSHA256(*record.Outcome, *record.Cleanup)
	return err == nil && record.LastCheckpointSHA256 == digest
}

func releaseRecoveryClosedScriptExecutionMatches(
	record scriptexecutions.ScriptExecutionRecord,
	assignmentID string,
) bool {
	if releaseRecoveryParentFailureExecutionMatches(record) {
		return true
	}
	return record.State == scriptexecutions.ScriptExecutionCleanupProven && record.AssignmentID == assignmentID &&
		record.ControllerCleanup == "" && !record.ActiveReference && !record.ReconciliationRequired &&
		scriptexecutions.ValidateScriptExecutionRecord(record) == nil
}
