package etcd

import (
	"context"
	"errors"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type blueprintRecoveryScriptSourceRelease struct {
	conditions []Condition
	mutations  []Mutation
}

func (change *blueprintRecoveryScriptSourceRelease) clear() {
	if change == nil {
		return
	}
	clearMutationValues(change.mutations)
	*change = blueprintRecoveryScriptSourceRelease{}
}

// prepareBlueprintRecoveryScriptSourceRelease advances the existing bounded
// source-reference release protocol without surrendering Task recovery
// authority. Only its final fragment joins the Task terminal transaction.
func (repository *TaskRepository) prepareBlueprintRecoveryScriptSourceRelease(
	ctx context.Context,
	task TaskRecord,
	taskValue *KeyValue,
	assignment TaskAssignmentRecord,
	assignmentValue *KeyValue,
	assignmentIndexValue *KeyValue,
	recovery releaseRecoveryAcknowledgement,
	terminalAt time.Time,
	revision int64,
) (blueprintRecoveryScriptSourceRelease, bool, error) {
	steps, err := releaseHookExecutionSteps(task)
	if err != nil || len(steps) == 0 {
		return blueprintRecoveryScriptSourceRelease{}, false, err
	}
	if !recovery.final || recovery.value == nil || taskValue == nil || assignmentValue == nil ||
		assignmentIndexValue == nil || revision <= 0 || assignment.ExecutionMode != TaskExecutionModeRecoveryOnly {
		return blueprintRecoveryScriptSourceRelease{}, false, corruptTaskAssignment()
	}
	lifecycleKey, lifecycleValue, _, err := repository.assignmentLifecycleIndexAtRevision(ctx, assignment, assignmentValue, revision)
	if err != nil {
		return blueprintRecoveryScriptSourceRelease{}, false, err
	}
	environmentID, materializes, err := taskEnvironmentWriter(task)
	if err != nil {
		return blueprintRecoveryScriptSourceRelease{}, false, err
	}
	if !materializes {
		environmentID = task.Owner.EnvironmentID
	}
	if environmentID == "" || environmentID != task.Owner.EnvironmentID {
		return blueprintRecoveryScriptSourceRelease{}, false, corruptTaskAssignment()
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
	read, err := repository.store.GetMany(ctx, GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return blueprintRecoveryScriptSourceRelease{}, false, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
		return blueprintRecoveryScriptSourceRelease{}, false, corruptReleaseRecord()
	}
	for _, value := range read.Values {
		if value == nil {
			return blueprintRecoveryScriptSourceRelease{}, false, corruptReleaseRecord()
		}
	}
	root, err := decodeScriptOperationSourceRoot(read.Values[0].Value)
	if err != nil || root.OperationID != task.OperationID {
		return blueprintRecoveryScriptSourceRelease{}, false, corruptReleaseRecord()
	}
	activeTaskID, err := decodeTaskReference(read.Values[2].Value)
	if err != nil || activeTaskID != task.ID {
		return blueprintRecoveryScriptSourceRelease{}, false, corruptTaskAssignment()
	}
	if materializes {
		writer, decodeErr := decodeTaskMaterializationWriter(read.Values[writerIndex].Value)
		if decodeErr != nil || validateTaskMaterializationWriterForTask(writer, task, environmentID) != nil {
			return blueprintRecoveryScriptSourceRelease{}, false, corruptTaskMaterializationWriter()
		}
	}
	executions := make([]ScriptExecutionRecord, len(steps))
	for index, step := range steps {
		value := read.Values[index+executionOffset]
		record, decodeErr := decodeEnvelope[ScriptExecutionRecord](value.Value, "script-execution")
		if decodeErr != nil || validateScriptExecutionRecord(record) != nil || !taskOwnsScriptExecution(task, record) ||
			record.ID != step.executionID || record.StepID != step.stepID || record.CurrentTaskID != task.ID ||
			record.OperationID != task.OperationID || record.PlanHash != task.PlanHash {
			return blueprintRecoveryScriptSourceRelease{}, false, corruptReleaseRecord()
		}
		executions[index] = record
	}
	guards := []Condition{
		{Key: taskKey(task.ID), ModRevision: taskValue.ModRevision},
		{Key: taskExecutionClaimKey(assignment.Executor, assignment.AgentID, task.ID), ModRevision: assignmentValue.ModRevision},
		{Key: taskAssignmentIndexKey(task.ID), ModRevision: assignmentIndexValue.ModRevision},
		{Key: releaseRecoveryKey(task.ID), ModRevision: recovery.value.ModRevision},
		{Key: lifecycleKey, ModRevision: lifecycleValue.ModRevision},
		{Key: environmentMutationEpochKey(environmentID), ModRevision: read.Values[1].ModRevision},
		{Key: taskActiveOperationKey(task.OperationID), ModRevision: read.Values[2].ModRevision},
	}
	if materializes {
		guards = append(guards, Condition{
			Key: taskMaterializationWriterKey(environmentID), ModRevision: read.Values[writerIndex].ModRevision,
		})
	}
	for index := range steps {
		guards = append(guards, Condition{
			Key: read.Values[index+executionOffset].Key, ModRevision: read.Values[index+executionOffset].ModRevision,
		})
	}
	if root.Phase == ScriptOperationSourceActive {
		if root.ReleasePath != ScriptSourceReleaseAbsent ||
			(root.RetryDisposition != ScriptRetryDispositionUndecided && root.RetryDisposition != ScriptRetryDispositionTransferred) {
			return blueprintRecoveryScriptSourceRelease{}, false, corruptReleaseRecord()
		}
		release, releaseErr := repository.beginBlueprintRecoveryScriptSourceRelease(
			ctx, task, executions, read.Values[executionOffset:], terminalAt,
		)
		if releaseErr != nil {
			return blueprintRecoveryScriptSourceRelease{}, false, releaseErr
		}
		defer release.clear()
		transaction, transactErr := repository.store.Transact(ctx, append(guards, release.conditions...), release.mutations)
		if transactErr != nil {
			return blueprintRecoveryScriptSourceRelease{}, false, transactErr
		}
		clearKeyValues(transaction.FailureReads)
		return blueprintRecoveryScriptSourceRelease{}, true, nil
	}
	if root.Phase != ScriptOperationSourceReleasing || root.ReleasePath != ScriptSourceReleaseNormal ||
		root.RetryDisposition != ScriptRetryDispositionForbidden {
		return blueprintRecoveryScriptSourceRelease{}, false, corruptReleaseRecord()
	}
	for _, execution := range executions {
		if !releaseRecoveryClosedScriptExecutionMatches(execution, assignment.AssignmentID) {
			return blueprintRecoveryScriptSourceRelease{}, false, corruptReleaseRecord()
		}
	}
	authority, err := newScriptSourceReferenceAuthority(repository.store)
	if err != nil {
		return blueprintRecoveryScriptSourceRelease{}, false, err
	}
	processed, drained, err := authority.ReleaseNext(ctx, task.OperationID, guards)
	if err != nil {
		if errors.Is(err, errs.New(errs.KindStateConflict, "")) {
			return blueprintRecoveryScriptSourceRelease{}, true, nil
		}
		return blueprintRecoveryScriptSourceRelease{}, false, err
	}
	if processed {
		return blueprintRecoveryScriptSourceRelease{}, true, nil
	}
	if !drained {
		return blueprintRecoveryScriptSourceRelease{}, false, corruptReleaseRecord()
	}
	final, err := authority.PrepareReleaseFinalization(ctx, task.OperationID)
	if err != nil {
		return blueprintRecoveryScriptSourceRelease{}, false, err
	}
	defer final.Clear()
	return blueprintRecoveryScriptSourceRelease{
		conditions: append(append([]Condition(nil), final.conditions...), guards...),
		mutations:  cloneBlueprintCandidateMutations(final.mutations),
	}, false, nil
}

func (repository *TaskRepository) beginBlueprintRecoveryScriptSourceRelease(
	ctx context.Context,
	task TaskRecord,
	executions []ScriptExecutionRecord,
	values []*KeyValue,
	terminalAt time.Time,
) (blueprintRecoveryScriptSourceRelease, error) {
	authority, err := newScriptSourceReferenceAuthority(repository.store)
	if err != nil {
		return blueprintRecoveryScriptSourceRelease{}, err
	}
	release, err := authority.PrepareNormalRelease(ctx, task.OperationID, ScriptRetryDispositionForbidden)
	if err != nil {
		return blueprintRecoveryScriptSourceRelease{}, err
	}
	conditions := append([]Condition(nil), release.conditions...)
	mutations := cloneBlueprintCandidateMutations(release.mutations)
	release.Clear()
	for index, execution := range executions {
		if !terminalAt.After(execution.UpdatedAt) {
			clearMutationValues(mutations)
			return blueprintRecoveryScriptSourceRelease{}, errs.New(
				errs.KindStateConflict, "recovery parent Script execution timestamp changed",
			)
		}
		next := execution
		switch execution.State {
		case ScriptExecutionNotStarted:
			if execution.AssignmentID != "" || execution.StartAuthorized || !execution.ActiveReference {
				clearMutationValues(mutations)
				return blueprintRecoveryScriptSourceRelease{}, errs.New(
					errs.KindStateConflict, "recovery parent Script execution may already have started",
				)
			}
			outcome := ScriptOutcomeEvidence{Reason: ScriptOutcomeParentFailureBeforeStart, ObservedAt: terminalAt}
			cleanup := ScriptCleanupEvidence{ContainerAbsent: true, BodyAbsent: true, ExecutionDirectoryAbsent: true}
			digest, digestErr := blueprintPendingAbortCheckpointSHA256(outcome, cleanup)
			if digestErr != nil {
				clearMutationValues(mutations)
				return blueprintRecoveryScriptSourceRelease{}, digestErr
			}
			next.State = ScriptExecutionCleanupProven
			next.Outcome = &outcome
			next.Cleanup = &cleanup
			next.ControllerCleanup = ScriptControllerCleanupReleaseRecoveryParentFailure
			next.LastCheckpointSHA256 = digest
		case ScriptExecutionCleanupProven:
			if execution.AssignmentID == "" || execution.ControllerCleanup != "" || execution.ReconciliationRequired {
				clearMutationValues(mutations)
				return blueprintRecoveryScriptSourceRelease{}, errs.New(
					errs.KindStateConflict, "recovery parent Script cleanup authority changed",
				)
			}
		default:
			clearMutationValues(mutations)
			return blueprintRecoveryScriptSourceRelease{}, errs.New(
				errs.KindStateConflict, "recovery parent Script execution may already have started",
			)
		}
		next.ActiveReference = false
		next.UpdatedAt = terminalAt
		if validateScriptExecutionRecord(next) != nil || !releaseRecoveryClosedScriptExecutionMatches(next, execution.AssignmentID) {
			clearMutationValues(mutations)
			return blueprintRecoveryScriptSourceRelease{}, corruptReleaseRecord()
		}
		encoded, encodeErr := encodeEnvelope("script-execution", next)
		if encodeErr != nil {
			clearMutationValues(mutations)
			return blueprintRecoveryScriptSourceRelease{}, encodeErr
		}
		conditions = append(conditions, Condition{Key: values[index].Key, ModRevision: values[index].ModRevision})
		mutations = append(mutations, Mutation{Type: MutationPut, Key: values[index].Key, Value: encoded})
	}
	return blueprintRecoveryScriptSourceRelease{conditions: conditions, mutations: mutations}, nil
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
	digest, err := blueprintPendingAbortCheckpointSHA256(*record.Outcome, *record.Cleanup)
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
