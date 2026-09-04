package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type blueprintPendingAbortChange struct {
	applies    bool
	advanced   bool
	terminalAt time.Time
	conditions []Condition
	mutations  []Mutation
}

func (change *blueprintPendingAbortChange) clear() {
	if change == nil {
		return
	}
	clearMutationValues(change.mutations)
	*change = blueprintPendingAbortChange{}
}

func (repository *TaskRepository) prepareBlueprintScriptTaskClaimSourceAuthority(
	ctx context.Context,
	task TaskRecord,
	taskRevision int64,
	revision int64,
) ([]Condition, bool, error) {
	if !blueprintScriptTaskShape(task) {
		return nil, true, nil
	}
	steps, err := releaseHookExecutionSteps(task)
	if err != nil {
		return nil, false, err
	}
	if len(steps) == 0 {
		return nil, true, nil
	}
	key := scriptSourceRootKey(task.OperationID)
	read, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return nil, false, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		return nil, false, corruptReleaseRecord()
	}
	root, err := decodeScriptOperationSourceRoot(read.Values[0].Value)
	if err != nil || root.OperationID != task.OperationID {
		return nil, false, corruptReleaseRecord()
	}
	if root.Phase == ScriptOperationSourceReleasing && root.ReleasePath == ScriptSourceReleaseNormal &&
		(root.RetryDisposition == ScriptRetryDispositionAbandoned ||
			root.RetryDisposition == ScriptRetryDispositionForbidden) {
		return nil, false, nil
	}
	if root.Phase != ScriptOperationSourceActive || root.ReleasePath != ScriptSourceReleaseAbsent ||
		(root.RetryDisposition != ScriptRetryDispositionUndecided &&
			root.RetryDisposition != ScriptRetryDispositionTransferred) ||
		read.Values[0].ModRevision != taskRevision {
		return nil, false, corruptReleaseRecord()
	}
	return []Condition{{Key: key, ModRevision: read.Values[0].ModRevision}}, true, nil
}

func (repository *TaskRepository) prepareBlueprintPendingAbort(
	ctx context.Context,
	task Versioned[TaskRecord],
	requestedTerminalAt time.Time,
) (blueprintPendingAbortChange, error) {
	if !blueprintScriptTaskShape(task.Record) {
		return blueprintPendingAbortChange{}, nil
	}
	steps, err := releaseHookExecutionSteps(task.Record)
	if err != nil {
		return blueprintPendingAbortChange{}, err
	}
	if len(steps) == 0 {
		return blueprintPendingAbortChange{}, nil
	}
	keys := make([]string, 1, len(steps)+1)
	keys[0] = scriptSourceRootKey(task.Record.OperationID)
	for _, step := range steps {
		keys = append(keys, scriptExecutionKey(step.executionID))
	}
	read, err := repository.store.GetMany(ctx, GetManyRequest{Keys: keys, Revision: task.ReadRevision})
	if err != nil {
		return blueprintPendingAbortChange{}, err
	}
	if read == nil || read.ReadRevision != task.ReadRevision || len(read.Values) != len(keys) ||
		read.Values[0] == nil {
		return blueprintPendingAbortChange{}, corruptReleaseRecord()
	}
	root, err := decodeScriptOperationSourceRoot(read.Values[0].Value)
	if err != nil || root.OperationID != task.Record.OperationID {
		return blueprintPendingAbortChange{}, corruptReleaseRecord()
	}
	executions, err := decodeBlueprintPendingAbortExecutions(task.Record, steps, read.Values[1:])
	if err != nil {
		return blueprintPendingAbortChange{}, err
	}
	if root.Phase == ScriptOperationSourceActive {
		if root.ReleasePath != ScriptSourceReleaseAbsent ||
			(root.RetryDisposition != ScriptRetryDispositionUndecided &&
				root.RetryDisposition != ScriptRetryDispositionTransferred) ||
			read.Values[0].ModRevision != task.Revision {
			return blueprintPendingAbortChange{}, corruptReleaseRecord()
		}
		return repository.beginBlueprintPendingAbort(ctx, task, requestedTerminalAt, steps, executions, read.Values)
	}
	if root.Phase != ScriptOperationSourceReleasing || root.ReleasePath != ScriptSourceReleaseNormal ||
		root.RetryDisposition != ScriptRetryDispositionAbandoned {
		return blueprintPendingAbortChange{}, corruptReleaseRecord()
	}
	terminalAt, err := blueprintPendingAbortTerminalAt(task.Record, steps, executions)
	if err != nil {
		return blueprintPendingAbortChange{}, err
	}
	authority, err := newScriptSourceReferenceAuthority(repository.store)
	if err != nil {
		return blueprintPendingAbortChange{}, err
	}
	guards := make([]Condition, 0, len(executions)+1)
	guards = append(guards, Condition{Key: taskKey(task.Record.ID), ModRevision: task.Revision})
	for index := range executions {
		guards = append(guards, Condition{Key: read.Values[index+1].Key, ModRevision: read.Values[index+1].ModRevision})
	}
	processed, drained, err := authority.ReleaseNext(ctx, task.Record.OperationID, guards)
	if err != nil {
		if errors.Is(err, errs.New(errs.KindStateConflict, "")) {
			return blueprintPendingAbortChange{applies: true, advanced: true, terminalAt: terminalAt}, nil
		}
		return blueprintPendingAbortChange{}, err
	}
	if processed {
		return blueprintPendingAbortChange{applies: true, advanced: true, terminalAt: terminalAt}, nil
	}
	if !drained {
		return blueprintPendingAbortChange{}, corruptReleaseRecord()
	}
	final, err := authority.PrepareReleaseFinalization(ctx, task.Record.OperationID)
	if err != nil {
		return blueprintPendingAbortChange{}, err
	}
	conditions := append([]Condition(nil), final.conditions...)
	conditions = append(conditions, guards[1:]...)
	mutations := append([]Mutation(nil), final.mutations...)
	final.Clear()
	return blueprintPendingAbortChange{
		applies: true, terminalAt: terminalAt, conditions: conditions, mutations: mutations,
	}, nil
}

func (repository *TaskRepository) beginBlueprintPendingAbort(
	ctx context.Context,
	task Versioned[TaskRecord],
	requestedTerminalAt time.Time,
	steps []releaseHookExecutionStep,
	executions []ScriptExecutionRecord,
	values []*KeyValue,
) (blueprintPendingAbortChange, error) {
	terminal, err := transitionTaskStatus(task.Record, TaskStatusPending, TaskStatusAborted, requestedTerminalAt)
	if err != nil {
		return blueprintPendingAbortChange{}, err
	}
	terminalAt := *terminal.FinishedAt
	authority, err := newScriptSourceReferenceAuthority(repository.store)
	if err != nil {
		return blueprintPendingAbortChange{}, err
	}
	release, err := authority.PrepareNormalRelease(
		ctx,
		task.Record.OperationID,
		ScriptRetryDispositionAbandoned,
	)
	if err != nil {
		return blueprintPendingAbortChange{}, err
	}
	defer release.Clear()
	if len(release.conditions) == 0 || len(release.mutations) == 0 {
		return blueprintPendingAbortChange{applies: true, advanced: true, terminalAt: terminalAt}, nil
	}
	conditions := append([]Condition{{Key: taskKey(task.Record.ID), ModRevision: task.Revision}}, release.conditions...)
	mutations := append([]Mutation(nil), release.mutations...)
	defer clearMutationValues(mutations)
	for index, execution := range executions {
		next, encodeErr := abortBlueprintScriptExecutionBeforeStart(
			execution, task.Record, steps[index], terminalAt,
		)
		if encodeErr != nil {
			return blueprintPendingAbortChange{}, encodeErr
		}
		encoded, encodeErr := encodeEnvelope("script-execution", next)
		if encodeErr != nil {
			return blueprintPendingAbortChange{}, encodeErr
		}
		conditions = append(conditions, Condition{Key: values[index+1].Key, ModRevision: values[index+1].ModRevision})
		mutations = append(mutations, Mutation{Type: MutationPut, Key: values[index+1].Key, Value: encoded})
	}
	transaction, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return blueprintPendingAbortChange{}, err
	}
	clearKeyValues(transaction.FailureReads)
	return blueprintPendingAbortChange{applies: true, advanced: true, terminalAt: terminalAt}, nil
}

func decodeBlueprintPendingAbortExecutions(
	task TaskRecord,
	steps []releaseHookExecutionStep,
	values []*KeyValue,
) ([]ScriptExecutionRecord, error) {
	if len(values) != len(steps) {
		return nil, corruptReleaseRecord()
	}
	result := make([]ScriptExecutionRecord, len(steps))
	for index, value := range values {
		if value == nil {
			return nil, corruptReleaseRecord()
		}
		record, err := decodeEnvelope[ScriptExecutionRecord](value.Value, "script-execution")
		if err != nil || validateScriptExecutionRecord(record) != nil || record.ID != steps[index].executionID ||
			record.CurrentTaskID != task.ID || record.OperationID != task.OperationID ||
			record.StepID != steps[index].stepID || record.PlanHash != task.PlanHash {
			return nil, corruptReleaseRecord()
		}
		result[index] = record
	}
	return result, nil
}

func abortBlueprintScriptExecutionBeforeStart(
	record ScriptExecutionRecord,
	task TaskRecord,
	step releaseHookExecutionStep,
	terminalAt time.Time,
) (ScriptExecutionRecord, error) {
	if task.Status != TaskStatusPending || !blueprintScriptTaskShape(task) || !taskOwnsScriptExecution(task, record) ||
		record.State != ScriptExecutionNotStarted || record.AssignmentID != "" || record.StartAuthorized ||
		!record.ActiveReference || record.CurrentTaskID != task.ID || record.OperationID != task.OperationID ||
		record.ID != step.executionID || record.StepID != step.stepID || !terminalAt.After(record.UpdatedAt) {
		return ScriptExecutionRecord{}, errs.New(
			errs.KindStateConflict,
			"pending Blueprint Script execution is not abortable before start",
		)
	}
	outcome := ScriptOutcomeEvidence{Reason: ScriptOutcomeAbortBeforeStart, ObservedAt: terminalAt}
	cleanup := ScriptCleanupEvidence{ContainerAbsent: true, BodyAbsent: true, ExecutionDirectoryAbsent: true}
	digest, err := blueprintPendingAbortCheckpointSHA256(outcome, cleanup)
	if err != nil {
		return ScriptExecutionRecord{}, err
	}
	next := record
	next.State = ScriptExecutionCleanupProven
	next.Outcome = &outcome
	next.Cleanup = &cleanup
	next.ControllerCleanup = ScriptControllerCleanupBlueprintPendingAbort
	next.LastCheckpointSHA256 = digest
	next.ActiveReference = false
	next.UpdatedAt = terminalAt
	if validateScriptExecutionRecord(next) != nil {
		return ScriptExecutionRecord{}, corruptReleaseRecord()
	}
	return next, nil
}

func blueprintPendingAbortTerminalAt(
	task TaskRecord,
	steps []releaseHookExecutionStep,
	executions []ScriptExecutionRecord,
) (time.Time, error) {
	var terminalAt time.Time
	for index, execution := range executions {
		if execution.Outcome == nil || !blueprintPendingAbortExecutionMatches(
			execution, task, steps[index], execution.Outcome.ObservedAt,
		) {
			return time.Time{}, corruptReleaseRecord()
		}
		if terminalAt.IsZero() {
			terminalAt = execution.Outcome.ObservedAt
			continue
		}
		if !terminalAt.Equal(execution.Outcome.ObservedAt) {
			return time.Time{}, corruptReleaseRecord()
		}
	}
	if terminalAt.IsZero() {
		return time.Time{}, corruptReleaseRecord()
	}
	return terminalAt, nil
}

func blueprintPendingAbortExecutionMatches(
	record ScriptExecutionRecord,
	task TaskRecord,
	step releaseHookExecutionStep,
	terminalAt time.Time,
) bool {
	if record.CurrentTaskID != task.ID || record.OperationID != task.OperationID || record.ID != step.executionID ||
		record.StepID != step.stepID || record.State != ScriptExecutionCleanupProven || record.AssignmentID != "" ||
		record.StartAuthorized || record.BodyPrepared != nil || record.ContainerCreated != nil || record.ActiveReference ||
		record.ReconciliationRequired || record.Outcome == nil || record.Cleanup == nil ||
		record.ControllerCleanup != ScriptControllerCleanupBlueprintPendingAbort ||
		record.Outcome.Reason != ScriptOutcomeAbortBeforeStart || record.Outcome.ExitCode != nil ||
		record.Outcome.OutputTruncated || !record.Outcome.ObservedAt.Equal(terminalAt) ||
		!record.Cleanup.ContainerAbsent || !record.Cleanup.BodyAbsent || !record.Cleanup.ExecutionDirectoryAbsent ||
		record.Cleanup.ContainerID != "" || record.Cleanup.BodyDevice != 0 || record.Cleanup.BodyInode != 0 ||
		record.Cleanup.BodyLeaf != "" || !record.UpdatedAt.Equal(terminalAt) {
		return false
	}
	digest, err := blueprintPendingAbortCheckpointSHA256(*record.Outcome, *record.Cleanup)
	return err == nil && record.LastCheckpointSHA256 == digest
}

func blueprintPendingAbortCheckpointSHA256(
	outcome ScriptOutcomeEvidence,
	cleanup ScriptCleanupEvidence,
) (string, error) {
	payload, err := json.Marshal(struct {
		Schema  int                   `json:"schema"`
		Outcome ScriptOutcomeEvidence `json:"outcome"`
		Cleanup ScriptCleanupEvidence `json:"cleanup"`
	}{Schema: 1, Outcome: outcome, Cleanup: cleanup})
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(payload)
	clear(payload)
	return hex.EncodeToString(digest[:]), nil
}

func (repository *TaskRepository) validateBlueprintPendingAbortReplay(
	ctx context.Context,
	task Versioned[TaskRecord],
) error {
	if !blueprintScriptTaskShape(task.Record) {
		return nil
	}
	steps, err := releaseHookExecutionSteps(task.Record)
	if err != nil || len(steps) == 0 {
		return err
	}
	keys := make([]string, 1, len(steps)+1)
	keys[0] = scriptSourceRootKey(task.Record.OperationID)
	for _, step := range steps {
		keys = append(keys, scriptExecutionKey(step.executionID))
	}
	read, err := repository.store.GetMany(ctx, GetManyRequest{Keys: keys, Revision: task.ReadRevision})
	if err != nil {
		return err
	}
	if read == nil || read.ReadRevision != task.ReadRevision || len(read.Values) != len(keys) || read.Values[0] != nil {
		return corruptReleaseRecord()
	}
	executions, err := decodeBlueprintPendingAbortExecutions(task.Record, steps, read.Values[1:])
	if err != nil {
		return err
	}
	terminalAt, err := blueprintPendingAbortTerminalAt(task.Record, steps, executions)
	if err != nil || task.Record.FinishedAt == nil || !terminalAt.Equal(*task.Record.FinishedAt) {
		return corruptReleaseRecord()
	}
	return nil
}
