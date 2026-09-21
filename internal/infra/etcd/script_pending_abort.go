package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	releases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	scriptexecutions "github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	scriptsourceevidence "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	sourceref "github.com/AlanD20/groundplane/internal/infra/scriptsourcereference"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type pendingScriptAbortChange struct {
	applies    bool
	advanced   bool
	terminalAt time.Time
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
}

func (change *pendingScriptAbortChange) clear() {
	if change == nil {
		return
	}
	clearMutationValues(change.mutations)
	*change = pendingScriptAbortChange{}
}

func (repository *TaskRepository) prepareScriptTaskClaimSourceAuthority(
	ctx context.Context,
	task TaskRecord,
	taskRevision int64,
	revision int64,
) (ScriptSourceReleaseFragment, bool, error) {
	if task.Type != taskjournal.TaskScript && !blueprintScriptTaskShape(task) {
		return ScriptSourceReleaseFragment{}, true, nil
	}
	steps, err := preparedScriptExecutionSteps(task)
	if err != nil {
		return ScriptSourceReleaseFragment{}, false, err
	}
	if len(steps) == 0 {
		return ScriptSourceReleaseFragment{}, true, nil
	}
	key := scriptsourceevidence.ScriptSourceRootKey(task.OperationID)
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return ScriptSourceReleaseFragment{}, false, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		return ScriptSourceReleaseFragment{}, false, releases.CorruptReleaseRecord()
	}
	root, err := scriptsourceevidence.DecodeScriptOperationSourceRoot(read.Values[0].Value)
	if err != nil || root.OperationID != task.OperationID {
		return ScriptSourceReleaseFragment{}, false, releases.CorruptReleaseRecord()
	}
	if root.Phase == scriptsourceevidence.ScriptOperationSourceReleasing && root.ReleasePath == scriptsourceevidence.ScriptSourceReleaseNormal &&
		(root.RetryDisposition == sourceref.RetryDispositionAbandoned ||
			root.RetryDisposition == sourceref.RetryDispositionForbidden) {
		return ScriptSourceReleaseFragment{}, false, nil
	}
	if root.Phase != scriptsourceevidence.ScriptOperationSourceActive || root.ReleasePath != scriptsourceevidence.ScriptSourceReleaseAbsent ||
		(root.RetryDisposition != sourceref.RetryDispositionUndecided &&
			root.RetryDisposition != sourceref.RetryDispositionTransferred) ||
		read.Values[0].ModRevision != taskRevision {
		return ScriptSourceReleaseFragment{}, false, releases.CorruptReleaseRecord()
	}
	change := ScriptSourceReleaseFragment{conditions: []etcdstore.Condition{{Key: key, ModRevision: read.Values[0].ModRevision}}}
	if task.Type == taskjournal.TaskScript {
		execution, value, err := (&ScriptRepository{store: repository.store}).manualScriptExecutionAtRevision(
			ctx,
			task,
			revision,
		)
		if err != nil || !manualScriptRootMatches(execution, root) || execution.State != scriptexecutions.ScriptExecutionNotStarted ||
			!execution.ActiveReference {
			return ScriptSourceReleaseFragment{}, false, errs.New(
				errs.KindInternal,
				"manual Script claim source authority is corrupt",
			)
		}
		if root.RetryDisposition == sourceref.RetryDispositionTransferred {
			authority, err := newScriptSourceReferenceAuthority(repository.store)
			if err != nil {
				return ScriptSourceReleaseFragment{}, false, err
			}
			change, err = authority.PrepareRetryActivation(ctx, task.OperationID, read.Values[0].ModRevision)
			if err != nil {
				return ScriptSourceReleaseFragment{}, false, err
			}
		}
		change.conditions = append(change.conditions, etcdstore.Condition{Key: value.Key, ModRevision: value.ModRevision})
	}
	return change, true, nil
}

func (repository *TaskRepository) preparePendingScriptAbort(
	ctx context.Context,
	task etcdstore.Versioned[TaskRecord],
	requestedTerminalAt time.Time,
) (pendingScriptAbortChange, error) {
	if task.Record.Type != taskjournal.TaskScript && !blueprintScriptTaskShape(task.Record) {
		return pendingScriptAbortChange{}, nil
	}
	steps, err := preparedScriptExecutionSteps(task.Record)
	if err != nil {
		return pendingScriptAbortChange{}, err
	}
	if len(steps) == 0 {
		return pendingScriptAbortChange{}, nil
	}
	keys := make([]string, 1, len(steps)+1)
	keys[0] = scriptsourceevidence.ScriptSourceRootKey(task.Record.OperationID)
	for _, step := range steps {
		keys = append(keys, scriptexecutions.ScriptExecutionKey(step.executionID))
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: task.ReadRevision})
	if err != nil {
		return pendingScriptAbortChange{}, err
	}
	if read == nil || read.ReadRevision != task.ReadRevision || len(read.Values) != len(keys) ||
		read.Values[0] == nil {
		return pendingScriptAbortChange{}, releases.CorruptReleaseRecord()
	}
	root, err := scriptsourceevidence.DecodeScriptOperationSourceRoot(read.Values[0].Value)
	if err != nil || root.OperationID != task.Record.OperationID {
		return pendingScriptAbortChange{}, releases.CorruptReleaseRecord()
	}
	executions, err := decodePendingScriptAbortExecutions(task.Record, steps, read.Values[1:])
	if err != nil {
		return pendingScriptAbortChange{}, err
	}
	if task.Record.Type == taskjournal.TaskScript && (len(executions) != 1 || !manualScriptRootMatches(executions[0], root)) {
		return pendingScriptAbortChange{}, errs.New(
			errs.KindInternal,
			"manual Script Abort source authority is corrupt",
		)
	}
	if root.Phase == scriptsourceevidence.ScriptOperationSourceActive {
		if root.ReleasePath != scriptsourceevidence.ScriptSourceReleaseAbsent ||
			(root.RetryDisposition != sourceref.RetryDispositionUndecided &&
				root.RetryDisposition != sourceref.RetryDispositionTransferred) ||
			read.Values[0].ModRevision != task.Revision {
			return pendingScriptAbortChange{}, releases.CorruptReleaseRecord()
		}
		return repository.beginPendingScriptAbort(ctx, task, requestedTerminalAt, steps, executions, read.Values)
	}
	if root.Phase != scriptsourceevidence.ScriptOperationSourceReleasing || root.ReleasePath != scriptsourceevidence.ScriptSourceReleaseNormal ||
		root.RetryDisposition != sourceref.RetryDispositionAbandoned {
		return pendingScriptAbortChange{}, releases.CorruptReleaseRecord()
	}
	terminalAt, err := pendingScriptAbortTerminalAt(task.Record, steps, executions)
	if err != nil {
		return pendingScriptAbortChange{}, err
	}
	authority, err := newScriptSourceReferenceAuthority(repository.store)
	if err != nil {
		return pendingScriptAbortChange{}, err
	}
	guards := make([]etcdstore.Condition, 0, len(executions)+1)
	guards = append(guards, etcdstore.Condition{Key: taskjournal.TaskStorageKey(task.Record.ID), ModRevision: task.Revision})
	for index := range executions {
		guards = append(guards, etcdstore.Condition{Key: read.Values[index+1].Key, ModRevision: read.Values[index+1].ModRevision})
	}
	processed, drained, err := authority.ReleaseNext(ctx, task.Record.OperationID, guards)
	if err != nil {
		if errors.Is(err, errs.New(errs.KindStateConflict, "")) {
			return pendingScriptAbortChange{applies: true, advanced: true, terminalAt: terminalAt}, nil
		}
		return pendingScriptAbortChange{}, err
	}
	if processed {
		return pendingScriptAbortChange{applies: true, advanced: true, terminalAt: terminalAt}, nil
	}
	if !drained {
		return pendingScriptAbortChange{}, releases.CorruptReleaseRecord()
	}
	final, err := authority.PrepareReleaseFinalization(ctx, task.Record.OperationID)
	if err != nil {
		return pendingScriptAbortChange{}, err
	}
	conditions := append([]etcdstore.Condition(nil), final.conditions...)
	conditions = append(conditions, guards[1:]...)
	mutations := append([]etcdstore.Mutation(nil), final.mutations...)
	final.Clear()
	return pendingScriptAbortChange{
		applies: true, terminalAt: terminalAt, conditions: conditions, mutations: mutations,
	}, nil
}

func (repository *TaskRepository) beginPendingScriptAbort(
	ctx context.Context,
	task etcdstore.Versioned[TaskRecord],
	requestedTerminalAt time.Time,
	steps []releaseHookExecutionStep,
	executions []scriptexecutions.ScriptExecutionRecord,
	values []*etcdstore.KeyValue,
) (pendingScriptAbortChange, error) {
	terminal, err := TransitionTaskStatus(task.Record, taskjournal.TaskStatusPending, taskjournal.TaskStatusAborted, requestedTerminalAt)
	if err != nil {
		return pendingScriptAbortChange{}, err
	}
	terminalAt := *terminal.FinishedAt
	authority, err := newScriptSourceReferenceAuthority(repository.store)
	if err != nil {
		return pendingScriptAbortChange{}, err
	}
	release, err := authority.PrepareNormalRelease(
		ctx,
		task.Record.OperationID,
		sourceref.RetryDispositionAbandoned,
	)
	if err != nil {
		return pendingScriptAbortChange{}, err
	}
	defer release.Clear()
	if len(release.conditions) == 0 || len(release.mutations) == 0 {
		return pendingScriptAbortChange{applies: true, advanced: true, terminalAt: terminalAt}, nil
	}
	conditions := append([]etcdstore.Condition{{Key: taskjournal.TaskStorageKey(task.Record.ID), ModRevision: task.Revision}}, release.conditions...)
	mutations := append([]etcdstore.Mutation(nil), release.mutations...)
	defer clearMutationValues(mutations)
	for index, execution := range executions {
		next, encodeErr := abortScriptExecutionBeforeStart(
			execution, task.Record, steps[index], terminalAt,
		)
		if encodeErr != nil {
			return pendingScriptAbortChange{}, encodeErr
		}
		encoded, encodeErr := recordcodec.Encode("script-execution", next)
		if encodeErr != nil {
			return pendingScriptAbortChange{}, encodeErr
		}
		conditions = append(conditions, etcdstore.Condition{Key: values[index+1].Key, ModRevision: values[index+1].ModRevision})
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: values[index+1].Key, Value: encoded})
	}
	transaction, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return pendingScriptAbortChange{}, err
	}
	etcdstore.ClearValues(transaction.FailureReads)
	return pendingScriptAbortChange{applies: true, advanced: true, terminalAt: terminalAt}, nil
}

func decodePendingScriptAbortExecutions(
	task TaskRecord,
	steps []releaseHookExecutionStep,
	values []*etcdstore.KeyValue,
) ([]scriptexecutions.ScriptExecutionRecord, error) {
	if len(values) != len(steps) {
		return nil, releases.CorruptReleaseRecord()
	}
	result := make([]scriptexecutions.ScriptExecutionRecord, len(steps))
	for index, value := range values {
		if value == nil {
			return nil, releases.CorruptReleaseRecord()
		}
		record, err := recordcodec.Decode[scriptexecutions.ScriptExecutionRecord](value.Value, "script-execution")
		if err != nil || scriptexecutions.ValidateScriptExecutionRecord(record) != nil || record.ID != steps[index].executionID ||
			record.CurrentTaskID != task.ID || record.OperationID != task.OperationID ||
			record.StepID != steps[index].stepID || record.PlanHash != task.PlanHash {
			return nil, releases.CorruptReleaseRecord()
		}
		result[index] = record
	}
	return result, nil
}

func abortScriptExecutionBeforeStart(
	record scriptexecutions.ScriptExecutionRecord,
	task TaskRecord,
	step releaseHookExecutionStep,
	terminalAt time.Time,
) (scriptexecutions.ScriptExecutionRecord, error) {
	if task.Status != taskjournal.TaskStatusPending ||
		(task.Type != taskjournal.TaskScript && !blueprintScriptTaskShape(task)) ||
		!taskOwnsScriptExecution(task, record) ||
		record.State != scriptexecutions.ScriptExecutionNotStarted || record.AssignmentID != "" || record.StartAuthorized ||
		!record.ActiveReference ||
		record.CurrentTaskID != task.ID ||
		record.OperationID != task.OperationID ||
		record.ID != step.executionID ||
		record.StepID != step.stepID ||
		!terminalAt.After(record.UpdatedAt) {
		return scriptexecutions.ScriptExecutionRecord{}, errs.New(
			errs.KindStateConflict,
			"pending Blueprint Script execution is not abortable before start",
		)
	}
	outcome := scriptexecutions.ScriptOutcomeEvidence{Reason: scriptexecutions.ScriptOutcomeAbortBeforeStart, ObservedAt: terminalAt}
	cleanup := scriptexecutions.ScriptCleanupEvidence{ContainerAbsent: true, BodyAbsent: true, ExecutionDirectoryAbsent: true}
	digest, err := scriptControllerCleanupSHA256(outcome, cleanup)
	if err != nil {
		return scriptexecutions.ScriptExecutionRecord{}, err
	}
	next := record
	next.State = scriptexecutions.ScriptExecutionCleanupProven
	next.Outcome = &outcome
	next.Cleanup = &cleanup
	next.ControllerCleanup = pendingScriptCleanupAuthority(task)
	next.LastCheckpointSHA256 = digest
	next.ActiveReference = false
	next.UpdatedAt = terminalAt
	if scriptexecutions.ValidateScriptExecutionRecord(next) != nil {
		return scriptexecutions.ScriptExecutionRecord{}, releases.CorruptReleaseRecord()
	}
	return next, nil
}

func pendingScriptAbortTerminalAt(
	task TaskRecord,
	steps []releaseHookExecutionStep,
	executions []scriptexecutions.ScriptExecutionRecord,
) (time.Time, error) {
	var terminalAt time.Time
	for index, execution := range executions {
		if execution.Outcome == nil || !pendingScriptAbortExecutionMatches(
			execution, task, steps[index], execution.Outcome.ObservedAt,
		) {
			return time.Time{}, releases.CorruptReleaseRecord()
		}
		if terminalAt.IsZero() {
			terminalAt = execution.Outcome.ObservedAt
			continue
		}
		if !terminalAt.Equal(execution.Outcome.ObservedAt) {
			return time.Time{}, releases.CorruptReleaseRecord()
		}
	}
	if terminalAt.IsZero() {
		return time.Time{}, releases.CorruptReleaseRecord()
	}
	return terminalAt, nil
}

func pendingScriptAbortExecutionMatches(
	record scriptexecutions.ScriptExecutionRecord,
	task TaskRecord,
	step releaseHookExecutionStep,
	terminalAt time.Time,
) bool {
	if record.CurrentTaskID != task.ID || record.OperationID != task.OperationID || record.ID != step.executionID ||
		record.StepID != step.stepID || record.State != scriptexecutions.ScriptExecutionCleanupProven || record.AssignmentID != "" ||
		record.StartAuthorized || record.BodyPrepared != nil || record.ContainerCreated != nil || record.ActiveReference ||
		record.ReconciliationRequired || record.Outcome == nil || record.Cleanup == nil ||
		record.ControllerCleanup != pendingScriptCleanupAuthority(task) ||
		record.Outcome.Reason != scriptexecutions.ScriptOutcomeAbortBeforeStart || record.Outcome.ExitCode != nil ||
		record.Outcome.OutputTruncated || !record.Outcome.ObservedAt.Equal(terminalAt) ||
		!record.Cleanup.ContainerAbsent || !record.Cleanup.BodyAbsent || !record.Cleanup.ExecutionDirectoryAbsent ||
		record.Cleanup.ContainerID != "" || record.Cleanup.BodyDevice != 0 || record.Cleanup.BodyInode != 0 ||
		record.Cleanup.BodyLeaf != "" || !record.UpdatedAt.Equal(terminalAt) {
		return false
	}
	digest, err := scriptControllerCleanupSHA256(*record.Outcome, *record.Cleanup)
	return err == nil && record.LastCheckpointSHA256 == digest
}

func scriptControllerCleanupSHA256(
	outcome scriptexecutions.ScriptOutcomeEvidence,
	cleanup scriptexecutions.ScriptCleanupEvidence,
) (string, error) {
	payload, err := json.Marshal(struct {
		Schema  int                                    `json:"schema"`
		Outcome scriptexecutions.ScriptOutcomeEvidence `json:"outcome"`
		Cleanup scriptexecutions.ScriptCleanupEvidence `json:"cleanup"`
	}{Schema: 1, Outcome: outcome, Cleanup: cleanup})
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(payload)
	clear(payload)
	return hex.EncodeToString(digest[:]), nil
}

func (repository *TaskRepository) validatePendingScriptAbortReplay(
	ctx context.Context,
	task etcdstore.Versioned[TaskRecord],
) error {
	if task.Record.Type != taskjournal.TaskScript && !blueprintScriptTaskShape(task.Record) {
		return nil
	}
	steps, err := preparedScriptExecutionSteps(task.Record)
	if err != nil || len(steps) == 0 {
		return err
	}
	keys := make([]string, 1, len(steps)+1)
	keys[0] = scriptsourceevidence.ScriptSourceRootKey(task.Record.OperationID)
	for _, step := range steps {
		keys = append(keys, scriptexecutions.ScriptExecutionKey(step.executionID))
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: task.ReadRevision})
	if err != nil {
		return err
	}
	if read == nil || read.ReadRevision != task.ReadRevision || len(read.Values) != len(keys) || read.Values[0] != nil {
		return releases.CorruptReleaseRecord()
	}
	executions, err := decodePendingScriptAbortExecutions(task.Record, steps, read.Values[1:])
	if err != nil {
		return err
	}
	terminalAt, err := pendingScriptAbortTerminalAt(task.Record, steps, executions)
	if err != nil || task.Record.FinishedAt == nil || !terminalAt.Equal(*task.Record.FinishedAt) {
		return releases.CorruptReleaseRecord()
	}
	return nil
}
