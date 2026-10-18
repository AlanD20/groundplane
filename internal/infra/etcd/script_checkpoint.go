package etcd

import (
	"bytes"
	"context"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasework"
	scriptexecutions "github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type scriptCheckpointAnchor struct {
	record     scriptexecutions.ScriptExecutionRecord
	revision   int64
	read       int64
	conditions []etcdstore.Condition
	checkpoint workqueue.Change
	replayed   bool
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
		return etcdstore.Versioned[scriptexecutions.ScriptExecutionRecord]{}, errs.New(
			errs.KindInternal,
			"Script execution record is corrupt",
		)
	}
	return etcdstore.Versioned[scriptexecutions.ScriptExecutionRecord]{
		Record: record, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, nil
}

func (repository *ScriptRepository) CheckpointScriptExecution(
	ctx context.Context,
	authorization agentregistration.WorkAuthorization,
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
	guards, err := authorization.Conditions()
	if err != nil {
		return etcdstore.Versioned[scriptexecutions.ScriptExecutionRecord]{}, err
	}
	if authorization.Identity() != input.Executor {
		return etcdstore.Versioned[scriptexecutions.ScriptExecutionRecord]{}, errs.New(errs.KindStateConflict, "Script checkpoint executor does not match the authenticated Agent")
	}
	for attempt := 0; attempt < 2; attempt++ {
		anchor, err := repository.loadScriptCheckpointAnchor(ctx, input)
		if err != nil {
			return etcdstore.Versioned[scriptexecutions.ScriptExecutionRecord]{}, err
		}
		if anchor.replayed {
			return etcdstore.Versioned[scriptexecutions.ScriptExecutionRecord]{
				Record: anchor.record, Revision: anchor.revision, ReadRevision: anchor.read,
			}, nil
		}
		next, err := scriptexecutions.AdvanceScriptExecutionRecord(anchor.record, input)
		if err != nil {
			etcdstore.ClearMutationValues(anchor.checkpoint.Mutations)
			return etcdstore.Versioned[scriptexecutions.ScriptExecutionRecord]{}, err
		}
		value, err := recordcodec.Encode("script-execution", next)
		if err != nil {
			etcdstore.ClearMutationValues(anchor.checkpoint.Mutations)
			return etcdstore.Versioned[scriptexecutions.ScriptExecutionRecord]{}, err
		}
		conditions := append(anchor.conditions, anchor.checkpoint.Conditions...)
		conditions = append(conditions, guards...)
		mutations := append(anchor.checkpoint.Mutations, etcdstore.Mutation{
			Type: etcdstore.MutationPut, Key: scriptexecutions.ScriptExecutionKey(input.ExecutionID), Value: value,
		})
		transaction, err := repository.store.Transact(ctx, conditions, mutations)
		etcdstore.ClearMutationValues(anchor.checkpoint.Mutations)
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
	ctx context.Context, input scriptexecutions.ScriptCheckpointInput,
) (scriptCheckpointAnchor, error) {
	keys := []string{
		scriptexecutions.ScriptExecutionKey(input.ExecutionID),
		workqueue.TaskKey(input.TaskID), workqueue.ItemKey(input.WorkItemID),
		workqueue.AttemptKey(input.AttemptID), workqueue.ExecutorKey(input.Executor),
		scriptsourceevidence.ScriptSourceRootKey(input.OperationID),
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys})
	if err != nil {
		return scriptCheckpointAnchor{}, err
	}
	if read != nil {
		defer etcdstore.ClearValues(read.Values)
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != len(keys) {
		return scriptCheckpointAnchor{}, errs.New(errs.KindStateConflict, "Script checkpoint ownership read is incomplete")
	}
	conditions := make([]etcdstore.Condition, len(keys))
	for index, value := range read.Values {
		if value == nil || value.Key != keys[index] || value.ModRevision <= 0 || value.ModRevision > read.ReadRevision {
			return scriptCheckpointAnchor{}, errs.New(errs.KindStateConflict, "Script work, attempt or retained sources are unavailable")
		}
		conditions[index] = etcdstore.Condition{Key: keys[index], ModRevision: value.ModRevision}
	}
	execution, err := recordcodec.Decode[scriptexecutions.ScriptExecutionRecord](read.Values[0].Value, "script-execution")
	if err != nil || scriptexecutions.ValidateScriptExecutionRecord(execution) != nil {
		return scriptCheckpointAnchor{}, errs.New(errs.KindInternal, "Script execution state is corrupt")
	}
	task, err := workqueue.DecodeTask(read.Values[1].Value)
	if err != nil {
		return scriptCheckpointAnchor{}, err
	}
	item, err := workqueue.DecodeItem(read.Values[2].Value)
	if err != nil {
		return scriptCheckpointAnchor{}, err
	}
	attempt, err := workqueue.DecodeAttempt(read.Values[3].Value)
	if err != nil {
		return scriptCheckpointAnchor{}, err
	}
	executor, err := workqueue.DecodeExecutor(read.Values[4].Value)
	if err != nil {
		return scriptCheckpointAnchor{}, err
	}
	if execution.ID != input.ExecutionID || execution.CurrentTaskID != input.TaskID ||
		execution.WorkItemID != input.WorkItemID || execution.OperationID != input.OperationID ||
		execution.Input != input.Input || execution.WorkInput == nil ||
		execution.AttemptID != "" && (execution.AttemptID != input.AttemptID || execution.Executor == nil || *execution.Executor != input.Executor) ||
		task.ID != input.TaskID || task.Scope.EnvironmentID != execution.EnvironmentID ||
		!task.PublicationComplete || task.FinishedAt != nil || task.CollectionStartedAt != nil ||
		item.ID != input.WorkItemID || item.TaskID != task.ID || item.Phase != workexecution.Running ||
		item.ActiveAttemptID != input.AttemptID || item.Placement.Executor != workexecution.Agent ||
		item.Placement.HostID != input.Executor.HostID || item.Input == nil || *item.Input != *execution.WorkInput ||
		attempt.ID != input.AttemptID || attempt.TaskID != task.ID || attempt.WorkItemID != item.ID ||
		attempt.Number != item.AttemptNumber || attempt.Input != *item.Input ||
		attempt.Executor != input.Executor || attempt.StartedAt == nil || attempt.FinishedAt != nil ||
		executor.Identity != input.Executor || input.At.Before(*attempt.StartedAt) {
		return scriptCheckpointAnchor{}, errs.New(errs.KindStateConflict, "Script checkpoint does not own the active work attempt")
	}
	switch item.Operation {
	case workexecution.Script:
		if task.Action != workexecution.ActionScript || item.TargetID != execution.ScriptID || *item.Input != execution.Input {
			return scriptCheckpointAnchor{}, errs.New(errs.KindStateConflict, "Script work input changed")
		}
	case workexecution.Release:
		if item.TargetID != execution.ServiceID ||
			(task.Action != workexecution.ActionDeploy && task.Action != workexecution.ActionRollback && task.Action != workexecution.ActionApply) {
			return scriptCheckpointAnchor{}, errs.New(errs.KindStateConflict, "Release hook belongs to another Service action")
		}
	default:
		return scriptCheckpointAnchor{}, errs.New(errs.KindStateConflict, "work operation cannot own a Script")
	}
	// Only a start checkpoint grants a new effect. Cancellation/deadline never
	// prevents reporting an already-authorized effect, failure or cleanup.
	if input.Evidence.Kind == scriptexecutions.ScriptCheckpointEvidenceStartAuthorized &&
		(attempt.Direction != workexecution.Forward || task.Stop != nil || item.ForwardRevoked ||
			!executor.Ready || !time.Now().UTC().Before(attempt.Deadline) || !time.Now().UTC().Before(task.Deadline)) {
		return scriptCheckpointAnchor{}, errs.New(errs.KindStateConflict, "Script start is no longer authorized")
	}
	root, err := scriptsourceevidence.DecodeScriptOperationSourceRoot(read.Values[5].Value)
	if err != nil {
		return scriptCheckpointAnchor{}, err
	}
	if execution.Recovery != nil || !execution.ActiveReference || !manualScriptRootMatches(execution, root) ||
		root.Phase != scriptsourceevidence.ScriptOperationSourceActive ||
		root.ReleasePath != scriptsourceevidence.ScriptSourceReleaseAbsent {
		return scriptCheckpointAnchor{}, errs.New(errs.KindStateConflict, "Script retained sources are closed to execution")
	}
	claimKeys := make([]string, len(item.Claims))
	for index, claim := range item.Claims {
		claimKeys[index] = workqueue.ClaimKey(claim)
	}
	claims, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: claimKeys, Revision: read.ReadRevision})
	if err != nil {
		return scriptCheckpointAnchor{}, err
	}
	if claims != nil {
		defer etcdstore.ClearValues(claims.Values)
	}
	if claims == nil || claims.ReadRevision != read.ReadRevision || len(claims.Values) != len(claimKeys) {
		return scriptCheckpointAnchor{}, errs.New(errs.KindStateConflict, "Script resource ownership read is incomplete")
	}
	for index, value := range claims.Values {
		if value == nil || value.Key != claimKeys[index] || value.ModRevision <= 0 {
			return scriptCheckpointAnchor{}, errs.New(errs.KindStateConflict, "Script resource ownership was released")
		}
		owner, err := recordcodec.Decode[workqueue.ClaimOwner](value.Value, "execution_claim")
		if err != nil || owner.TaskID != task.ID || owner.WorkItemID != item.ID {
			return scriptCheckpointAnchor{}, errs.New(errs.KindStateConflict, "another work item owns the Script resource")
		}
		conditions = append(conditions, etcdstore.Condition{Key: value.Key, ModRevision: value.ModRevision})
	}
	anchor := scriptCheckpointAnchor{record: execution, revision: read.Values[0].ModRevision,
		read: read.ReadRevision, conditions: conditions}
	evidenceKey := scriptexecutions.CheckpointEvidenceKey(input.ExecutionID, input.AttemptID, input.Sequence)
	cursorKey := workqueue.CheckpointKey(input.AttemptID)
	checkpoint, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{cursorKey, evidenceKey}, Revision: read.ReadRevision,
	})
	if err != nil {
		return scriptCheckpointAnchor{}, err
	}
	if checkpoint != nil {
		defer etcdstore.ClearValues(checkpoint.Values)
	}
	if checkpoint == nil || checkpoint.ReadRevision != read.ReadRevision || len(checkpoint.Values) != 2 {
		return scriptCheckpointAnchor{}, errs.New(errs.KindStateConflict, "Script checkpoint cursor read is incomplete")
	}
	evidence, _, err := scriptexecutions.EncodeCheckpointEvidence(input)
	if err != nil {
		return scriptCheckpointAnchor{}, err
	}
	defer clear(evidence)
	if existing := checkpoint.Values[1]; existing != nil {
		if existing.Key != evidenceKey || !bytes.Equal(existing.Value, evidence) {
			return scriptCheckpointAnchor{}, errs.New(errs.KindStateConflict, "Script checkpoint sequence already has different evidence")
		}
		anchor.replayed = true
		return anchor, nil
	}
	if item.Operation == workexecution.Release {
		hookConditions, err := releasework.HookCheckpointConditions(ctx, repository.store, attempt, execution.ID, read.ReadRevision)
		if err != nil {
			return scriptCheckpointAnchor{}, err
		}
		anchor.conditions = append(anchor.conditions, hookConditions...)
	}
	var previous workqueue.CheckpointSnapshot
	if value := checkpoint.Values[0]; value != nil {
		if value.Key != cursorKey {
			return scriptCheckpointAnchor{}, errs.New(errs.KindInternal, "Script checkpoint cursor key differs")
		}
		cursor, err := workqueue.DecodeCheckpoint(value.Value)
		if err != nil {
			return scriptCheckpointAnchor{}, err
		}
		previous = workqueue.CheckpointSnapshot{Record: &cursor, Revision: value.ModRevision}
	}
	evidenceID, err := scriptexecutions.CheckpointEvidenceID(task.ID, input.ExecutionID, input.AttemptID, input.Sequence)
	if err != nil {
		return scriptCheckpointAnchor{}, err
	}
	change, err := workqueue.PrepareCheckpoint(
		workqueue.ItemSnapshot{Item: item, Revision: read.Values[2].ModRevision},
		workqueue.AttemptSnapshot{Attempt: attempt, Revision: read.Values[3].ModRevision}, previous,
		workexecution.Checkpoint{AttemptID: input.AttemptID, Sequence: input.Sequence, StageID: input.StageID,
			Evidence:   workexecution.Input{ID: evidenceID, SHA256: input.PayloadSHA256},
			RecordedAt: input.At},
	)
	if err != nil {
		return scriptCheckpointAnchor{}, err
	}
	change.Conditions = append(change.Conditions, etcdstore.Condition{Key: evidenceKey})
	change.Mutations = append(change.Mutations, etcdstore.Mutation{Type: etcdstore.MutationPut,
		Key: evidenceKey, Value: append([]byte(nil), evidence...)})
	anchor.checkpoint = change
	return anchor, nil
}
