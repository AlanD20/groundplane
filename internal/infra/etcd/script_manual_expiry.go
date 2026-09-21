package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	scriptexecutions "github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	scriptsourceevidence "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	scriptsourcepublication "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcepublication"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	sourceref "github.com/AlanD20/groundplane/internal/infra/scriptsourcereference"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// prepareManualScriptExpiry runs before journal pruning. Every release batch
// compares the original terminal Task and retention index; neither is rewritten
// to manufacture a new cleanup Task or extend the retry deadline.
func (repository *TaskRepository) prepareManualScriptExpiry(
	ctx context.Context, task TaskRecord, taskRevision int64, retention etcdstore.KeyValue,
	revision int64, now time.Time,
) (bool, error) {
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		scriptsourceevidence.ScriptSourceRootKey(
			task.OperationID,
		), scriptexecutions.ScriptExecutionKey(task.Params[scriptexecutions.ScriptExecutionIDParam]),
		taskjournal.TaskActiveOperationKey(
			task.OperationID,
		), taskjournal.TaskAssignmentIndexKey(task.ID), manualScriptClosingReportKey(task.ID),
	}, Revision: revision})
	if err != nil {
		return false, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 5 || read.Values[1] == nil {
		return false, taskjournal.CorruptPruneIntent()
	}
	execution, err := recordcodec.Decode[scriptexecutions.ScriptExecutionRecord](
		read.Values[1].Value,
		"script-execution",
	)
	if err != nil || scriptexecutions.ValidateScriptExecutionRecord(execution) != nil || !taskOwnsScriptExecution(task, execution) ||
		execution.OperationID != task.OperationID ||
		execution.PlanHash != task.PlanHash ||
		execution.EnvironmentID != task.Owner.EnvironmentID {
		return false, taskjournal.CorruptPruneIntent()
	}
	if read.Values[0] == nil {
		if execution.ActiveReference || execution.State != scriptexecutions.ScriptExecutionCleanupProven {
			return false, errs.New(errs.KindInternal, "manual Script source root is missing before cleanup")
		}
		return false, nil
	}
	root, err := scriptsourceevidence.DecodeScriptOperationSourceRoot(read.Values[0].Value)
	if err != nil || !manualScriptRootMatches(execution, root) {
		return false, taskjournal.CorruptPruneIntent()
	}
	if execution.CurrentTaskID != task.ID || (root.Phase == scriptsourceevidence.ScriptOperationSourceActive &&
		(root.RetryDisposition == sourceref.RetryDispositionUndecided || root.RetryDisposition == sourceref.RetryDispositionTransferred)) {
		return true, nil
	}
	if root.RetryExpiresAt == nil || task.RetainUntil == nil || !root.RetryExpiresAt.Equal(*task.RetainUntil) ||
		now.Before(*root.RetryExpiresAt) || read.Values[2] != nil || read.Values[3] != nil || read.Values[4] != nil ||
		(task.Status != taskjournal.TaskStatusFailed && task.Status != taskjournal.TaskStatusTimedOut) {
		return false, taskjournal.CorruptPruneIntent()
	}
	guards := []etcdstore.Condition{
		{Key: taskjournal.TaskStorageKey(task.ID), ModRevision: taskRevision},
		{Key: retention.Key, ModRevision: retention.ModRevision},
		{Key: taskjournal.TaskActiveOperationKey(task.OperationID)}, {Key: taskjournal.TaskAssignmentIndexKey(task.ID)},
		{Key: manualScriptClosingReportKey(task.ID)},
		{Key: read.Values[1].Key, ModRevision: read.Values[1].ModRevision},
	}
	authority, err := scriptsourcepublication.NewAuthority(repository.store)
	if err != nil {
		return false, err
	}
	if root.Phase == scriptsourceevidence.ScriptOperationSourceActive &&
		root.RetryDisposition == sourceref.RetryDispositionAvailable {
		next, err := expireManualScriptExecution(execution, now)
		if err != nil {
			return false, err
		}
		fragment, err := authority.PrepareRetryExpiry(ctx, task.OperationID, read.Values[0].ModRevision, now)
		if err != nil {
			return false, err
		}
		defer fragment.Clear()
		encoded, err := recordcodec.Encode("script-execution", next)
		if err != nil {
			return false, err
		}
		defer clear(encoded)
		mutations := append(cloneBlueprintCandidateMutations(fragment.Mutations()),
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: read.Values[1].Key, Value: encoded})
		defer etcdstore.ClearMutationValues(mutations)
		transaction, err := repository.store.Transact(ctx, append(guards, fragment.Conditions()...), mutations)
		if err != nil {
			return false, err
		}
		etcdstore.ClearValues(transaction.FailureReads)
		if !transaction.Succeeded {
			return false, errs.New(errs.KindStateConflict, "manual Script expiry authority changed")
		}
		guards[len(guards)-1].ModRevision = transaction.Revision
	} else if root.Phase != scriptsourceevidence.ScriptOperationSourceReleasing || root.ReleasePath != scriptsourceevidence.ScriptSourceReleaseRetryExpiry ||
		root.RetryDisposition != sourceref.RetryDispositionExpired || !manualScriptExpiryExecutionMatches(execution, *root.RetryExpiresAt) {
		return false, taskjournal.CorruptPruneIntent()
	}
	for {
		processed, drained, err := authority.ReleaseRetryExpiryNext(ctx, task.OperationID, guards)
		if err != nil {
			return false, err
		}
		if processed {
			continue
		}
		if !drained {
			return false, taskjournal.CorruptPruneIntent()
		}
		break
	}
	final, err := authority.PrepareRetryExpiryFinalization(ctx, task.OperationID)
	if err != nil {
		return false, err
	}
	defer final.Clear()
	transaction, err := repository.store.Transact(ctx, append(guards, final.Conditions()...), final.Mutations())
	if err != nil {
		return false, err
	}
	etcdstore.ClearValues(transaction.FailureReads)
	// Successful release also invalidates the caller's pre-release MVCC read.
	// The ordinary prune retry loop must refresh it before admitting a journal.
	return false, errs.New(errs.KindStateConflict, "manual Script source expiry changed pruning authority")
}

func expireManualScriptExecution(
	execution scriptexecutions.ScriptExecutionRecord,
	now time.Time,
) (scriptexecutions.ScriptExecutionRecord, error) {
	if scriptexecutions.ValidateScriptExecutionRecord(execution) != nil || execution.State != scriptexecutions.ScriptExecutionNotStarted ||
		!execution.ActiveReference || execution.StartAuthorized || execution.AssignmentID != "" ||
		!now.After(execution.UpdatedAt) {
		return scriptexecutions.ScriptExecutionRecord{}, errs.New(
			errs.KindStateConflict,
			"manual Script may already have started",
		)
	}
	outcome := scriptexecutions.ScriptOutcomeEvidence{
		Reason:     scriptexecutions.ScriptOutcomeExpiryBeforeStart,
		ObservedAt: now.UTC(),
	}
	cleanup := scriptexecutions.ScriptCleanupEvidence{
		ContainerAbsent:          true,
		BodyAbsent:               true,
		ExecutionDirectoryAbsent: true,
	}
	digest, err := scriptControllerCleanupSHA256(outcome, cleanup)
	if err != nil {
		return scriptexecutions.ScriptExecutionRecord{}, err
	}
	execution.State, execution.ControllerCleanup = scriptexecutions.ScriptExecutionCleanupProven, scriptexecutions.ScriptControllerCleanupManualRetryExpiry
	execution.Outcome, execution.Cleanup, execution.LastCheckpointSHA256 = &outcome, &cleanup, digest
	execution.ActiveReference, execution.UpdatedAt = false, now.UTC()
	if err := scriptexecutions.ValidateScriptExecutionRecord(execution); err != nil {
		return scriptexecutions.ScriptExecutionRecord{}, err
	}
	return execution, nil
}

func manualScriptExpiryExecutionMatches(execution scriptexecutions.ScriptExecutionRecord, deadline time.Time) bool {
	if scriptexecutions.ValidateScriptExecutionRecord(execution) != nil || execution.State != scriptexecutions.ScriptExecutionCleanupProven ||
		execution.ControllerCleanup != scriptexecutions.ScriptControllerCleanupManualRetryExpiry || execution.ActiveReference ||
		execution.Outcome == nil || execution.Outcome.Reason != scriptexecutions.ScriptOutcomeExpiryBeforeStart || execution.Cleanup == nil ||
		!execution.UpdatedAt.Equal(execution.Outcome.ObservedAt) ||
		execution.UpdatedAt.Before(deadline) {
		return false
	}
	digest, err := scriptControllerCleanupSHA256(*execution.Outcome, *execution.Cleanup)
	return err == nil && digest == execution.LastCheckpointSHA256
}
