package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	scriptexecutions "github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	scriptsourceevidence "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	sourceref "github.com/AlanD20/groundplane/internal/infra/scriptsourcereference"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// manualScriptRootMatches binds the source-set digest to the same durable
// execution authority that seals the plan bytes and hash. Checkpoints and retry
// may transfer execution ownership but never change this pair.
func manualScriptRootMatches(execution scriptexecutions.ScriptExecutionRecord, root sourceref.OperationSourceRoot) bool {
	return execution.SourceMembershipCount > 0 && execution.OperationID == root.OperationID &&
		execution.SourceMembershipCount == root.MembershipCount &&
		execution.SourceMembershipSHA256 == root.MembershipSHA256
}

func (repository *ScriptRepository) manualScriptExecutionAtRevision(
	ctx context.Context,
	task TaskRecord,
	revision int64,
) (scriptexecutions.ScriptExecutionRecord, *etcdstore.KeyValue, error) {
	if task.Type != taskjournal.TaskScript || len(task.Steps) != 1 || revision <= 0 {
		return scriptexecutions.ScriptExecutionRecord{}, nil, errs.New(errs.KindInternal, "manual Script Task identity is corrupt")
	}
	key := scriptexecutions.ScriptExecutionKey(task.Params[scriptexecutions.ScriptExecutionIDParam])
	value, err := scriptexecutions.ScriptExecutionValueAt(ctx, repository.store, key, revision)
	if err != nil {
		return scriptexecutions.ScriptExecutionRecord{}, nil, err
	}
	execution, err := recordcodec.Decode[scriptexecutions.ScriptExecutionRecord](value.Value, "script-execution")
	if err != nil || scriptexecutions.ValidateScriptExecutionRecord(execution) != nil || !taskOwnsScriptExecution(task, execution) ||
		execution.CurrentTaskID != task.ID || execution.OperationID != task.OperationID ||
		execution.EnvironmentID != task.Owner.EnvironmentID || execution.PlanHash != task.PlanHash ||
		execution.SourceMembershipCount == 0 {
		return scriptexecutions.ScriptExecutionRecord{}, nil, errs.New(errs.KindInternal, "manual Script execution authority is corrupt")
	}
	return execution, value, nil
}

func (repository *ScriptRepository) manualScriptExecutionAuthority(
	ctx context.Context,
	task TaskRecord,
	execution scriptexecutions.ScriptExecutionRecord,
	revision int64,
) ([]etcdstore.Condition, error) {
	if task.Type != taskjournal.TaskScript || !taskOwnsScriptExecution(task, execution) ||
		execution.CurrentTaskID != task.ID || execution.OperationID != task.OperationID ||
		execution.EnvironmentID != task.Owner.EnvironmentID || execution.PlanHash != task.PlanHash || !execution.ActiveReference {
		return nil, errs.New(errs.KindStateConflict, "manual Script execution does not own its Task")
	}
	key := scriptsourceevidence.ScriptSourceRootKey(task.OperationID)
	value, err := scriptexecutions.ScriptExecutionValueAt(ctx, repository.store, key, revision)
	if err != nil {
		return nil, err
	}
	root, err := scriptsourceevidence.DecodeScriptOperationSourceRoot(value.Value)
	if err != nil || !manualScriptRootMatches(execution, root) {
		return nil, errs.New(errs.KindInternal, "manual Script source root does not match its sealed plan")
	}
	if root.Phase != scriptsourceevidence.ScriptOperationSourceActive || root.ReleasePath != scriptsourceevidence.ScriptSourceReleaseAbsent ||
		(root.RetryDisposition != sourceref.RetryDispositionUndecided && root.RetryDisposition != sourceref.RetryDispositionTransferred) {
		return nil, errs.New(errs.KindStateConflict, "manual Script source authority is closed to execution")
	}
	return []etcdstore.Condition{{Key: key, ModRevision: value.ModRevision}}, nil
}

func preparedScriptExecutionSteps(task TaskRecord) ([]releaseHookExecutionStep, error) {
	if task.Type != taskjournal.TaskScript {
		return releaseHookExecutionSteps(task)
	}
	if task.Executor != taskjournal.TaskExecutorAgent || task.Owner.EnvironmentID == "" || len(task.Steps) != 1 ||
		!scriptexecutions.ValidRawScriptExecutionID(task.Params[scriptexecutions.ScriptExecutionIDParam]) {
		return nil, errs.New(errs.KindInternal, "manual Script execution step is corrupt")
	}
	return []releaseHookExecutionStep{{stepID: task.Steps[0].ID, executionID: task.Params[scriptexecutions.ScriptExecutionIDParam]}}, nil
}

func pendingScriptCleanupAuthority(task TaskRecord) scriptexecutions.ScriptControllerCleanupAuthority {
	if task.Type == taskjournal.TaskScript {
		return scriptexecutions.ScriptControllerCleanupManualPendingAbort
	}
	return scriptexecutions.ScriptControllerCleanupBlueprintPendingAbort
}
