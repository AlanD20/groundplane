package etcd

import (
	"bytes"
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	sourceref "github.com/AlanD20/groundplane/internal/infra/scriptsourcereference"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) prepareManualScriptRetryAvailability(
	ctx context.Context, task TaskRecord, execution ScriptExecutionRecord,
	executionValue, rootValue *etcdstore.KeyValue, status taskjournal.TaskStatus, terminalAt *time.Time,
) (scriptTerminalSourceRelease, error) {
	if execution.State != ScriptExecutionNotStarted || execution.StartAuthorized || execution.AssignmentID != "" ||
		!execution.ActiveReference || execution.ReconciliationRequired {
		return scriptTerminalSourceRelease{}, errs.New(errs.KindStateConflict, "manual Script may already have started")
	}
	terminal, err := transitionTaskStatus(task, taskjournal.TaskStatusRunning, status, *terminalAt)
	if err != nil || terminal.RetainUntil == nil || terminal.FinishedAt == nil {
		return scriptTerminalSourceRelease{}, corruptTaskAssignment()
	}
	*terminalAt = *terminal.FinishedAt
	authority, err := newScriptSourceReferenceAuthority(repository.store)
	if err != nil {
		return scriptTerminalSourceRelease{}, err
	}
	fragment, err := authority.PrepareRetryAvailable(
		ctx,
		task.OperationID,
		rootValue.ModRevision,
		*terminal.RetainUntil,
	)
	if err != nil {
		return scriptTerminalSourceRelease{}, err
	}
	defer fragment.Clear()
	return scriptTerminalSourceRelease{
		conditions: append(append([]etcdstore.Condition(nil), fragment.conditions...),
			etcdstore.Condition{Key: executionValue.Key, ModRevision: executionValue.ModRevision},
			etcdstore.Condition{Key: manualScriptClosingReportKey(task.ID)}),
		mutations: cloneBlueprintCandidateMutations(fragment.mutations),
	}, nil
}

func (repository *TaskRepository) prepareManualScriptRetry(
	ctx context.Context, source, retry TaskRecord, revision int64,
) (scriptTaskChange, error) {
	if (source.Status != taskjournal.TaskStatusFailed && source.Status != taskjournal.TaskStatusTimedOut) || source.RetainUntil == nil ||
		!retry.CreatedAt.Before(
			*source.RetainUntil,
		) || retry.Type != taskjournal.TaskScript || retry.OperationID != source.OperationID ||
		retry.PlanID != source.PlanID || retry.PlanHash != source.PlanHash || retry.Target != source.Target ||
		retry.Params[ScriptExecutionIDParam] != source.Params[ScriptExecutionIDParam] ||
		len(source.Steps) != 1 || len(retry.Steps) != 1 || retry.Steps[0].ID != source.Steps[0].ID {
		return scriptTaskChange{}, scriptRetryUnsafe("manual Script retry authority is unavailable")
	}
	execution, executionValue, err := (&ScriptRepository{store: repository.store}).manualScriptExecutionAtRevision(
		ctx,
		source,
		revision,
	)
	if err != nil {
		return scriptTaskChange{}, err
	}
	if execution.State != ScriptExecutionNotStarted || execution.StartAuthorized || execution.AssignmentID != "" ||
		!execution.ActiveReference || !retry.CreatedAt.After(execution.UpdatedAt) {
		return scriptTaskChange{}, scriptRetryUnsafe("manual Script may already have started")
	}
	retentionKey, retentionValue, err := prepareTaskRetentionIndex(source)
	if err != nil {
		return scriptTaskChange{}, err
	}
	defer clear(retentionValue)
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{scriptSourceRootKey(source.OperationID), retentionKey}, Revision: revision,
	})
	if err != nil {
		return scriptTaskChange{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 2 || read.Values[0] == nil ||
		read.Values[1] == nil || !bytes.Equal(read.Values[1].Value, retentionValue) {
		return scriptTaskChange{}, scriptRetryUnsafe("manual Script retry retention authority is unavailable")
	}
	root, err := decodeScriptOperationSourceRoot(read.Values[0].Value)
	if err != nil || !manualScriptRootMatches(execution, root) ||
		root.RetryDisposition != sourceref.RetryDispositionAvailable ||
		root.RetryExpiresAt == nil ||
		!root.RetryExpiresAt.Equal(*source.RetainUntil) {
		return scriptTaskChange{}, scriptRetryUnsafe("manual Script retry sources changed")
	}
	authority, err := newScriptSourceReferenceAuthority(repository.store)
	if err != nil {
		return scriptTaskChange{}, err
	}
	fragment, err := authority.PrepareRetryTransfer(
		ctx,
		source.OperationID,
		read.Values[0].ModRevision,
		retry.CreatedAt,
	)
	if err != nil {
		return scriptTaskChange{}, err
	}
	defer fragment.Clear()
	execution.CurrentTaskID, execution.UpdatedAt = retry.ID, retry.CreatedAt.UTC()
	if err := validateScriptExecutionRecord(execution); err != nil {
		return scriptTaskChange{}, err
	}
	encoded, err := recordcodec.Encode("script-execution", execution)
	if err != nil {
		return scriptTaskChange{}, err
	}
	mutations := append(cloneBlueprintCandidateMutations(fragment.mutations),
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: executionValue.Key, Value: encoded})
	values := make([][]byte, len(mutations))
	for index, mutation := range mutations {
		values[index] = mutation.Value
	}
	return scriptTaskChange{applies: true,
		conditions: append(append([]etcdstore.Condition(nil), fragment.conditions...),
			etcdstore.Condition{Key: executionValue.Key, ModRevision: executionValue.ModRevision},
			etcdstore.Condition{Key: retentionKey, ModRevision: read.Values[1].ModRevision},
			etcdstore.Condition{Key: taskjournal.TaskAssignmentIndexKey(source.ID)}, etcdstore.Condition{Key: manualScriptClosingReportKey(source.ID)}),
		mutations: mutations, values: values,
	}, nil
}
