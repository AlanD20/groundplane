package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RetireReleaseHookSources finishes one bounded pass. Its returned conditions
// prove retirement for subsequent hook deletion; no missing root is accepted
// without the matching durable receipt.
func (repository *ScriptRepository) RetireReleaseHookSources(ctx context.Context, view workqueue.View,
	executionID string, reference workexecution.Input, at time.Time,
) (bool, workqueue.Change, error) {
	loaded, err := repository.loadCapturedReleaseScriptWork(ctx, view, executionID, reference)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	defer loaded.Execution.Clear()
	return repository.retireScriptWorkSources(ctx, view, loaded, at)
}

// RetireStandaloneScriptSources uses the same bounded reference protocol as
// hooks, but resolves ownership through the standalone work binding.
func (repository *ScriptRepository) RetireStandaloneScriptSources(ctx context.Context, view workqueue.View, at time.Time) (bool, workqueue.Change, error) {
	loaded, err := repository.LoadScriptWork(ctx, view)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	defer loaded.Execution.Clear()
	return repository.retireScriptWorkSources(ctx, view, loaded, at)
}

func (repository *ScriptRepository) retireScriptWorkSources(ctx context.Context, view workqueue.View, loaded ScriptWorkInput, at time.Time) (bool, workqueue.Change, error) {
	task := view.Task.Task
	if task.Validate() != nil || task.CollectionStartedAt == nil || task.FinishedAt == nil ||
		task.Progress.Unsettled() != 0 || at.Before(task.UpdatedAt) {
		return false, workqueue.Change{}, errs.New(errs.KindStateConflict, "Script source retirement requires the collecting Task")
	}
	execution := loaded.Execution
	if execution.Recovery == nil && execution.State != scriptexecutions.ScriptExecutionNotStarted && execution.State != scriptexecutions.ScriptExecutionCleanupProven {
		return false, workqueue.Change{}, errs.New(errs.KindStateConflict, "Script execution still requires its retained inputs")
	}
	key := scriptexecutions.SourceRetirementKey(task.ID, execution.OperationID)
	rootKey := scriptsourceevidence.ScriptSourceRootKey(execution.OperationID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key, rootKey}, Revision: view.ReadRevision})
	if err != nil {
		return false, workqueue.Change{}, err
	}
	if read == nil || read.ReadRevision != view.ReadRevision || len(read.Values) != 2 {
		return false, workqueue.Change{}, errs.New(errs.KindStateConflict, "Script retirement receipt read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	if value := read.Values[0]; value != nil {
		receipt, err := recordcodec.Decode[scriptexecutions.SourceRetirement](value.Value, "script-source-retirement")
		if err != nil || receipt.Validate() != nil || value.Key != key || value.ModRevision <= 0 ||
			receipt.TaskID != task.ID || receipt.OperationID != execution.OperationID || read.Values[1] != nil ||
			receipt.MembershipCount != execution.SourceMembershipCount || receipt.MembershipSHA256 != execution.SourceMembershipSHA256 {
			return false, workqueue.Change{}, errs.New(errs.KindStateConflict, "Script retirement receipt differs from the captured operation")
		}
		conditions := append(loaded.Conditions, keyvalue.Condition{Key: key, ModRevision: value.ModRevision}, keyvalue.Condition{Key: rootKey})
		return true, workqueue.Change{Conditions: conditions}, nil
	}
	drained, fragment, err := repository.drainScriptWorkSources(ctx, view, loaded)
	defer fragment.Clear()
	if err != nil || !drained {
		return false, workqueue.Change{}, err
	}
	receipt := scriptexecutions.SourceRetirement{TaskID: task.ID, OperationID: execution.OperationID,
		MembershipCount: execution.SourceMembershipCount, MembershipSHA256: execution.SourceMembershipSHA256, RetiredAt: at.UTC()}
	if err := receipt.Validate(); err != nil {
		return false, workqueue.Change{}, err
	}
	raw, err := recordcodec.Encode("script-source-retirement", receipt)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	defer clear(raw)
	conditions := append(loaded.Conditions, fragment.Conditions()...)
	conditions = append(conditions, keyvalue.Condition{Key: key})
	mutations := append([]keyvalue.Mutation(nil), fragment.Mutations()...)
	mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: raw})
	result, err := repository.store.Transact(ctx, conditions, mutations)
	keyvalue.ClearValues(result.FailureReads)
	// Reload on the next bounded pass, including after a concurrent winner.
	return false, workqueue.Change{}, err
}
