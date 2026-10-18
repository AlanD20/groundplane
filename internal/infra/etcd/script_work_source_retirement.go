package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcepublication"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	sourceref "github.com/AlanD20/groundplane/internal/infra/scriptsourcereference"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// drainScriptWorkSources performs one bounded source-reference release pass.
// Sources are shared by an operation: require the whole Task's collection fence,
// not merely one completed Service. Finalization is returned for the owner to
// commit with its durable retirement receipt, never as an unguarded deletion.
func (repository *ScriptRepository) drainScriptWorkSources(ctx context.Context, view workqueue.View,
	loaded ScriptWorkInput,
) (bool, scriptsourcepublication.ScriptSourceReleaseFragment, error) {
	var empty scriptsourcepublication.ScriptSourceReleaseFragment
	task := view.Task.Task
	if task.Validate() != nil || task.CollectionStartedAt == nil || task.FinishedAt == nil || task.Progress.Unsettled() != 0 {
		return false, empty, errs.New(errs.KindStateConflict, "Script input cleanup requires a fully settled Task closed to Retry")
	}
	execution := loaded.Execution
	if execution.Recovery == nil && execution.State != scriptexecutions.ScriptExecutionNotStarted &&
		execution.State != scriptexecutions.ScriptExecutionCleanupProven {
		return false, empty, errs.New(errs.KindStateConflict, "Script artifacts have not been cleaned up")
	}
	rootKey := scriptsourceevidence.ScriptSourceRootKey(execution.OperationID)
	value, err := scriptexecutions.ScriptExecutionValueAt(ctx, repository.store, rootKey, view.ReadRevision)
	if err != nil {
		return false, empty, err
	}
	defer clear(value.Value)
	root, err := scriptsourceevidence.DecodeScriptOperationSourceRoot(value.Value)
	if err != nil || !manualScriptRootMatches(execution, root) {
		return false, empty, errs.New(errs.KindStateConflict, "Script retained input membership differs from its captured operation")
	}
	authority, err := scriptsourcepublication.NewAuthority(repository.store)
	if err != nil {
		return false, empty, err
	}
	guards := loaded.Conditions
	if root.Phase == scriptsourceevidence.ScriptOperationSourceActive {
		fragment, err := authority.PrepareNormalRelease(ctx, execution.OperationID, sourceref.RetryDispositionForbidden)
		if err != nil {
			return false, empty, err
		}
		defer fragment.Clear()
		conditions := append(guards, fragment.Conditions()...)
		result, err := repository.store.Transact(ctx, conditions, fragment.Mutations())
		keyvalue.ClearValues(result.FailureReads)
		return false, empty, err
	}
	if root.Phase != scriptsourceevidence.ScriptOperationSourceReleasing ||
		root.ReleasePath != scriptsourceevidence.ScriptSourceReleaseNormal || root.RetryDisposition != sourceref.RetryDispositionForbidden {
		return false, empty, errs.New(errs.KindStateConflict, "Script source cleanup is owned by a different retention operation")
	}
	processed, drained, err := authority.ReleaseNext(ctx, execution.OperationID, guards)
	if err != nil || processed || !drained {
		return false, empty, err
	}
	fragment, err := authority.PrepareReleaseFinalization(ctx, execution.OperationID)
	return err == nil, fragment, err
}
