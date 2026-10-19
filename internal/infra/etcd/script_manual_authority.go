package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	sourceref "github.com/AlanD20/groundplane/internal/infra/scriptsourcereference"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Source membership is captured with the Script input. Delivery and retries may
// change attempts, but cannot substitute a newer source set.
func manualScriptRootMatches(
	execution scriptexecutions.ScriptExecutionRecord,
	root sourceref.OperationSourceRoot,
) bool {
	return execution.SourceMembershipCount > 0 && execution.OperationID == root.OperationID &&
		execution.SourceMembershipCount == root.MembershipCount &&
		execution.SourceMembershipSHA256 == root.MembershipSHA256
}

// LoadRetainedScriptWork adds the source ownership comparison to native work
// input. Assignment/start must commit these conditions with their own attempt,
// host, claims and stop/deadline conditions; this read alone grants no effects.
func (repository *ScriptRepository) LoadRetainedScriptWork(
	ctx context.Context,
	view workqueue.View,
) (ScriptWorkInput, error) {
	input, err := repository.LoadScriptWork(ctx, view)
	if err != nil {
		return ScriptWorkInput{}, err
	}
	return repository.requireScriptWorkSources(ctx, view, input)
}

func (repository *ScriptRepository) requireScriptWorkSources(
	ctx context.Context,
	view workqueue.View,
	input ScriptWorkInput,
) (ScriptWorkInput, error) {
	fail := func(err error) (ScriptWorkInput, error) {
		input.Execution.Clear()
		return ScriptWorkInput{}, err
	}
	execution := input.Execution
	if execution.Recovery != nil || !execution.ActiveReference || view.Task.Task.FinishedAt != nil ||
		!view.Task.Task.PublicationComplete {
		return fail(errs.New(errs.KindStateConflict, "Script work no longer holds executable inputs"))
	}
	key := scriptsourceevidence.ScriptSourceRootKey(execution.OperationID)
	value, err := scriptexecutions.ScriptExecutionValueAt(ctx, repository.store, key, view.ReadRevision)
	if err != nil {
		return fail(err)
	}
	defer clear(value.Value)
	root, err := scriptsourceevidence.DecodeScriptOperationSourceRoot(value.Value)
	if err != nil || !manualScriptRootMatches(execution, root) {
		return fail(errs.New(errs.KindInternal, "Script source membership differs from captured work"))
	}
	if root.Phase != scriptsourceevidence.ScriptOperationSourceActive ||
		root.ReleasePath != scriptsourceevidence.ScriptSourceReleaseAbsent ||
		root.RetryDisposition != sourceref.RetryDispositionUndecided &&
			root.RetryDisposition != sourceref.RetryDispositionTransferred {
		return fail(errs.New(errs.KindStateConflict, "Script source retention is closed to execution"))
	}
	input.Conditions = append(input.Conditions, keyvalue.Condition{Key: key, ModRevision: value.ModRevision})
	return input, nil
}
