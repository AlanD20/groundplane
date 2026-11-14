package attachremoval

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RetireCapturePins releases the exact captured Secret set after the caller
// proves settled work and expiry. It advances at most one release batch. The
// collecting Task prevents dispatch and Retry throughout the release.
func RetireCapturePins(
	ctx context.Context,
	store workqueue.Store,
	revision int64,
	capture WorkCapture,
	guards []keyvalue.Condition,
) (bool, workqueue.Change, error) {
	if ctx == nil || store == nil || revision <= 0 || capture.Validate() != nil || capture.Pins == nil ||
		len(guards) == 0 {
		return false, workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Attach Secret release requires captured pins and settled-work comparisons",
		)
	}
	return RetireOwnedPins(ctx, store, revision, capture.BackingProjectID, *capture.Pins, guards)
}

// RetireOwnedPins releases an exact admission binding even when executable
// work was never published. The caller proves collection and input custody.
func RetireOwnedPins(ctx context.Context, store workqueue.Store, revision int64, projectID string,
	binding tasksecretpins.Binding, guards []keyvalue.Condition,
) (bool, workqueue.Change, error) {
	if ctx == nil || store == nil || revision <= 0 || binding.Validate() != nil || len(guards) == 0 {
		return false, workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Secret release requires exact settled admission custody",
		)
	}
	pins, err := tasksecretpins.NewEtcdRepository(store, projectID)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	root, found, err := pins.LoadActive(ctx, binding.OperationID, revision)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	if !found {
		return true, workqueue.Change{Conditions: []keyvalue.Condition{
			{Key: tasksecretpins.RootKey(binding.OperationID)},
			{Key: tasksecretpins.PreparationKey(binding.OperationID)},
			{Key: tasksecretpins.ReversePrefix(binding.OperationID), Prefix: true},
		}}, nil
	}
	_, releasing, err := root.RetentionCondition(binding)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	if releasing {
		_, _, err := pins.ReleaseBatch(ctx, binding.OperationID)
		return false, workqueue.Change{}, err
	}
	fragment, err := tasksecretpins.BeginRelease(root)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	defer fragment.Clear()
	conditions, mutations, err := tasksecretpins.EtcdFragment(fragment)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	defer keyvalue.ClearMutationValues(mutations)
	queue, err := workqueue.NewRepository(store)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	result, err := queue.Commit(
		ctx,
		workqueue.Change{
			Conditions: append(append([]keyvalue.Condition(nil), guards...), conditions...),
			Mutations:  mutations,
		},
	)
	keyvalue.ClearValues(result.FailureReads)
	return false, workqueue.Change{}, err
}
