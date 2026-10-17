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
func RetireCapturePins(ctx context.Context, store workqueue.Store, revision int64, capture WorkCapture, guards []keyvalue.Condition) (bool, workqueue.Change, error) {
	if ctx == nil || store == nil || revision <= 0 || capture.Validate() != nil || capture.Pins == nil || len(guards) == 0 {
		return false, workqueue.Change{}, errs.New(errs.KindStateConflict, "Attach Secret release requires captured pins and settled-work comparisons")
	}
	pins, err := tasksecretpins.NewEtcdRepository(store, capture.BackingProjectID)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	root, found, err := pins.LoadActive(ctx, capture.OperationID, revision)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	if !found {
		return true, workqueue.Change{Conditions: []keyvalue.Condition{
			{Key: tasksecretpins.RootKey(capture.OperationID)},
			{Key: tasksecretpins.PreparationKey(capture.OperationID)},
			{Key: tasksecretpins.ReversePrefix(capture.OperationID), Prefix: true},
		}}, nil
	}
	_, releasing, err := root.RetentionCondition(*capture.Pins)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	if releasing {
		_, _, err := pins.ReleaseBatch(ctx, capture.OperationID)
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
	result, err := queue.Commit(ctx, workqueue.Change{Conditions: append(append([]keyvalue.Condition(nil), guards...), conditions...), Mutations: mutations})
	keyvalue.ClearValues(result.FailureReads)
	return false, workqueue.Change{}, err
}
