package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type DetachWorkCapture struct {
	Change                                        workqueue.Change
	store                                         hierarchyStore
	taskID, workID, operationID, backingProjectID string
	input                                         workexecution.Input
}

func (capture DetachWorkCapture) Finish(ctx context.Context, applied bool) error {
	if applied {
		return nil
	}
	keys := []string{workqueue.TaskKey(capture.taskID), workqueue.ItemKey(capture.workID),
		attachremoval.DetachInputKey(capture.workID), attachremoval.WorkCaptureKey(capture.workID)}
	read, err := capture.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys})
	if err != nil {
		return err
	}
	if read == nil || len(read.Values) != 4 {
		return errs.New(errs.KindStateConflict, "Detach publication outcome is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return errs.New(errs.KindStateConflict, "Detach accepted Task disappeared during capture")
	}
	task, err := workqueue.DecodeTask(read.Values[0].Value)
	if err != nil {
		return err
	}
	if read.Values[1] != nil && read.Values[2] != nil && read.Values[3] != nil {
		item, err := workqueue.DecodeItem(read.Values[1].Value)
		if err == nil && item.TaskID == capture.taskID && item.Input != nil && *item.Input == capture.input &&
			task.Intent == capture.input {
			return nil
		}
		return errs.New(errs.KindStateConflict, "Detach publication differs from its captured input")
	}
	if read.Values[1] != nil || read.Values[2] != nil || read.Values[3] != nil || task.AcceptedIntent == nil ||
		task.Intent != (workexecution.Input{}) ||
		task.ExpectedItems != 0 {
		return errs.New(
			errs.KindStateConflict,
			"Detach publication is partially visible; captured inputs remain protected",
		)
	}
	pins, err := tasksecretpins.NewEtcdRepository(capture.store, capture.backingProjectID)
	if err != nil {
		return err
	}
	if err := pins.Abandon(ctx, capture.operationID); err != nil {
		return err
	}
	result, err := capture.store.Transact(
		ctx,
		[]keyvalue.Condition{{Key: keys[1]}, {Key: keys[2]}, {Key: keys[3]}},
		[]keyvalue.Mutation{
			{Type: keyvalue.MutationDelete, Key: attachremoval.DetachInputPrefix(capture.workID), Prefix: true},
		},
	)
	keyvalue.ClearValues(result.FailureReads)
	return err
}
