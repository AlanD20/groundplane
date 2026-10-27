package etcd

import (
	"context"
	"encoding/json"
	"github.com/AlanD20/groundplane/internal/common/imagefence"

	"github.com/AlanD20/groundplane/internal/infra/etcd/controllersoftware"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) CreateControllerSoftwareWork(
	ctx context.Context,
	capture controllersoftware.Capture,
	marker idempotency.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	task, item, err := capture.Work()
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	input, err := controllersoftware.EncodeCapture(capture)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(input)
	current, err := controllersoftware.EncodeCurrent(controllersoftware.Current{Capture: capture})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(current)
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{controllersoftware.LatestKey, imagefence.Key}},
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if read == nil || len(read.Values) != 2 || read.ReadRevision <= 0 {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindStorageUnavailable,
			"Controller software admission snapshot is unavailable",
		)
	}
	defer keyvalue.ClearValues(read.Values)
	if value := read.Values[1]; value != nil {
		var fence struct {
			Operation string `json:"operation"`
		}
		if json.Unmarshal(value.Value, &fence) != nil {
			return IdempotencyTransactionResult{}, errs.New(errs.KindInternal, "Host image cleanup fence is invalid")
		}
		if fence.Operation != "" {
			return IdempotencyTransactionResult{}, errs.New(
				errs.KindResourceInUse,
				"Host image cleanup is in progress; wait before updating the Controller",
			)
		}
	}
	var latestRevision int64
	if read.Values[0] != nil {
		latestRevision = read.Values[0].ModRevision
	}
	work, err := workqueue.PrepareSingleTask(task, item, marker)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer keyvalue.ClearMutationValues(work.Mutations)
	work.Conditions = append(work.Conditions, keyvalue.Condition{Key: controllersoftware.InputKey(item.ID)},
		keyvalue.Condition{Key: imagefence.Key, ModRevision: keyvalue.RevisionOf(read.Values[1])},
		keyvalue.Condition{Key: controllersoftware.LatestKey, ModRevision: latestRevision})
	work.Mutations = append(
		work.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: controllersoftware.InputKey(item.ID), Value: input},
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: controllersoftware.LatestKey, Value: current},
	)
	classify := func(_ int64, _ []*keyvalue.KeyValue) error {
		return errs.New(errs.KindStateConflict, "Controller software admission changed")
	}
	plan, err := newIdempotencyMutationPlanForMarker(
		idempotency.IdempotencyMarkerTask,
		work.Conditions,
		work.Mutations,
		classify,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return (&IdempotencyRepository{store: repository.store}).Apply(ctx, marker, plan)
}
