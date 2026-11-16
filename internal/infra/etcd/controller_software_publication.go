package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/imagefence"

	"github.com/AlanD20/groundplane/internal/infra/etcd/controllersoftware"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) PrepareControllerSoftwareAuthority(
	ctx context.Context,
	capture controllersoftware.Capture,
) (workqueue.Change, error) {
	_, item, err := capture.Work()
	if err != nil {
		return workqueue.Change{}, err
	}
	input, err := controllersoftware.EncodeCapture(capture)
	if err != nil {
		return workqueue.Change{}, err
	}
	current, err := controllersoftware.EncodeCurrent(controllersoftware.Current{Capture: capture})
	if err != nil {
		return workqueue.Change{}, err
	}
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{controllersoftware.LatestKey, imagefence.Key}},
	)
	if err != nil {
		return workqueue.Change{}, err
	}
	if read == nil || len(read.Values) != 2 || read.ReadRevision <= 0 {
		return workqueue.Change{}, errs.New(
			errs.KindStorageUnavailable,
			"Controller software admission snapshot is unavailable",
		)
	}
	defer keyvalue.ClearValues(read.Values)
	fence, err := softwareImageFence(read.Values[1])
	if err != nil {
		return workqueue.Change{}, err
	}
	var latestRevision int64
	if read.Values[0] != nil {
		latestRevision = read.Values[0].ModRevision
	}
	work := workqueue.Change{}
	work.Conditions = append(work.Conditions, keyvalue.Condition{Key: controllersoftware.InputKey(item.ID)},
		fence,
		keyvalue.Condition{Key: controllersoftware.LatestKey, ModRevision: latestRevision})
	work.Mutations = append(
		work.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: controllersoftware.InputKey(item.ID), Value: input},
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: controllersoftware.LatestKey, Value: current},
	)
	return work, nil
}
