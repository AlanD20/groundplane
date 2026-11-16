package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	preparation "github.com/AlanD20/groundplane/internal/infra/etcd/softwarepreparation"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// PrepareSoftwarePreparationAuthority captures artifact publication authority
// after the pure Task's prerequisites have succeeded. Its fragment must join
// promotion of that same accepted Task.
func (repository *TaskRepository) PrepareSoftwarePreparationAuthority(ctx context.Context, capture preparation.Capture,
	record preparation.Record,
) (workqueue.Change, error) {
	task, item, err := capture.Work()
	if err != nil {
		return workqueue.Change{}, err
	}
	if err := capture.Match(item, record); err != nil {
		return workqueue.Change{}, err
	}
	value, err := preparation.Encode(record)
	if err != nil {
		return workqueue.Change{}, err
	}
	input, err := preparation.EncodeCapture(capture)
	if err != nil {
		clear(value)
		return workqueue.Change{}, err
	}
	work := workqueue.Change{}
	work.Conditions = append(
		work.Conditions,
		keyvalue.Condition{Key: preparation.Key(task.ID)},
		keyvalue.Condition{Key: preparation.InputKey(item.ID)},
	)
	work.Mutations = append(work.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: preparation.Key(task.ID), Value: value},
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: preparation.InputKey(item.ID), Value: input})
	return work, nil
}
