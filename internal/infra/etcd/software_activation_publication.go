package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	activation "github.com/AlanD20/groundplane/internal/infra/etcd/softwareactivation"
	preparation "github.com/AlanD20/groundplane/internal/infra/etcd/softwarepreparation"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) PrepareSoftwareActivationAuthority(
	ctx context.Context,
	record activation.Record,
	prepared keyvalue.Versioned[preparation.Record],
) (workqueue.Change, error) {
	task, _, err := record.Graph()
	if err != nil {
		return workqueue.Change{}, err
	}
	if prepared.Revision <= 0 || !sameSoftwarePreparation(record.Input.Preparation, prepared.Record) {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"software activation must retain its exact verified preparation",
		)
	}
	raw, err := activation.Encode(record)
	if err != nil {
		return workqueue.Change{}, err
	}
	fence, err := repository.readSoftwareImageFence(ctx)
	if err != nil {
		clear(raw)
		return workqueue.Change{}, err
	}
	change := workqueue.Change{}
	change.Conditions = append(
		change.Conditions,
		keyvalue.Condition{Key: activation.Key(task.ID)},
		keyvalue.Condition{Key: preparation.Key(prepared.Record.TaskID), ModRevision: prepared.Revision},
		fence,
	)
	change.Mutations = append(
		change.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: activation.Key(task.ID), Value: raw},
	)
	return change, nil
}

func sameSoftwarePreparation(left, right preparation.Record) bool {
	return left.TaskID == right.TaskID && left.OperationID == right.OperationID &&
		left.InputSHA256 == right.InputSHA256 &&
		left.Source.Equal(right.Source) &&
		left.Progress.Equal(right.Progress) &&
		left.CreatedAt.Equal(right.CreatedAt) &&
		left.UpdatedAt.Equal(right.UpdatedAt)
}
