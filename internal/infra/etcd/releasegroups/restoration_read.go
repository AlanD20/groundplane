package releasegroups

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type RestorationSnapshot struct {
	Input      RestorationInput
	Revision   int64
	Conditions []keyvalue.Condition
}

// LoadRestorationInput reads retained operation identity without requiring the
// source's bytes during final collection. An actual effect grant additionally
// calls LoadRestorationSource with this input's explicitly saved FailedWorkID.
func (repository *ExecutionRepository) LoadRestorationInput(
	ctx context.Context,
	view workqueue.View,
) (RestorationSnapshot, error) {
	item, task := view.Item.Item, view.Task.Task
	if item.Operation != workexecution.ReleaseRestoration || item.Input == nil || view.Item.Revision <= 0 ||
		view.Task.Revision <= 0 || view.ReadRevision < view.Item.Revision || view.ReadRevision < view.Task.Revision {
		return RestorationSnapshot{}, errs.New(
			errs.KindStateConflict,
			"Group restoration requires its retained native input",
		)
	}
	root, err := repository.Load(ctx, view.Task, view.ReadRevision)
	if err != nil {
		return RestorationSnapshot{}, err
	}
	index := slices.IndexFunc(
		root.Record.Policy.Members,
		func(member domain.GroupWorkMember) bool { return member.RestoreID == item.ID },
	)
	if index < 0 {
		return RestorationSnapshot{}, errs.New(
			errs.KindStateConflict,
			"Group has no captured restoration for this work",
		)
	}
	key := RestorationInputKey(item.ID)
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.ReadRevision},
	)
	if err != nil {
		return RestorationSnapshot{}, err
	}
	if read == nil || read.ReadRevision != view.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil {
		return RestorationSnapshot{}, errs.New(errs.KindStateConflict, "Group restoration input is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	if value.Key != key || value.ModRevision <= 0 || value.ModRevision > view.ReadRevision {
		return RestorationSnapshot{}, errs.New(errs.KindStateConflict, "Group restoration input revision is invalid")
	}
	input, err := DecodeRestorationInput(value.Value, *item.Input)
	if err != nil {
		return RestorationSnapshot{}, err
	}
	preparation, err := root.Record.RestorationPreparation(index)
	if err != nil {
		return RestorationSnapshot{}, err
	}
	expected := root.Record.memberWork(item.ID)
	if input.Preparation != preparation || input.Authority.GroupInput != task.Intent ||
		input.Source.Executor.HostID != root.Record.Placement.HostID || item.TaskID != task.ID || item.TargetID != preparation.ServiceID ||
		item.Placement != root.Record.Placement || !item.CreatedAt.Equal(expected.CreatedAt) || item.AttemptNumber == 0 && !item.Deadline.Equal(expected.Deadline) ||
		!slices.Equal(item.Claims, expected.Claims) || !slices.Equal(item.Dependencies, expected.Dependencies) {
		return RestorationSnapshot{}, errs.New(
			errs.KindStateConflict,
			"Group restoration input differs from its accepted source or host",
		)
	}
	return RestorationSnapshot{Input: input, Revision: value.ModRevision, Conditions: []keyvalue.Condition{
		{Key: key, ModRevision: value.ModRevision}, {Key: ExecutionCaptureKey(task.ID), ModRevision: root.Revision},
	}}, nil
}
