package servicelifecyclework

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type readStore interface {
	GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error)
	Range(context.Context, keyvalue.RangeRequest) (*keyvalue.RangeResult, error)
	Transact(context.Context, []keyvalue.Condition, []keyvalue.Mutation) (keyvalue.TransactionResult, error)
}

type Repository struct{ store readStore }

func NewRepository(store readStore) (*Repository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "Service lifecycle storage is required")
	}
	return &Repository{store: store}, nil
}

// PreparePublication must join the native Task, resource intent, runtime-source
// comparisons and encrypted hook ownership in one admission transaction.
func PreparePublication(input Input) (workqueue.Change, error) {
	value, err := Encode(input)
	if err != nil {
		return workqueue.Change{}, err
	}
	key := InputKey(input.WorkItemID)
	return workqueue.Change{Conditions: []keyvalue.Condition{{Key: key}},
		Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: value}}}, nil
}

func (repository *Repository) Load(ctx context.Context, view workqueue.View) (Input, keyvalue.Condition, error) {
	task, item := view.Task.Task, view.Item.Item
	if view.ReadRevision <= 0 || item.TaskID != task.ID || item.Operation != workexecution.ServiceLifecycle || item.Input == nil {
		return Input{}, keyvalue.Condition{}, errs.New(errs.KindStateConflict, "Service lifecycle work has no captured input")
	}
	key := InputKey(item.ID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.ReadRevision})
	if err != nil {
		return Input{}, keyvalue.Condition{}, err
	}
	if read == nil {
		return Input{}, keyvalue.Condition{}, errs.New(errs.KindInternal, "Service lifecycle input read is missing")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision != view.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil ||
		read.Values[0].Key != key || read.Values[0].ModRevision <= 0 || read.Values[0].ModRevision > read.ReadRevision {
		return Input{}, keyvalue.Condition{}, errs.New(errs.KindStateConflict, "Service lifecycle input is unavailable")
	}
	input, err := Decode(read.Values[0].Value)
	if err != nil {
		return Input{}, keyvalue.Condition{}, err
	}
	ref, err := input.Reference()
	if err != nil {
		return Input{}, keyvalue.Condition{}, err
	}
	placement := workexecution.Placement{Executor: workexecution.Controller}
	if input.Runtime != nil {
		placement = workexecution.Placement{Executor: workexecution.Agent, HostID: input.HostID}
	}
	if input.TaskID != task.ID || input.WorkItemID != item.ID || input.ServiceID != item.TargetID || input.ServiceID != task.TargetID ||
		input.Action != task.Action || input.Scope != task.Scope || item.Placement != placement || ref != *item.Input ||
		task.Intent != ref || task.ExpectedItems != 1 {
		return Input{}, keyvalue.Condition{}, errs.New(errs.KindStateConflict, "Service lifecycle input differs from its accepted action")
	}
	return input, keyvalue.Condition{Key: key, ModRevision: read.Values[0].ModRevision}, nil
}
