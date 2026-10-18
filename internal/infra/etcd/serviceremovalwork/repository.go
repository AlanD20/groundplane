package serviceremovalwork

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Reader interface {
	GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error)
	Range(context.Context, keyvalue.RangeRequest) (*keyvalue.RangeResult, error)
	Transact(context.Context, []keyvalue.Condition, []keyvalue.Mutation) (keyvalue.TransactionResult, error)
	MeasureTransaction(context.Context, []keyvalue.Condition, []keyvalue.Mutation) (keyvalue.TransactionBudget, error)
}

type Repository struct{ store Reader }

func NewRepository(store Reader) (*Repository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "Service removal requires captured input storage")
	}
	return &Repository{store: store}, nil
}

// PreparePublication joins the desired-revision, tombstone, Task and work-item
// admission. A capture alone never permits host execution or definition deletion.
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
	if view.ReadRevision <= 0 || task.Validate() != nil || item.Validate() != nil ||
		item.TaskID != task.ID || item.Operation != workexecution.ServiceRemoval || item.Input == nil {
		return Input{}, keyvalue.Condition{}, conflict("Service removal work has no captured input")
	}
	key := InputKey(item.ID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.ReadRevision})
	if err != nil {
		return Input{}, keyvalue.Condition{}, err
	}
	if read == nil {
		return Input{}, keyvalue.Condition{}, conflict("Service removal input read is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision != view.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil ||
		read.Values[0].Key != key || read.Values[0].ModRevision <= 0 || read.Values[0].ModRevision > view.ReadRevision {
		return Input{}, keyvalue.Condition{}, conflict("Service removal input is unavailable")
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
	if input.HostID != "" {
		placement = workexecution.Placement{Executor: workexecution.Agent, HostID: input.HostID}
	}
	if input.TaskID != task.ID || input.WorkItemID != item.ID || input.Scope != task.Scope ||
		input.ServiceID != item.TargetID || ref != *item.Input || item.Placement != placement ||
		len(item.Claims) != len(removalClaims(input)) {
		return Input{}, keyvalue.Condition{}, conflict("Service removal capture differs from its accepted work")
	}
	for _, claim := range removalClaims(input) {
		if !slices.Contains(item.Claims, claim) {
			return Input{}, keyvalue.Condition{}, conflict("Service removal is missing its captured resource or configuration claim")
		}
	}
	switch task.Action {
	case workexecution.ActionRemove:
		if task.TargetID != input.ServiceID || task.ExpectedItems != 1 || task.Intent != ref {
			return Input{}, keyvalue.Condition{}, conflict("Service removal capture differs from its operator action")
		}
	case workexecution.ActionApply:
		if task.TargetID != input.Scope.EnvironmentID || input.DesiredRevisionID != task.ID {
			return Input{}, keyvalue.Condition{}, conflict("Service omission belongs to another Blueprint Apply")
		}
	default:
		return Input{}, keyvalue.Condition{}, conflict("Task action cannot own Service removal")
	}
	return input, keyvalue.Condition{Key: key, ModRevision: read.Values[0].ModRevision}, nil
}

func conflict(detail string) error { return errs.New(errs.KindStateConflict, detail) }
