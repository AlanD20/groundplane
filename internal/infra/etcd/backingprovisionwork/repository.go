package backingprovisionwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Repository struct{ store workqueue.Store }

func NewRepository(store workqueue.Store) (*Repository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "backing creation storage is required")
	}
	return &Repository{store: store}, nil
}

// PreparePublication must join the complete facade's resource/desired-state
// transaction and the native Task/marker publication. This root alone grants
// no assignment, filesystem access or hook execution.
func PreparePublication(input Input) (workqueue.Change, error) {
	value, err := Encode(input)
	if err != nil {
		return workqueue.Change{}, err
	}
	procedure, err := input.Procedure()
	if err != nil {
		clear(value)
		return workqueue.Change{}, err
	}
	key := InputKey(procedure.WorkItemId)
	return workqueue.Change{Conditions: []keyvalue.Condition{{Key: key}},
		Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: value}}}, nil
}

func (repository *Repository) Load(ctx context.Context, view workqueue.View) (Input, keyvalue.Condition, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return Input{}, keyvalue.Condition{}, err
	}
	if view.Task.Revision <= 0 || view.Item.Revision <= 0 || view.ReadRevision < view.Task.Revision || view.ReadRevision < view.Item.Revision {
		return Input{}, keyvalue.Condition{}, invalid("backing creation needs its retained work snapshot")
	}
	key := InputKey(view.Item.Item.ID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.ReadRevision})
	if err != nil {
		return Input{}, keyvalue.Condition{}, err
	}
	if read == nil || read.ReadRevision != view.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil {
		return Input{}, keyvalue.Condition{}, invalid("backing creation captured input is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	if value.Key != key || value.ModRevision <= 0 || value.ModRevision > view.ReadRevision {
		return Input{}, keyvalue.Condition{}, invalid("backing creation captured input revision is invalid")
	}
	input, err := Decode(value.Value)
	if err != nil {
		return Input{}, keyvalue.Condition{}, err
	}
	if err := input.Matches(view.Task.Task, view.Item.Item); err != nil {
		clear(input.Wire)
		return Input{}, keyvalue.Condition{}, err
	}
	return input, keyvalue.Condition{Key: key, ModRevision: value.ModRevision}, nil
}
