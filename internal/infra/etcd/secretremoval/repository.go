package secretremoval

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type store interface {
	Get(context.Context, string) (*keyvalue.GetResult, error)
	GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error)
	Range(context.Context, keyvalue.RangeRequest) (*keyvalue.RangeResult, error)
	Transact(context.Context, []keyvalue.Condition, []keyvalue.Mutation) (keyvalue.TransactionResult, error)
}

type Repository struct{ store store }

func NewRepository(backend store) (*Repository, error) {
	if backend == nil {
		return nil, errs.New(errs.KindInternal, "Secret removal storage is required")
	}
	return &Repository{store: backend}, nil
}

func (repository *Repository) Load(ctx context.Context, view workqueue.View) (Input, keyvalue.Condition, error) {
	item, task := view.Item.Item, view.Task.Task
	if item.Operation != workexecution.SecretRemoval || item.TaskID != task.ID || item.Input == nil ||
		item.Placement.Executor != workexecution.Controller || view.ReadRevision <= 0 {
		return Input{}, keyvalue.Condition{}, errs.New(errs.KindStateConflict, "Secret removal work is invalid")
	}
	key := InputKey(item.ID)
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.ReadRevision},
	)
	if err != nil {
		return Input{}, keyvalue.Condition{}, err
	}
	if read == nil {
		return Input{}, keyvalue.Condition{}, errs.New(errs.KindInternal, "Secret removal input read is missing")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision != view.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil {
		return Input{}, keyvalue.Condition{}, errs.New(errs.KindStateConflict, "Secret removal input is unavailable")
	}
	input, err := Decode(read.Values[0].Value)
	if err != nil {
		return Input{}, keyvalue.Condition{}, err
	}
	ref, err := input.Reference()
	if err != nil {
		return Input{}, keyvalue.Condition{}, err
	}
	if input.TaskID != task.ID || input.WorkItemID != item.ID || input.SecretID != item.TargetID ||
		ref != *item.Input ||
		task.Action != workexecution.ActionRemove ||
		task.TargetID != input.SecretID ||
		task.Scope != (workexecution.Scope{TenantID: input.TenantID, ProjectID: input.ProjectID}) {
		return Input{}, keyvalue.Condition{}, errs.New(
			errs.KindStateConflict,
			"Secret removal differs from its captured resource",
		)
	}
	return input, keyvalue.Condition{Key: key, ModRevision: read.Values[0].ModRevision}, nil
}

// PrepareDecision removes the captured Secret and encrypted value atomically
// with work settlement. Abort only releases this action's tombstone.
func (repository *Repository) PrepareDecision(
	ctx context.Context,
	view workqueue.View,
	outcome workexecution.Outcome,
	at time.Time,
) (workqueue.Change, error) {
	input, capture, err := repository.Load(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	id := input.SecretID
	tombstoneKey := deletions.TombstoneKey(string(deletions.DeletionTargetSecret), id)
	keys := []string{secrets.RecordKey(id), secrets.ValueKey(id), tombstoneKey}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: view.ReadRevision})
	if err != nil {
		return workqueue.Change{}, err
	}
	if read == nil {
		return workqueue.Change{}, errs.New(errs.KindInternal, "Secret removal resource read is missing")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision != view.ReadRevision || len(read.Values) != 3 || read.Values[0] == nil ||
		read.Values[1] == nil ||
		read.Values[2] == nil ||
		read.Values[0].ModRevision != input.SecretRevision ||
		read.Values[1].ModRevision != input.ValueRevision {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Secret or encrypted value changed after removal was accepted",
		)
	}
	record, err := secrets.DecodeRecord(read.Values[0].Value)
	if err != nil {
		return workqueue.Change{}, err
	}
	value, err := secrets.DecodeEncryptedValue(read.Values[1].Value)
	clear(value.Ciphertext)
	if err != nil || value.SecretID != id {
		return workqueue.Change{}, secrets.CorruptRecord()
	}
	tombstone, err := deletions.DecodeDeletionTombstone(read.Values[2].Value)
	if err != nil {
		return workqueue.Change{}, err
	}
	if record.Secret.ID != id || record.Secret.ProjectID != input.ProjectID ||
		tombstone.TargetKind != deletions.DeletionTargetSecret || tombstone.TargetID != id ||
		tombstone.TargetRevision != input.SecretRevision || tombstone.TaskID != input.TaskID || tombstone.Phase != deletions.DeletionPhaseFinalizing {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Secret removal lost its captured ownership")
	}
	change, err := workqueue.PrepareUnstartedSettlement(view.Task, view.Item, workexecution.Completion{
		Outcome: outcome, Evidence: *view.Item.Item.Input, EffectsSettled: true}, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, capture)
	for index, key := range keys {
		change.Conditions = append(
			change.Conditions,
			keyvalue.Condition{Key: key, ModRevision: read.Values[index].ModRevision},
		)
	}
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: tombstoneKey})
	if outcome != workexecution.Succeeded {
		return change, nil
	}
	guards, err := PrepareReferenceAbsence(ctx, repository.store, id)
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, guards...)
	indexes := []string{secrets.SecretOwnerKey(record.Secret), secrets.SecretScopedKey(record.Secret)}
	indexed, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: indexes, Revision: view.ReadRevision})
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, err
	}
	if indexed == nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, errs.New(errs.KindInternal, "Secret removal indexes are missing")
	}
	defer keyvalue.ClearValues(indexed.Values)
	if indexed.ReadRevision != view.ReadRevision || len(indexed.Values) != len(indexes) {
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, errs.New(errs.KindInternal, "Secret removal indexes are incomplete")
	}
	for index, key := range indexes {
		value := indexed.Values[index]
		if value == nil || string(value.Value) != id {
			keyvalue.ClearMutationValues(change.Mutations)
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "Secret removal index changed owner")
		}
		change.Conditions = append(change.Conditions, keyvalue.Condition{Key: key, ModRevision: value.ModRevision})
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: key})
	}
	change.Mutations = append(
		change.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: keys[0]},
		keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: keys[1]},
	)
	return change, nil
}
