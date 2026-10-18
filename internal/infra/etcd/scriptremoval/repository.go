package scriptremoval

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
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
		return nil, errs.New(errs.KindInternal, "Script removal storage is required")
	}
	return &Repository{store: backend}, nil
}

func (repository *Repository) Load(ctx context.Context, view workqueue.View) (Input, keyvalue.Condition, error) {
	item, task := view.Item.Item, view.Task.Task
	if item.Operation != workexecution.ScriptRemoval || item.TaskID != task.ID || item.Input == nil ||
		item.Placement.Executor != workexecution.Controller || view.ReadRevision <= 0 {
		return Input{}, keyvalue.Condition{}, errs.New(errs.KindStateConflict, "Script removal work is invalid")
	}
	key := InputKey(item.ID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.ReadRevision})
	if err != nil {
		return Input{}, keyvalue.Condition{}, err
	}
	if read == nil {
		return Input{}, keyvalue.Condition{}, errs.New(errs.KindInternal, "Script removal input read is missing")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision != view.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil {
		return Input{}, keyvalue.Condition{}, errs.New(errs.KindStateConflict, "Script removal input is unavailable")
	}
	input, err := Decode(read.Values[0].Value)
	if err != nil {
		return Input{}, keyvalue.Condition{}, err
	}
	ref, err := input.Reference()
	if err != nil {
		return Input{}, keyvalue.Condition{}, err
	}
	if input.TaskID != task.ID || input.WorkItemID != item.ID || input.ScriptID != item.TargetID || ref != *item.Input ||
		task.Action != workexecution.ActionRemove || task.TargetID != input.ScriptID ||
		task.Scope != (workexecution.Scope{TenantID: input.TenantID, ProjectID: input.ProjectID, EnvironmentID: input.EnvironmentID}) {
		return Input{}, keyvalue.Condition{}, errs.New(errs.KindStateConflict, "Script removal differs from its captured resource")
	}
	return input, keyvalue.Condition{Key: key, ModRevision: read.Values[0].ModRevision}, nil
}

func (repository *Repository) PrepareDecision(ctx context.Context, view workqueue.View, outcome workexecution.Outcome, at time.Time) (workqueue.Change, error) {
	input, capture, err := repository.Load(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	storage, err := scriptrecord.ReadActiveScriptStorage(ctx, repository.store, input.ScriptID, view.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	stored, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{
			deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetScript), input.ScriptID),
		}, Revision: view.ReadRevision,
	})
	if err != nil {
		return workqueue.Change{}, err
	}
	if stored != nil {
		defer keyvalue.ClearValues(stored.Values)
	}
	if stored == nil || stored.ReadRevision != view.ReadRevision || len(stored.Values) != 1 || stored.Values[0] == nil {
		return workqueue.Change{}, errs.New(errs.KindInternal, "Script deletion state is inconsistent")
	}
	record := storage.Script.Record
	if storage.Script.Revision != input.ScriptRevision || record.EnvironmentID != input.EnvironmentID ||
		record.ServiceID != input.ServiceID || record.ScriptSetGeneration != input.ScriptSetGeneration || record.ActiveReferences != 0 {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Script changed or has active executions after removal was accepted")
	}
	tombstone, err := deletionrecord.DecodeDeletionTombstone(stored.Values[0].Value)
	if err != nil || tombstone.TargetKind != deletionrecord.DeletionTargetScript || tombstone.TargetID != input.ScriptID ||
		tombstone.TargetRevision != storage.Script.Revision || tombstone.TaskID != input.TaskID ||
		tombstone.Phase != deletionrecord.DeletionPhaseFinalizing {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Script deletion tombstone does not match its Task")
	}
	indexes, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{
			scriptrecord.ScriptSetOwnerKey(record.EnvironmentID, record.ScriptSetGeneration, record.Desired.ID),
			scriptrecord.ScriptSetSlugKey(record.EnvironmentID, record.ScriptSetGeneration, record.Desired.Slug),
		},
		Revision: view.ReadRevision,
	})
	if err != nil {
		return workqueue.Change{}, err
	}
	if indexes != nil {
		defer keyvalue.ClearValues(indexes.Values)
	}
	if indexes == nil || indexes.ReadRevision != view.ReadRevision || len(indexes.Values) != 2 || indexes.Values[0] == nil || indexes.Values[1] == nil ||
		string(indexes.Values[0].Value) != record.Desired.ID || string(indexes.Values[1].Value) != record.Desired.ID {
		return workqueue.Change{}, errs.New(errs.KindInternal, "Script deletion indexes are corrupt")
	}
	resource := workqueue.Change{
		Conditions: []keyvalue.Condition{
			{
				Key: scriptrecord.ScriptSetScriptKey(
					record.EnvironmentID,
					record.ScriptSetGeneration,
					input.ScriptID,
				),
				ModRevision: storage.Script.Revision,
			},
			{
				Key:         deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetScript), input.ScriptID),
				ModRevision: stored.Values[0].ModRevision,
			},
			{
				Key: scriptrecord.ScriptSetOwnerKey(
					record.EnvironmentID,
					record.ScriptSetGeneration,
					input.ScriptID,
				),
				ModRevision: indexes.Values[0].ModRevision,
			},
			{
				Key: scriptrecord.ScriptSetSlugKey(
					record.EnvironmentID,
					record.ScriptSetGeneration,
					record.Desired.Slug,
				),
				ModRevision: indexes.Values[1].ModRevision,
			},
			{Key: scriptrecord.ScriptSetActiveKey(record.EnvironmentID), ModRevision: storage.Active.Revision},
			{Key: scriptrecord.ScriptLocatorKey(input.ScriptID), ModRevision: storage.Locator.Revision},
			{
				Key:         scriptrecord.ScriptEnvironmentLocatorKey(record.EnvironmentID, input.ScriptID),
				ModRevision: storage.EnvironmentLocator.Revision,
			},
		},
		Mutations: []keyvalue.Mutation{{
			Type: keyvalue.MutationDelete, Key: deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetScript), input.ScriptID),
		}},
	}
	if outcome == workexecution.Succeeded {
		fence, err := environmentfence.LoadOrdinary(ctx, repository.store, input.EnvironmentID, view.ReadRevision)
		if err != nil {
			return workqueue.Change{}, err
		}
		epoch, err := fence.EpochRewriteMutation()
		if err != nil {
			return workqueue.Change{}, err
		}
		resource.Conditions = append(resource.Conditions, fence.TransactionConditions()...)
		resource.Mutations = append(resource.Mutations, epoch)
		resource.Mutations = append(
			resource.Mutations,
			keyvalue.Mutation{
				Type: keyvalue.MutationDelete,
				Key:  scriptrecord.ScriptSetOwnerKey(record.EnvironmentID, record.ScriptSetGeneration, input.ScriptID),
			},
			keyvalue.Mutation{
				Type: keyvalue.MutationDelete,
				Key: scriptrecord.ScriptSetSlugKey(
					record.EnvironmentID,
					record.ScriptSetGeneration,
					record.Desired.Slug,
				),
			},
			keyvalue.Mutation{
				Type: keyvalue.MutationDelete,
				Key: scriptrecord.ScriptSetBodyGenerationPrefix(
					record.EnvironmentID,
					record.ScriptSetGeneration,
					input.ScriptID,
				),
				Prefix: true,
			},
			keyvalue.Mutation{
				Type: keyvalue.MutationDelete,
				Key:  scriptrecord.ScriptSetScriptKey(record.EnvironmentID, record.ScriptSetGeneration, input.ScriptID),
			},
			keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: scriptrecord.ScriptLocatorKey(input.ScriptID)},
			keyvalue.Mutation{
				Type: keyvalue.MutationDelete,
				Key:  scriptrecord.ScriptEnvironmentLocatorKey(record.EnvironmentID, input.ScriptID),
			},
		)
	}
	activeValue, err := scriptrecord.EncodeScriptSetGeneration(storage.Active.Record)
	if err != nil {
		keyvalue.ClearMutationValues(resource.Mutations)
		return workqueue.Change{}, err
	}
	resource.Mutations = append(
		resource.Mutations,
		keyvalue.Mutation{
			Type:  keyvalue.MutationPut,
			Key:   scriptrecord.ScriptSetActiveKey(record.EnvironmentID),
			Value: activeValue,
		},
	)
	change, err := workqueue.PrepareUnstartedSettlement(view.Task, view.Item, workexecution.Completion{
		Outcome: outcome, Evidence: *view.Item.Item.Input, EffectsSettled: true}, at)
	if err != nil {
		keyvalue.ClearMutationValues(resource.Mutations)
		return workqueue.Change{}, err
	}
	resource.Conditions = append(resource.Conditions, capture)
	change.Conditions = append(change.Conditions, resource.Conditions...)
	change.Mutations = append(change.Mutations, resource.Mutations...)
	return change, nil
}
