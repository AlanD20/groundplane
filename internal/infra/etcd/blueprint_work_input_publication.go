package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func BlueprintReleaseWorkInputKey(workItemID string) string {
	return "/v1/execution/release-inputs/" + workItemID
}

type PreparedBlueprintWorkInput struct {
	Item   workexecution.Item
	Input  workexecution.Input
	Change workqueue.Change
}

// PrepareBlueprintWorkInput joins native readiness, exact input storage, file
// source comparisons and Secret activation. The Release owner must append its
// manifest, hook/source publication, predecessor and dependency conditions to
// this fragment and commit once. This method performs no publication transaction.
func (ledger *ReleaseLedger) PrepareBlueprintWorkInput(ctx context.Context,
	work workqueue.View, rollout keyvalue.Versioned[releaserender.BlueprintServiceRolloutCapture],
	input releaserender.BlueprintReleaseWorkInput, secrets tasksecretpins.Prepared, at time.Time,
) (PreparedBlueprintWorkInput, error) {
	var empty PreparedBlueprintWorkInput
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return empty, err
	}
	if ledger == nil || ledger.store == nil || work.Task.Revision <= 0 || work.Item.Revision <= 0 ||
		work.Task.Task.Action != workexecution.ActionApply || work.Item.Item.Operation != workexecution.Release ||
		work.Task.Task.Stop != nil || work.Task.Task.FinishedAt != nil {
		return empty, errs.New(errs.KindValidationFailed, "Blueprint input publication requires its waiting native work")
	}
	if err := input.ValidateRollout(rollout.Record); err != nil {
		return empty, err
	}
	if err := input.Configuration.Matches(work.Task.Task, work.Item.Item); err != nil {
		return empty, err
	}
	if (input.Secrets == nil) != secrets.IsZero() {
		return empty, errs.New(errs.KindValidationFailed, "Blueprint input Secret activation is incomplete")
	}
	if !secrets.IsZero() {
		binding, err := secrets.Binding()
		if err != nil || binding != *input.Secrets {
			return empty, errs.New(errs.KindStateConflict, "Blueprint input Secret activation differs from its captured binding")
		}
	}
	// Immutable sources were staged after the planning snapshot. Refresh only
	// the storage revision, never the selected Task/item or captured intent.
	read, err := ledger.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{
		workqueue.TaskKey(work.Task.Task.ID), workqueue.ItemKey(work.Item.Item.ID),
	}})
	if err != nil {
		return empty, err
	}
	if read == nil || read.ReadRevision < work.ReadRevision || len(read.Values) != 2 {
		return empty, errs.New(errs.KindStateConflict, "Blueprint work publication snapshot is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil || read.Values[1] == nil ||
		read.Values[0].Key != workqueue.TaskKey(work.Task.Task.ID) || read.Values[0].ModRevision != work.Task.Revision ||
		read.Values[1].Key != workqueue.ItemKey(work.Item.Item.ID) || read.Values[1].ModRevision != work.Item.Revision {
		return empty, errs.New(errs.KindStateConflict, "Blueprint work changed during input preparation")
	}
	current := work
	current.ReadRevision = read.ReadRevision
	rolloutCondition, err := BlueprintServiceRolloutPublicationCondition(current, rollout, input.Rollout)
	if err != nil {
		return empty, err
	}
	configurationConditions, err := ledger.ReleaseWorkConfigurationConditions(ctx, current, input.Configuration)
	if err != nil {
		return empty, err
	}
	value, reference, err := releaserender.EncodeBlueprintReleaseWorkInput(input)
	if err != nil {
		return empty, err
	}
	defer clear(value)
	item, err := workexecution.PrepareInput(work.Item.Item, reference, at)
	if err != nil {
		return empty, err
	}
	change, err := workqueue.PrepareInput(work.Task, work.Item, reference, at)
	if err != nil {
		return empty, err
	}
	keep := false
	defer func() {
		if !keep {
			keyvalue.ZeroMutationBytes(change.Mutations)
		}
	}()
	change.Conditions = append(change.Conditions, rolloutCondition,
		keyvalue.Condition{Key: BlueprintReleaseWorkInputKey(item.ID)})
	change.Conditions = append(change.Conditions, configurationConditions...)
	change.Mutations = append(change.Mutations, keyvalue.Mutation{
		Type: keyvalue.MutationPut, Key: BlueprintReleaseWorkInputKey(item.ID), Value: append([]byte(nil), value...),
	})
	if !secrets.IsZero() {
		activation, err := tasksecretpins.Activation(secrets)
		if err != nil {
			return empty, err
		}
		conditions, mutations, err := tasksecretpins.EtcdFragment(activation)
		if err != nil {
			activation.Clear()
			return empty, err
		}
		change.Conditions = append(change.Conditions, conditions...)
		change.Mutations = append(change.Mutations, mutations...)
	}
	keep = true
	return PreparedBlueprintWorkInput{Item: item, Input: reference, Change: change}, nil
}
