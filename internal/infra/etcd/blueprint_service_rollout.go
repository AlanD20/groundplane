package etcd

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// A staged rollout is inert until native work publication compares this exact
// record and captures its source references and Release marker together.
// The immutable desired revision owns abandoned captures and their cleanup.
func BlueprintServiceRolloutKey(environmentID, parentID, taskID string) string {
	return blueprints.EnvironmentBlueprintRevisionPrefixFinal(environmentID, parentID) + "service-rollouts/" + taskID
}

func (ledger *ReleaseLedger) StageBlueprintServiceRollout(ctx context.Context,
	capture releaserender.BlueprintServiceRolloutCapture,
) (keyvalue.Versioned[releaserender.BlueprintServiceRolloutCapture], error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return keyvalue.Versioned[releaserender.BlueprintServiceRolloutCapture]{}, err
	}
	if ledger == nil || ledger.store == nil {
		return keyvalue.Versioned[releaserender.BlueprintServiceRolloutCapture]{}, errs.New(
			errs.KindInternal,
			"Blueprint rollout storage is not configured",
		)
	}
	value, err := releaserender.EncodeBlueprintServiceRolloutCapture(capture)
	if err != nil {
		return keyvalue.Versioned[releaserender.BlueprintServiceRolloutCapture]{}, err
	}
	defer clear(value)
	guards, err := ledger.rolloutPreparationGuards(ctx, capture)
	if err != nil {
		return keyvalue.Versioned[releaserender.BlueprintServiceRolloutCapture]{}, err
	}
	key := BlueprintServiceRolloutKey(
		capture.Scope.EnvironmentID,
		capture.TaskID,
		capture.WorkItemID,
	)
	conditions := append([]keyvalue.Condition{{Key: key}}, guards...)
	result, err := ledger.store.Transact(ctx, conditions,
		[]keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: value}})
	if err != nil {
		return keyvalue.Versioned[releaserender.BlueprintServiceRolloutCapture]{}, err
	}
	defer keyvalue.ClearValues(result.FailureReads)
	revision := result.Revision
	if !result.Succeeded {
		if len(result.FailureReads) != len(conditions) || result.FailureReads[0] == nil ||
			result.FailureReads[0].Key != key || !bytes.Equal(result.FailureReads[0].Value, value) {
			return keyvalue.Versioned[releaserender.BlueprintServiceRolloutCapture]{}, errs.New(
				errs.KindStateConflict,
				"Blueprint Service rollout identity already has different prepared work",
			)
		}
		revision = result.FailureReads[0].ModRevision
	}
	// Decode the owned bytes rather than retaining the producer's mutable maps.
	owned, err := releaserender.DecodeBlueprintServiceRolloutCapture(value)
	if err != nil {
		return keyvalue.Versioned[releaserender.BlueprintServiceRolloutCapture]{}, err
	}
	return keyvalue.Versioned[releaserender.BlueprintServiceRolloutCapture]{
		Record:       owned,
		Revision:     revision,
		ReadRevision: result.Revision,
	}, nil
}

func (ledger *ReleaseLedger) GetBlueprintServiceRollout(ctx context.Context,
	work workqueue.View,
) (keyvalue.Versioned[releaserender.BlueprintServiceRolloutCapture], error) {
	var empty keyvalue.Versioned[releaserender.BlueprintServiceRolloutCapture]
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return empty, err
	}
	task, item := work.Task.Task, work.Item.Item
	if ledger == nil || ledger.store == nil || work.ReadRevision <= 0 ||
		task.Validate() != nil || item.Validate() != nil || task.Action != workexecution.ActionApply ||
		item.TaskID != task.ID || item.Operation != workexecution.Release || task.Scope.EnvironmentID == "" {
		return empty, errs.New(errs.KindValidationFailed, "Blueprint rollout read requires captured Service work")
	}
	key := BlueprintServiceRolloutKey(task.Scope.EnvironmentID, task.ID, item.ID)
	read, err := ledger.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: work.ReadRevision})
	if err != nil {
		return empty, err
	}
	if read == nil || read.ReadRevision != work.ReadRevision || len(read.Values) != 1 {
		return empty, errs.New(errs.KindStateConflict, "Blueprint rollout read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	if value == nil || value.Key != key || value.ModRevision <= 0 || value.ModRevision > work.ReadRevision {
		return empty, errs.New(errs.KindStateConflict, "Blueprint Service has no captured rollout at this revision")
	}
	capture, err := releaserender.DecodeBlueprintServiceRolloutCapture(value.Value)
	if err != nil {
		return empty, err
	}
	if err := validateBlueprintServiceRolloutWork(capture, task, item); err != nil {
		return empty, err
	}
	return keyvalue.Versioned[releaserender.BlueprintServiceRolloutCapture]{
		Record: capture, Revision: value.ModRevision, ReadRevision: read.ReadRevision}, nil
}

func validateBlueprintServiceRolloutWork(capture releaserender.BlueprintServiceRolloutCapture,
	task workexecution.Task, item workexecution.Item,
) error {
	render, err := releaserender.OpenBlueprintServiceRolloutCapture(capture)
	if err != nil {
		return err
	}
	if task.Validate() != nil || item.Validate() != nil || task.Action != workexecution.ActionApply ||
		item.TaskID != task.ID || item.Operation != workexecution.Release ||
		capture.TaskID != task.ID || capture.WorkItemID != item.ID || capture.Scope != task.Scope ||
		capture.Placement != item.Placement || render.ServiceID != item.TargetID ||
		!capture.CreatedAt.Equal(item.CreatedAt) || !capture.Deadline.Equal(item.Deadline) ||
		capture.Deadline.After(task.Deadline) {
		return errs.New(errs.KindStateConflict, "Blueprint rollout belongs to another Service work item")
	}
	return nil
}

// BlueprintServiceRolloutPublicationCondition binds final operation input to the
// exact stored preparation. File recovery is captured separately by the Release
// owner; it cannot change the frozen strategy, workload, hooks or predecessor.
func BlueprintServiceRolloutPublicationCondition(work workqueue.View,
	stored keyvalue.Versioned[releaserender.BlueprintServiceRolloutCapture],
	captured workexecution.Input,
) (keyvalue.Condition, error) {
	if stored.Revision <= 0 || stored.ReadRevision < stored.Revision ||
		stored.Revision > work.ReadRevision || captured.Validate() != nil {
		return keyvalue.Condition{}, errs.New(errs.KindStateConflict, "Blueprint rollout publication has no captured source revision")
	}
	if err := validateBlueprintServiceRolloutWork(stored.Record, work.Task.Task, work.Item.Item); err != nil {
		return keyvalue.Condition{}, err
	}
	expected, err := releaserender.BlueprintRolloutInput(stored.Record)
	if err != nil {
		return keyvalue.Condition{}, err
	}
	if expected != captured {
		return keyvalue.Condition{}, errs.New(errs.KindStateConflict, "Blueprint rollout changed after capture")
	}
	return keyvalue.Condition{Key: BlueprintServiceRolloutKey(stored.Record.Scope.EnvironmentID,
		stored.Record.TaskID, stored.Record.WorkItemID), ModRevision: stored.Revision}, nil
}
