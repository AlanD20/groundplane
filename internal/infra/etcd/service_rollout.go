package etcd

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// A staged rollout is inert until native work publication compares this exact
// record and captures its source references and Release marker together.
// Native work owns abandoned captures and their cleanup.

func (ledger *ReleaseLedger) StageServiceRollout(ctx context.Context,
	capture releaserender.ServiceRolloutCapture,
) (keyvalue.Versioned[releaserender.ServiceRolloutCapture], error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return keyvalue.Versioned[releaserender.ServiceRolloutCapture]{}, err
	}
	if ledger == nil || ledger.store == nil {
		return keyvalue.Versioned[releaserender.ServiceRolloutCapture]{}, errs.New(
			errs.KindInternal,
			"Service rollout storage is not configured",
		)
	}
	value, err := releaserender.EncodeServiceRolloutCapture(capture)
	if err != nil {
		return keyvalue.Versioned[releaserender.ServiceRolloutCapture]{}, err
	}
	defer clear(value)
	guards, err := ledger.rolloutPreparationGuards(ctx, capture)
	if err != nil {
		return keyvalue.Versioned[releaserender.ServiceRolloutCapture]{}, err
	}
	key := releaserender.ServiceRolloutKey(
		capture.Scope.EnvironmentID,
		capture.TaskID,
		capture.WorkItemID,
	)
	conditions := append([]keyvalue.Condition{{Key: key}}, guards...)
	result, err := ledger.store.Transact(ctx, conditions,
		[]keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: value}})
	if err != nil {
		return keyvalue.Versioned[releaserender.ServiceRolloutCapture]{}, err
	}
	defer keyvalue.ClearValues(result.FailureReads)
	revision := result.Revision
	if !result.Succeeded {
		if len(result.FailureReads) != len(conditions) || result.FailureReads[0] == nil ||
			result.FailureReads[0].Key != key || !bytes.Equal(result.FailureReads[0].Value, value) {
			return keyvalue.Versioned[releaserender.ServiceRolloutCapture]{}, errs.New(
				errs.KindStateConflict,
				"Service rollout identity already has different prepared work",
			)
		}
		revision = result.FailureReads[0].ModRevision
	}
	// Decode the owned bytes rather than retaining the producer's mutable maps.
	owned, err := releaserender.DecodeServiceRolloutCapture(value)
	if err != nil {
		return keyvalue.Versioned[releaserender.ServiceRolloutCapture]{}, err
	}
	return keyvalue.Versioned[releaserender.ServiceRolloutCapture]{
		Record:       owned,
		Revision:     revision,
		ReadRevision: result.Revision,
	}, nil
}

func (ledger *ReleaseLedger) GetServiceRollout(ctx context.Context,
	work workqueue.View,
) (keyvalue.Versioned[releaserender.ServiceRolloutCapture], error) {
	var empty keyvalue.Versioned[releaserender.ServiceRolloutCapture]
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return empty, err
	}
	task, item := work.Task.Task, work.Item.Item
	if ledger == nil || ledger.store == nil || work.ReadRevision <= 0 ||
		task.Validate() != nil || item.Validate() != nil || !releaserender.IsReleaseAction(task.Action) ||
		item.TaskID != task.ID || item.Operation != workexecution.Release || task.Scope.EnvironmentID == "" {
		return empty, errs.New(errs.KindValidationFailed, "Service rollout read requires captured Service work")
	}
	key := releaserender.ServiceRolloutKey(task.Scope.EnvironmentID, task.ID, item.ID)
	read, err := ledger.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: work.ReadRevision})
	if err != nil {
		return empty, err
	}
	if read == nil || read.ReadRevision != work.ReadRevision || len(read.Values) != 1 {
		return empty, errs.New(errs.KindStateConflict, "Service rollout read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	if value == nil || value.Key != key || value.ModRevision <= 0 || value.ModRevision > work.ReadRevision {
		return empty, errs.New(errs.KindStateConflict, "Service has no captured rollout at this revision")
	}
	capture, err := releaserender.DecodeServiceRolloutCapture(value.Value)
	if err != nil {
		return empty, err
	}
	if err := capture.MatchesWork(task, item); err != nil {
		return empty, err
	}
	return keyvalue.Versioned[releaserender.ServiceRolloutCapture]{
		Record: capture, Revision: value.ModRevision, ReadRevision: read.ReadRevision}, nil
}

// ServiceRolloutPublicationCondition binds final operation input to the
// exact stored preparation. File recovery is captured separately by the Release
// owner; it cannot change the frozen strategy, workload, hooks or predecessor.
func ServiceRolloutPublicationCondition(work workqueue.View,
	stored keyvalue.Versioned[releaserender.ServiceRolloutCapture],
	captured workexecution.Input,
) (keyvalue.Condition, error) {
	if stored.Revision <= 0 || stored.ReadRevision < stored.Revision ||
		stored.Revision > work.ReadRevision || captured.Validate() != nil {
		return keyvalue.Condition{}, errs.New(errs.KindStateConflict, "Service rollout publication has no captured source revision")
	}
	if err := stored.Record.MatchesWork(work.Task.Task, work.Item.Item); err != nil {
		return keyvalue.Condition{}, err
	}
	expected, err := releaserender.ServiceRolloutInput(stored.Record)
	if err != nil {
		return keyvalue.Condition{}, err
	}
	if expected != captured {
		return keyvalue.Condition{}, errs.New(errs.KindStateConflict, "Service rollout changed after capture")
	}
	return keyvalue.Condition{Key: releaserender.ServiceRolloutKey(stored.Record.Scope.EnvironmentID,
		stored.Record.TaskID, stored.Record.WorkItemID), ModRevision: stored.Revision}, nil
}
