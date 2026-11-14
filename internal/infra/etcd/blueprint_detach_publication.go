package etcd

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type blueprintDetachPinPreparation struct {
	repository  *tasksecretpins.Repository
	operationID string
}

type preparedBlueprintDetachInputs struct {
	change workqueue.Change
	pins   []blueprintDetachPinPreparation
}

// Frozen sources belong to the existing Blueprint revision descriptor until
// publication. Secret roots activate only in the transaction accepting the
// parent and graph. Staging bytes alone never authorize work or retain Secrets.
func (repository *EnvironmentBlueprintRepository) prepareBlueprintDetachInputs(ctx context.Context,
	parent workexecution.Task, graph blueprintunits.CapturedGraph, inputs []attachremoval.BlueprintInput,
	descriptor keyvalue.Condition, guards []keyvalue.Condition,
) (_ preparedBlueprintDetachInputs, resultErr error) {
	prepared := preparedBlueprintDetachInputs{}
	defer func() {
		if resultErr != nil {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancel()
			resultErr = errors.Join(resultErr, prepared.abandon(cleanupCtx))
			keyvalue.ClearMutationValues(prepared.change.Mutations)
		}
	}()
	if parent.Action != workexecution.ActionApply || parent.PublicationComplete || descriptor.ModRevision <= 0 {
		return preparedBlueprintDetachInputs{}, errs.New(
			errs.KindValidationFailed,
			"Detach inputs require unpublished Apply ownership",
		)
	}
	seen := make(map[string]bool, len(inputs))
	for _, input := range inputs {
		if err := input.Validate(ctx, false); err != nil {
			return preparedBlueprintDetachInputs{}, err
		}
		if input.TaskID != parent.ID || input.EnvironmentID != parent.Scope.EnvironmentID || seen[input.WorkItemID] {
			return preparedBlueprintDetachInputs{}, errs.New(
				errs.KindValidationFailed,
				"Blueprint Detach source belongs to another work item",
			)
		}
		seen[input.WorkItemID] = true
		matched := false
		for _, item := range graph.Items {
			if item.ID != input.WorkItemID {
				continue
			}
			matched = item.TargetID == input.AttachID && item.TaskID == parent.ID &&
				item.Operation == workexecution.Detach &&
				item.Input == nil &&
				item.Phase == workexecution.Waiting &&
				item.Wait != nil &&
				item.Wait.Reason == workexecution.WaitInput &&
				(item.Placement.Executor == workexecution.Agent) == input.HasHostEffects()
		}
		if !matched {
			return preparedBlueprintDetachInputs{}, errs.New(
				errs.KindValidationFailed,
				"Detach work differs from its captured source",
			)
		}
		key := attachremoval.BlueprintInputKey(input.EnvironmentID, input.TaskID, input.WorkItemID)
		stagingGuards := append(slices.Clone(guards), descriptor, keyvalue.Condition{Key: workqueue.TaskKey(parent.ID)},
			keyvalue.Condition{Key: key})
		if len(input.Sources) != 0 {
			pins, err := tasksecretpins.NewEtcdRepository(
				repository.store,
				input.Snapshot.Source.Attach.BackingProjectID,
			)
			if err != nil {
				return preparedBlueprintDetachInputs{}, err
			}
			prepared.pins = append(
				prepared.pins,
				blueprintDetachPinPreparation{repository: pins, operationID: input.OperationID},
			)
			pinGuards := []tasksecretpins.Condition{
				{Key: workqueue.TaskKey(parent.ID)},
				{Key: workqueue.ItemKey(input.WorkItemID)},
				{Key: descriptor.Key, ModRevision: descriptor.ModRevision},
				{Key: key},
			}
			capture, err := pins.PrepareOwned(
				ctx,
				input.OperationID,
				parent.ID,
				input.WorkItemID,
				input.Sources,
				pinGuards,
			)
			if err != nil {
				return preparedBlueprintDetachInputs{}, err
			}
			binding, err := capture.Binding()
			if err != nil {
				return preparedBlueprintDetachInputs{}, err
			}
			input.Pins = &binding
			activation, err := tasksecretpins.Activation(capture)
			if err != nil {
				return preparedBlueprintDetachInputs{}, err
			}
			conditions, mutations, err := tasksecretpins.EtcdFragment(activation)
			activation.Clear()
			if err != nil {
				return preparedBlueprintDetachInputs{}, err
			}
			prepared.change.Conditions = append(prepared.change.Conditions, conditions...)
			prepared.change.Mutations = append(prepared.change.Mutations, mutations...)
		}
		raw, err := attachremoval.EncodeBlueprintInput(ctx, input)
		if err != nil {
			return preparedBlueprintDetachInputs{}, err
		}
		result, err := repository.store.Transact(ctx, append(stagingGuards, input.Conditions...),
			[]keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: raw}})
		clear(raw)
		keyvalue.ClearValues(result.FailureReads)
		if err != nil {
			return preparedBlueprintDetachInputs{}, err
		}
		if !result.Succeeded {
			return preparedBlueprintDetachInputs{}, errs.New(
				errs.KindStateConflict,
				"Blueprint Detach source changed before admission",
			)
		}
		prepared.change.Conditions = append(prepared.change.Conditions, input.Conditions...)
		prepared.change.Conditions = append(
			prepared.change.Conditions,
			keyvalue.Condition{Key: key, ModRevision: result.Revision},
		)
	}
	for _, item := range graph.Items {
		if item.Operation == workexecution.Detach && !seen[item.ID] {
			return preparedBlueprintDetachInputs{}, errs.New(
				errs.KindValidationFailed,
				"Blueprint Detach work has no admission capture",
			)
		}
	}
	return prepared, nil
}

func (prepared *preparedBlueprintDetachInputs) abandon(ctx context.Context) error {
	var result error
	for index := len(prepared.pins) - 1; index >= 0; index-- {
		pin := prepared.pins[index]
		result = errors.Join(result, pin.repository.Abandon(ctx, pin.operationID))
	}
	return result
}

// A transport error does not prove rejection. Resolve parent publication before
// abandoning pending pins; active input custody belongs to the accepted Task.
func (prepared *preparedBlueprintDetachInputs) finish(ctx context.Context, store idempotencyRepositoryStore,
	parentID string, result keyvalue.TransactionResult, publicationErr error,
) error {
	defer keyvalue.ClearMutationValues(prepared.change.Mutations)
	if publicationErr == nil && result.Succeeded || len(prepared.pins) == 0 {
		return publicationErr
	}
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	key := workqueue.TaskKey(parentID)
	read, err := store.GetMany(cleanupCtx, keyvalue.GetManyRequest{Keys: []string{key}})
	if err != nil {
		return errors.Join(publicationErr, err)
	}
	if read == nil || len(read.Values) != 1 {
		return errors.Join(publicationErr, errs.New(errs.KindInternal, "Detach publication outcome is unavailable"))
	}
	defer keyvalue.ClearValues(read.Values)
	if value := read.Values[0]; value != nil {
		task, err := workqueue.DecodeTask(value.Value)
		if err != nil || value.Key != key || value.ModRevision <= 0 || task.ID != parentID ||
			task.Action != workexecution.ActionApply {
			return errors.Join(publicationErr, errs.New(errs.KindInternal, "Detach publication owner is invalid"))
		}
		return publicationErr
	}
	return errors.Join(publicationErr, prepared.abandon(cleanupCtx))
}
