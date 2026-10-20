package servicelifecyclework

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
)

// Publication must commit with its Task, item and Service intent. Abandon is
// allowed only when that publication is known absent, never after an unknown
// transaction outcome.
type Publication struct {
	Input  Input
	Change workqueue.Change
	pins   *tasksecretpins.Repository
	store  readStore
}

func (publication *Publication) Abandon(ctx context.Context) error {
	if publication == nil || publication.pins == nil {
		return nil
	}
	return publication.pins.Abandon(ctx, publication.Input.HookInputs.OperationID)
}

func (repository *Repository) PrepareInput(
	ctx context.Context,
	input Input,
	envelope *taskconfiguration.BackingHookEncryptedInputs,
) (_ Publication, resultErr error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return Publication{}, err
	}
	if ids.Validate(ids.KindTask, input.TaskID) != nil || ids.Validate(ids.KindWorkItem, input.WorkItemID) != nil ||
		ids.Validate(ids.KindProject, input.Scope.ProjectID) != nil || (input.HookInputs == nil) != (envelope == nil) {
		return Publication{}, conflict("Service lifecycle publication requires its captured owner and hook envelope")
	}
	publication := Publication{Input: input, store: repository.store}
	defer func() {
		if resultErr != nil {
			keyvalue.ClearMutationValues(publication.Change.Mutations)
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancel()
			resultErr = errors.Join(resultErr, publication.Abandon(cleanup))
		}
	}()
	if input.HookInputs != nil {
		hook := *input.HookInputs
		hook.SecretSources = slices.Clone(hook.SecretSources)
		publication.Input.HookInputs = &hook
		if hook.SecretPins != nil || taskconfiguration.ValidateBackingHookEncryptedInputs(*envelope) != nil ||
			envelope.OperationID != hook.OperationID || envelope.CiphertextSHA256 != hook.CiphertextSHA256 ||
			!slices.Equal(envelope.SecretSources, hook.SecretSources) {
			return Publication{}, conflict("Service lifecycle hook envelope differs from its captured input")
		}
		if len(hook.SecretSources) != 0 {
			pins, err := tasksecretpins.NewEtcdRepository(repository.store, input.Scope.ProjectID)
			if err != nil {
				return Publication{}, err
			}
			publication.pins = pins
			prepared, err := pins.PrepareOwned(
				ctx,
				hook.OperationID,
				input.TaskID,
				input.WorkItemID,
				hook.SecretSources,
				[]tasksecretpins.Condition{{Key: workqueue.TaskKey(input.TaskID)},
					{Key: workqueue.ItemKey(input.WorkItemID)}, {Key: InputKey(input.WorkItemID)}},
			)
			if err != nil {
				return Publication{}, err
			}
			binding, err := prepared.Binding()
			if err != nil {
				return Publication{}, err
			}
			hook.SecretPins = &binding
			activation, err := tasksecretpins.Activation(prepared)
			if err != nil {
				return Publication{}, err
			}
			conditions, mutations, err := tasksecretpins.EtcdFragment(activation)
			if err != nil {
				activation.Clear()
				return Publication{}, err
			}
			publication.Change.Conditions = append(publication.Change.Conditions, conditions...)
			publication.Change.Mutations = append(publication.Change.Mutations, mutations...)
		}
		value, err := taskconfiguration.EncodeBackingHookEncryptedInputs(*envelope)
		if err != nil {
			return Publication{}, err
		}
		key := taskconfiguration.BackingHookTaskInputKey(hook.OperationID)
		publication.Change.Conditions = append(publication.Change.Conditions, keyvalue.Condition{Key: key})
		publication.Change.Mutations = append(
			publication.Change.Mutations,
			keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: value},
		)
	}
	change, err := PreparePublication(publication.Input)
	if err != nil {
		return Publication{}, err
	}
	publication.Change.Mutations = append(publication.Change.Mutations, change.Mutations...)
	publication.Change.Conditions = append(publication.Change.Conditions, change.Conditions...)
	return publication, nil
}
