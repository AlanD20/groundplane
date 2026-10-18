package etcd

import (
	"context"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/serviceremovalwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpinrecord"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *ServiceRepository) prepareRemovalSecretPins(ctx context.Context, input serviceremovalwork.Input,
	revision int64, guards []keyvalue.Condition,
) (serviceremovalwork.Input, workqueue.Change, error) {
	if input.ConfigurationPins != nil || input.Validate() != nil {
		return input, workqueue.Change{}, errs.New(errs.KindValidationFailed, "Service removal Secret capture requires its unpublished configuration")
	}
	if input.Configuration == nil {
		return input, workqueue.Change{}, nil
	}
	sources, err := runtimeconfiguration.New(repository.store)
	if err != nil {
		return input, workqueue.Change{}, err
	}
	snapshot, err := sources.Load(ctx, *input.Configuration, revision)
	if err != nil {
		return input, workqueue.Change{}, err
	}
	files, err := serviceruntimerecord.ConfigurationFiles(ctx, *input.Runtime, snapshot)
	if err != nil {
		return input, workqueue.Change{}, err
	}
	selected := make(map[string]tasksecretpinrecord.Record)
	operationID := input.ConfigurationOperationID()
	if err := collectRecoverySecretPins(operationID, files, selected); err != nil {
		return input, workqueue.Change{}, err
	}
	if len(selected) == 0 {
		return input, workqueue.Change{}, nil
	}
	pins := make([]tasksecretpinrecord.Record, 0, len(selected))
	for _, pin := range selected {
		pins = append(pins, pin)
	}
	slices.SortFunc(pins, func(a, b tasksecretpinrecord.Record) int { return strings.Compare(a.SecretID, b.SecretID) })
	// Direct removal publishes a new Task; Blueprint omission prepares an item
	// in an existing Task. Both callers must supply their exact ownership guards.
	// Inferring Task absence here would make Apply preparation impossible.
	if err := requireRemovalPreparationOwner(input, guards); err != nil {
		return input, workqueue.Change{}, err
	}
	owner := make([]tasksecretpins.Condition, 0, len(guards))
	for _, guard := range guards {
		owner = append(owner, tasksecretpins.Condition{Key: guard.Key, ModRevision: guard.ModRevision, Prefix: guard.Prefix})
	}
	retention, err := tasksecretpins.NewEtcdRepository(repository.store, input.Scope.ProjectID)
	if err != nil {
		return input, workqueue.Change{}, err
	}
	prepared, err := retention.PrepareOwned(ctx, operationID, input.TaskID, input.WorkItemID, pins, owner)
	if err != nil {
		return input, workqueue.Change{}, err
	}
	binding, err := prepared.Binding()
	if err != nil {
		return input, workqueue.Change{}, err
	}
	activation, err := tasksecretpins.Activation(prepared)
	if err != nil {
		return input, workqueue.Change{}, err
	}
	conditions, mutations, err := tasksecretpins.EtcdFragment(activation)
	if err != nil {
		activation.Clear()
		return input, workqueue.Change{}, err
	}
	input.ConfigurationPins = &binding
	return input, workqueue.Change{Conditions: conditions, Mutations: mutations}, nil
}

func requireRemovalPreparationOwner(input serviceremovalwork.Input, guards []keyvalue.Condition) error {
	for _, key := range []string{workqueue.TaskKey(input.TaskID), workqueue.ItemKey(input.WorkItemID),
		serviceremovalwork.InputKey(input.WorkItemID)} {
		found := false
		for _, guard := range guards {
			if guard.Key != key {
				continue
			}
			if found || guard.Prefix || guard.ModRevision < 0 || guard.Key == serviceremovalwork.InputKey(input.WorkItemID) && guard.ModRevision != 0 {
				return errs.New(errs.KindValidationFailed, "Service removal Secret preparation has conflicting ownership guards")
			}
			found = true
		}
		if !found {
			return errs.New(errs.KindValidationFailed, "Service removal Secret preparation requires Task, work and absent input guards")
		}
	}
	return nil
}
