package etcd

import (
	"cmp"
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpinrecord"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/pkg/errs"
	"slices"
)

type configurationPinStore interface {
	GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error)
	Range(context.Context, keyvalue.RangeRequest) (*keyvalue.RangeResult, error)
	Transact(context.Context, []keyvalue.Condition, []keyvalue.Mutation) (keyvalue.TransactionResult, error)
}

// prepareConfigurationWorkSecretPins retains exact changed candidate and prior
// sources. Every write is fenced by native work ownership. Failed preparation
// remains resumable; stopped-owner cleanup owns abandoning unpublished pins.
func prepareConfigurationWorkSecretPins(ctx context.Context, store configurationPinStore,
	work workqueue.View, operationID string, capture runtimeconfiguration.WorkCapture,
) (tasksecretpins.Prepared, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return tasksecretpins.Prepared{}, err
	}
	if store == nil || (work.Item.Item.Operation != workexecution.Release && work.Item.Item.Operation != workexecution.Component) ||
		ids.Validate(ids.KindOperation, operationID) != nil ||
		work.Task.Revision <= 0 || work.Item.Revision <= 0 ||
		work.Item.Item.AttemptNumber != 0 || work.Item.Item.ForwardRevoked ||
		work.Task.Task.Stop != nil || work.Task.Task.FinishedAt != nil || work.Item.Item.Phase == workexecution.Settled {
		return tasksecretpins.Prepared{}, errs.New(errs.KindValidationFailed, "Secret retention requires unstarted native configuration work")
	}
	return prepareCapturedConfigurationPins(ctx, store, work, operationID, capture, []tasksecretpins.Condition{
		{Key: workqueue.TaskKey(work.Task.Task.ID), ModRevision: work.Task.Revision},
		{Key: workqueue.ItemKey(work.Item.Item.ID), ModRevision: work.Item.Revision},
	})
}

// The caller supplies its concrete publication owner: an accepted work item or
// a still-sealed direct-mutation claim. Both capture the same immutable sources.
func prepareCapturedConfigurationPins(ctx context.Context, store configurationPinStore,
	work workqueue.View, operationID string, capture runtimeconfiguration.WorkCapture, guards []tasksecretpins.Condition,
) (tasksecretpins.Prepared, error) {
	if ids.Validate(ids.KindOperation, operationID) != nil || len(guards) == 0 {
		return tasksecretpins.Prepared{}, errs.New(errs.KindValidationFailed, "configuration Secret capture has no publication owner")
	}
	if err := capture.Matches(work.Task.Task, work.Item.Item); err != nil {
		return tasksecretpins.Prepared{}, err
	}
	if len(capture.Destinations) == 0 {
		return tasksecretpins.Prepared{}, nil
	}
	sources, err := runtimeconfiguration.New(store)
	if err != nil {
		return tasksecretpins.Prepared{}, err
	}
	// The candidate may have been staged after the planning revision. LoadRetained
	// resolves only the immutable reference, never a mutable configuration head.
	current, err := sources.LoadRetained(ctx, capture.Current)
	if err != nil {
		return tasksecretpins.Prepared{}, err
	}
	selected := make(map[string]tasksecretpinrecord.Record)
	remaining := make(map[string]bool, len(capture.Destinations))
	for _, destination := range capture.Destinations {
		remaining[destination] = true
	}
	for _, file := range current.Files {
		if !remaining[file.Destination] {
			continue
		}
		if err := collectRecoverySecretPins(operationID, []taskmaterialization.Record{file}, selected); err != nil {
			return tasksecretpins.Prepared{}, err
		}
		delete(remaining, file.Destination)
	}
	if len(remaining) != 0 {
		return tasksecretpins.Prepared{}, errs.New(errs.KindStateConflict, "captured configuration is missing changed file sources")
	}
	if capture.Prior != nil {
		prior, err := sources.LoadRetained(ctx, *capture.Prior)
		if err != nil {
			return tasksecretpins.Prepared{}, err
		}
		for _, file := range prior.Files {
			if _, changed := slices.BinarySearch(capture.Destinations, file.Destination); !changed {
				continue
			}
			if err := collectRecoverySecretPins(operationID, []taskmaterialization.Record{file}, selected); err != nil {
				return tasksecretpins.Prepared{}, err
			}
		}
	}
	if len(selected) == 0 {
		return tasksecretpins.Prepared{}, nil
	}
	pins := make([]tasksecretpinrecord.Record, 0, len(selected))
	for _, pin := range selected {
		pins = append(pins, pin)
	}
	slices.SortFunc(pins, func(left, right tasksecretpinrecord.Record) int { return cmp.Compare(left.SecretID, right.SecretID) })
	repository, err := tasksecretpins.NewEtcdRepository(store, work.Task.Task.Scope.ProjectID)
	if err != nil {
		return tasksecretpins.Prepared{}, err
	}
	return repository.PrepareOwned(ctx, operationID, work.Task.Task.ID, work.Item.Item.ID, pins, guards)
}
