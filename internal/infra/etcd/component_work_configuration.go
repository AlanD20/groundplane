package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *EnvironmentBlueprintRepository) PrepareComponentWorkConfiguration(ctx context.Context,
	work workqueue.View, configurationID string, generation uint64, files []taskmaterialization.Record,
) (runtimeconfiguration.WorkCapture, error) {
	if repository == nil || repository.HierarchyRepository == nil || work.Item.Item.Operation != workexecution.Component {
		return runtimeconfiguration.WorkCapture{}, errs.New(errs.KindValidationFailed, "Component configuration requires native Component work")
	}
	sources, err := runtimeconfiguration.New(repository.store)
	if err != nil {
		return runtimeconfiguration.WorkCapture{}, err
	}
	return sources.CaptureWork(ctx, work.Task.Task, work.Item.Item, work.ReadRevision, configurationID, generation, files)
}

func (repository *EnvironmentBlueprintRepository) LoadComponentWorkConfiguration(ctx context.Context,
	reference runtimeconfiguration.Reference,
) (runtimeconfiguration.Snapshot, error) {
	if repository == nil || repository.HierarchyRepository == nil {
		return runtimeconfiguration.Snapshot{}, errs.New(errs.KindInternal, "Component configuration repository is missing")
	}
	sources, err := runtimeconfiguration.New(repository.store)
	if err != nil {
		return runtimeconfiguration.Snapshot{}, err
	}
	return sources.LoadRetained(ctx, reference)
}

func (repository *EnvironmentBlueprintRepository) PrepareComponentWorkSecretPins(ctx context.Context,
	work workqueue.View, operationID string, capture runtimeconfiguration.WorkCapture,
) (tasksecretpins.Prepared, error) {
	if repository == nil || repository.HierarchyRepository == nil || work.Item.Item.Operation != workexecution.Component {
		return tasksecretpins.Prepared{}, errs.New(errs.KindValidationFailed, "Secret retention requires native Component work")
	}
	return prepareConfigurationWorkSecretPins(ctx, repository.store, work, operationID, capture)
}

func (repository *EnvironmentBlueprintRepository) ComponentWorkConfigurationConditions(ctx context.Context,
	work workqueue.View, capture runtimeconfiguration.WorkCapture,
) ([]keyvalue.Condition, error) {
	if repository == nil || repository.HierarchyRepository == nil || work.Item.Item.Operation != workexecution.Component {
		return nil, errs.New(errs.KindValidationFailed, "Component configuration admission requires native work")
	}
	if err := capture.Matches(work.Task.Task, work.Item.Item); err != nil {
		return nil, err
	}
	sources, err := runtimeconfiguration.New(repository.store)
	if err != nil {
		return nil, err
	}
	conditions, err := sources.WorkConditions(ctx, capture, work.ReadRevision)
	if err != nil {
		return nil, err
	}
	result := make([]keyvalue.Condition, len(conditions))
	for index, condition := range conditions {
		result[index] = keyvalue.Condition{Key: condition.Key, ModRevision: condition.ModRevision}
	}
	return result, nil
}
