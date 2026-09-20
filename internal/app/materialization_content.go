package app

import (
	"context"
	"fmt"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"

	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

func initializeTaskMaterializationResolver(
	store etcdstore.Store,
	blueprints *etcd.HierarchyRepository,
	values *etcd.EntryValueGenerationRepository,
	secrets *etcd.SecretRepository,
	plans *controller.TaskPlanResolver,
	protector *secretvalue.Protector,
) (*controller.TaskMaterializationResolver, error) {
	contents, err := etcd.NewMaterializationContentRepository(store)
	if err != nil {
		return nil, fmt.Errorf("controller: initialize Component materialization content repository: %w", err)
	}
	if err := plans.EnableComponentMaterializationContent(contents); err != nil {
		return nil, fmt.Errorf("controller: initialize Component materialization plan content: %w", err)
	}
	resolver, err := controller.NewTaskMaterializationResolver(blueprints, values, secrets, plans, protector)
	if err != nil {
		return nil, fmt.Errorf("controller: initialize materialization value resolver: %w", err)
	}
	if err := resolver.EnableComponentMaterializationContent(contents); err != nil {
		return nil, fmt.Errorf("controller: initialize Component materialization runtime content: %w", err)
	}
	snapshots, err := etcd.NewRuntimeConfigurationRepository(store)
	if err != nil {
		return nil, err
	}
	if err := plans.EnableConfigurationRecovery(snapshots); err != nil {
		return nil, err
	}
	if err := resolver.EnableConfigurationRecovery(snapshots); err != nil {
		return nil, err
	}
	return resolver, nil
}
