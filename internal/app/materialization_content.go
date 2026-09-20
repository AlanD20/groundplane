package app

import (
	"fmt"
	taskmaterialization "github.com/AlanD20/groundplane/internal/controller/taskmaterialization"
	entryvalues "github.com/AlanD20/groundplane/internal/infra/etcd/entryvalues"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/materializationcontent"

	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	taskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

func initializeTaskMaterializationResolver(
	store etcdstore.Store,
	blueprints *etcd.HierarchyRepository,
	values *entryvalues.Repository,
	secrets *etcd.SecretRepository,
	plans *taskplanning.TaskPlanResolver,
	protector *secretvalue.Protector,
) (*taskmaterialization.TaskMaterializationResolver, error) {
	contents, err := materializationcontent.New(store)
	if err != nil {
		return nil, fmt.Errorf("controller: initialize Component materialization content repository: %w", err)
	}
	if err := plans.EnableComponentMaterializationContent(contents); err != nil {
		return nil, fmt.Errorf("controller: initialize Component materialization plan content: %w", err)
	}
	resolver, err := taskmaterialization.NewTaskMaterializationResolver(blueprints, values, secrets, plans, protector)
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
