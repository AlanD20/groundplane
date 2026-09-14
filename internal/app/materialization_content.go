package app

import (
	"context"
	"fmt"

	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

type environmentBlueprintMaterializationResolver interface {
	PinSecretValue(context.Context, string, string) (etcd.TaskSecretValueReference, error)
	RetainComponentFile(context.Context, etcd.TaskMaterializationRecord, uint64, []byte) error
	ResolveTaskMaterializationSource(
		context.Context,
		string,
		etcd.TaskMaterializationSource,
	) ([]byte, error)
}

func initializeTaskMaterializationResolver(
	store etcd.Store,
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
	return resolver, nil
}
