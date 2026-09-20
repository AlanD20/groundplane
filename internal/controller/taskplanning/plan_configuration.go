package taskplanning

import (
	"context"
	componentrender "github.com/AlanD20/groundplane/internal/controller/componentrender"

	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/controller/configurationrecovery"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func NewTaskPlanResolver(
	volumeRoot string,
	componentCatalog []componentrender.EnvironmentComponentRegistration,
) (*TaskPlanResolver, error) {
	if err := environmentpath.ValidateRoot(volumeRoot); err != nil {
		return nil, err
	}
	if err := componentrender.ValidateEnvironmentComponentCatalog(componentCatalog); err != nil {
		return nil, err
	}
	return &TaskPlanResolver{
		volumeRoot:       volumeRoot,
		componentCatalog: componentrender.CloneEnvironmentComponentCatalog(componentCatalog),
	}, nil
}

func NewTaskPlanResolverWithBlueprints(volumeRoot string, blueprints blueprintPlanStateReader,
	componentCatalog []componentrender.EnvironmentComponentRegistration) (*TaskPlanResolver, error) {
	resolver, err := NewTaskPlanResolver(volumeRoot, componentCatalog)
	if err != nil {
		return nil, err
	}
	if blueprints == nil {
		return nil, errs.New(errs.KindInternal, "Blueprint plan state reader is required")
	}
	resolver.blueprints = blueprints
	return resolver, nil
}

type configurationSnapshotReader interface {
	LoadRetained(context.Context, runtimeconfiguration.Reference) (runtimeconfiguration.Snapshot, error)
}

func (resolver *TaskPlanResolver) EnableConfigurationRecovery(snapshots configurationSnapshotReader) error {
	if resolver == nil {
		return errs.New(errs.KindInternal, "configuration recovery sources are required")
	}
	sources, err := configurationrecovery.NewSources(snapshots)
	if err != nil {
		return err
	}
	resolver.configurationRecovery = sources
	return nil
}
