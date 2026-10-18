package taskplanning

import (
	"github.com/AlanD20/groundplane/internal/controller/componentrender"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type servicePlanSources interface {
	serviceRemovalPlanReader
}

func NewTaskPlanResolverWithServiceSources(volumeRoot string, blueprints blueprintPlanStateReader,
	services servicePlanSources,
	componentCatalog []componentrender.EnvironmentComponentRegistration,
) (*TaskPlanResolver, error) {
	if services == nil {
		return nil, errs.New(errs.KindInternal, "Service planning sources are required")
	}
	resolver, err := NewTaskPlanResolverWithBlueprints(volumeRoot, blueprints, componentCatalog)
	if err != nil {
		return nil, err
	}
	resolver.services = services
	return resolver, nil
}
