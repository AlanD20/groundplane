package taskplanning

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/controller/attachplan"
	"github.com/AlanD20/groundplane/internal/controller/componentrender"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type servicePlanSources interface {
	serviceRemovalPlanReader
}

// Only Backing creation still uses the old planner's hook input boundary.
type backingCreationHookInputs interface {
	ResolveLifecycleHookInput(context.Context, etcd.TaskRecord, string, backinghook.Event, attachplan.HookInputConsumer) error
	ResolveDraftLifecycleHookInput(context.Context, etcd.TaskRecord, *taskconfiguration.BackingHookEncryptedInputs, string, backinghook.Event, attachplan.HookInputConsumer) error
}

func NewTaskPlanResolverWithServiceSources(volumeRoot string, blueprints blueprintPlanStateReader,
	services servicePlanSources, hooks backingCreationHookInputs,
	componentCatalog []componentrender.EnvironmentComponentRegistration,
) (*TaskPlanResolver, error) {
	if services == nil || hooks == nil {
		return nil, errs.New(errs.KindInternal, "Service planning sources are required")
	}
	resolver, err := NewTaskPlanResolverWithBlueprints(volumeRoot, blueprints, componentCatalog)
	if err != nil {
		return nil, err
	}
	resolver.services, resolver.backingCreationHooks = services, hooks
	return resolver, nil
}
