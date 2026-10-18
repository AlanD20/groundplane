package taskplanning

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/serviceproxy"
	componentrender "github.com/AlanD20/groundplane/internal/controller/componentrender"
	"github.com/AlanD20/groundplane/internal/controller/configurationrecovery"
	releasedomain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// TaskPlanResolver resolves exact procedures from their capability-owned sealed
// inputs. Rendered procedures use pinned inputs.
type TaskPlanResolver struct {
	volumeRoot            string
	blueprints            blueprintPlanStateReader
	services              servicePlanSources
	componentCatalog      []componentrender.EnvironmentComponentRegistration
	serviceProxyImages    map[serviceproxy.Implementation]*releasedomain.ProxyImage
	releases              *etcd.ReleaseLedger
	componentPlans        ComponentTaskPlanResolver
	materializations      componentMaterializationContentRepository
	configurationRecovery *configurationrecovery.Sources
	scriptPlans           ScriptExecutionPlanReader
}

type blueprintPlanStateReader interface {
	GetTenant(context.Context, string) (etcdstore.Versioned[hierarchyrecord.TenantRecord], error)
	GetProject(context.Context, string) (etcdstore.Versioned[hierarchyrecord.ProjectRecord], error)
	GetEnvironment(context.Context, string) (etcdstore.Versioned[hierarchyrecord.EnvironmentRecord], error)
	GetEnvironmentBlueprintRevision(
		context.Context,
		string,
		string,
	) (etcdstore.Versioned[blueprints.EnvironmentBlueprintRevision], bool, error)
	GetEnvironmentComposeProjection(
		context.Context,
		string,
	) (etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection], bool, error)
	GetEnvironmentComposeProjectionRevision(
		context.Context,
		string,
		string,
	) (etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection], bool, error)
}

func (resolver *TaskPlanResolver) resolveExecutionPlan(
	ctx context.Context,
	task etcd.TaskRecord,
) (*agentpb.ExecutionPlan, error) {
	if task.Type == taskjournal.TaskDeploy || task.Type == taskjournal.TaskRollback {
		return resolver.resolveReleasePlan(ctx, task)
	}
	if ctx == nil {
		return nil, errs.New(errs.KindInternal, "execution plan resolution context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if resolver == nil || resolver.volumeRoot == "" {
		return nil, errs.New(errs.KindInternal, "execution plan resolver is not configured")
	}
	if task.Type == taskjournal.TaskScript {
		if resolver.scriptPlans == nil {
			return nil, errs.New(errs.KindInternal, "Script Task plan resolver is not configured")
		}
		return resolver.scriptPlans.GetScriptExecutionPlan(ctx, task)
	}
	if task.Params[taskjournal.TaskReleaseNetworkParentParam] != "" {
		return resolver.resolveReleaseNetworkPlan(ctx, task)
	}
	if task.Params[taskjournal.TaskResourceKindParam] == taskjournal.TaskResourceComponent {
		if resolver.componentPlans == nil {
			return nil, errs.New(errs.KindInternal, "Component Task plan resolver is not configured")
		}
		return resolver.componentPlans.ResolveComponentExecutionPlan(ctx, task)
	}
	if _, backingCreation := task.Params[taskjournal.TaskBackingServiceCreationParam]; task.Type == taskjournal.TaskUpdate &&
		backingCreation {
		return resolver.resolveEnvironmentBlueprintPlan(ctx, task)
	}
	if task.Type == taskjournal.TaskRemove {
		if ids.Validate(ids.KindEnvironment, task.Target) == nil {
			return resolver.resolveEnvironmentRemovalPlan(ctx, task)
		}
		if ids.Validate(ids.KindNetwork, task.Target) == nil {
			return resolver.resolveZoneRemovalPlan(ctx, task)
		}
		if ids.Validate(ids.KindService, task.Target) == nil {
			return resolver.resolveServiceRemovalPlan(ctx, task)
		}
		return nil, errs.New(errs.KindInternal, "durable removal Task target is invalid")
	}
	return nil, errs.New(errs.KindStateConflict, "operation requires native captured work; no generic Task plan is available")
}
