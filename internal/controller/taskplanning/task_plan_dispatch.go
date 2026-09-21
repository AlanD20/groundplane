package taskplanning

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	componentrender "github.com/AlanD20/groundplane/internal/controller/componentrender"
	"github.com/AlanD20/groundplane/internal/controller/configurationrecovery"
	"github.com/AlanD20/groundplane/internal/controller/taskcontract"
	taskplan "github.com/AlanD20/groundplane/internal/controller/taskplan"
	releasedomain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"math"
)

// TaskPlanResolver rebuilds plans from closed durable Task inputs and daemon-owned policy; plans are never stored.
type TaskPlanResolver struct {
	volumeRoot            string
	blueprints            blueprintPlanStateReader
	attaches              attachPlanRecordReader
	services              attachPlanServiceReader
	attachIdentities      attachPlanIdentityResolver
	componentCatalog      []componentrender.EnvironmentComponentRegistration
	serviceProxyImage     *releasedomain.ProxyImage
	routeState            routeProviderStateReader
	releases              *etcd.ReleaseLedger
	backupRuns            backupRunPlanReader
	componentPlans        ComponentTaskPlanResolver
	materializations      componentMaterializationContentRepository
	configurationRecovery *configurationrecovery.Sources
	scriptPlans           ScriptExecutionPlanReader
	volumeRemovalPlans    volumeRemovalPlanReader
}

type blueprintPlanStateReader interface {
	GetTenant(context.Context, string) (etcdstore.Versioned[hierarchyrecord.TenantRecord], error)
	GetProject(context.Context, string) (etcdstore.Versioned[hierarchyrecord.ProjectRecord], error)
	GetEnvironment(context.Context, string) (etcdstore.Versioned[hierarchyrecord.EnvironmentRecord], error)
	GetEnvironmentBlueprintRevision(
		context.Context,
		string,
		string,
	) (etcdstore.Versioned[etcd.EnvironmentBlueprintRevision], bool, error)
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
	if task.Type == taskjournal.TaskStart || task.Type == taskjournal.TaskStop || task.Type == taskjournal.TaskDestroy {
		return resolver.resolveServiceLifecyclePlan(ctx, task)
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
	if task.Params[etcd.TaskResourceKindParam] == etcd.TaskResourceComponent {
		if resolver.componentPlans == nil {
			return nil, errs.New(errs.KindInternal, "Component Task plan resolver is not configured")
		}
		return resolver.componentPlans.ResolveComponentExecutionPlan(ctx, task)
	}
	if task.Type == taskjournal.TaskBackup {
		return resolver.resolveBackupRunPlan(ctx, task)
	}
	if task.Params[etcd.TaskResourceKindParam] == etcd.TaskResourceHierarchyDeletion {
		return resolver.resolveHierarchyDeletionPlan(ctx, task)
	}
	if task.Params[etcd.TaskResourceKindParam] == etcd.TaskResourceVolume {
		return resolver.resolveVolumePlan(ctx, task)
	}
	if task.Type == taskjournal.TaskAttach || task.Type == taskjournal.TaskDetach {
		return resolver.resolveAttachPlan(ctx, task)
	}
	if _, backingCreation := task.Params[taskjournal.TaskBackingServiceCreationParam]; task.Type == taskjournal.TaskUpdate && backingCreation {
		return resolver.resolveEnvironmentBlueprintPlan(ctx, task)
	}
	if task.Type == taskjournal.TaskUpdate {
		return resolver.resolveUpdatePlan(ctx, task)
	}
	if task.Type == taskjournal.TaskCreate && ids.Validate(ids.KindRoute, task.Target) == nil {
		return resolver.resolveRouteMutationPlan(ctx, task)
	}
	if task.Type == taskjournal.TaskRemove {
		if ids.Validate(ids.KindEnvironment, task.Target) == nil {
			return resolver.resolveEnvironmentRemovalPlan(ctx, task)
		}
		if ids.Validate(ids.KindNetwork, task.Target) == nil {
			return resolver.resolveZoneRemovalPlan(ctx, task)
		}
		if ids.Validate(ids.KindRoute, task.Target) == nil {
			return resolver.resolveRouteRemovalPlan(ctx, task)
		}
		if ids.Validate(ids.KindEnvEntry, task.Target) == nil {
			return resolver.resolveEntryRemovalPlan(ctx, task)
		}
		if ids.Validate(ids.KindService, task.Target) == nil {
			return resolver.resolveServiceRemovalPlan(ctx, task)
		}
		return nil, errs.New(errs.KindInternal, "durable removal Task target is invalid")
	}
	if task.Executor != taskjournal.TaskExecutorAgent || task.Type != taskjournal.TaskCreate ||
		ids.Validate(ids.KindEnvironment, task.Target) != nil || len(task.Params) != 1 ||
		len(task.Steps) != 1 || task.TimeoutSeconds <= 0 || task.TimeoutSeconds > math.MaxUint32 {
		return nil, errs.New(errs.KindInternal, "durable Environment creation Task shape is invalid")
	}
	volumeDirectory, exists := task.Params[taskcontract.EnvironmentCreateVolumeDirectoryParam]
	if !exists || ids.Validate(ids.KindStep, task.Steps[0].ID) != nil {
		return nil, errs.New(errs.KindInternal, "durable Environment creation Task procedure is invalid")
	}
	return taskplan.Build(taskplan.BuildInput{
		VolumeRoot: resolver.volumeRoot, PlanID: task.PlanID,
		RenderGeneration: uint64(task.RenderGeneration),
		Operation:        agentpb.PlanOperation_PLAN_OPERATION_ENVIRONMENT_CREATE,
		TargetID:         task.Target,
		Steps: []*agentpb.ExecutionStep{{
			StepId: task.Steps[0].ID, TimeoutSeconds: uint32(task.TimeoutSeconds),
			Payload: &agentpb.ExecutionStep_EnvironmentDirectoryCreate{
				EnvironmentDirectoryCreate: &agentpb.EnvironmentDirectoryCreate{
					EnvironmentId: task.Target, ExpectedVolumeDir: volumeDirectory,
				},
			},
		}},
	})
}
