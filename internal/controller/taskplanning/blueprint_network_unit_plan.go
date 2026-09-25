package taskplanning

import (
	"context"
	"encoding/hex"
	"math"

	"github.com/AlanD20/groundplane/internal/common/ids"
	composeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	composerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	taskplan "github.com/AlanD20/groundplane/internal/controller/taskplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

type blueprintNetworkUnitStateReader interface {
	GetEnvironmentDesiredInputRevision(
		context.Context,
		string,
		string,
	) (etcdstore.Versioned[projectionrecord.EnvironmentDesiredInput], bool, error)
	GetEnvironmentOwnedIdentitiesRevision(
		context.Context,
		string,
		string,
	) (etcdstore.Versioned[projectionrecord.EnvironmentOwnedIdentities], bool, error)
}

// PrepareBlueprintNetworkUnit seals one private Network child from the exact
// authored desired revision named by its parent. The resulting Task retains
// only immutable identities; dispatch reconstructs and hashes the same plan.
func (resolver *TaskPlanResolver) PrepareBlueprintNetworkUnit(
	ctx context.Context,
	task etcd.TaskRecord,
) (etcd.TaskRecord, *agentpb.ExecutionPlan, error) {
	if len(task.Steps) != 0 || task.PlanHash != "" {
		return etcd.TaskRecord{}, nil, errs.New(
			errs.KindValidationFailed,
			"Blueprint Network child is already prepared",
		)
	}
	stepID, err := blueprintNetworkUnitStepID(task)
	if err != nil {
		return etcd.TaskRecord{}, nil, err
	}
	prepared := task
	prepared.Steps = []taskjournal.TaskStepRecord{{Kind: taskjournal.TaskStepOperation, ID: stepID}}
	plan, err := resolver.buildBlueprintNetworkUnitPlan(ctx, prepared)
	if err != nil {
		return etcd.TaskRecord{}, nil, err
	}
	prepared.PlanHash = hex.EncodeToString(plan.GetPlanHash())
	return prepared, plan, nil
}

func (resolver *TaskPlanResolver) resolveBlueprintNetworkUnitPlan(
	ctx context.Context,
	task etcd.TaskRecord,
) (*agentpb.ExecutionPlan, error) {
	return resolver.buildBlueprintNetworkUnitPlan(ctx, task)
}

func (resolver *TaskPlanResolver) buildBlueprintNetworkUnitPlan(
	ctx context.Context,
	task etcd.TaskRecord,
) (*agentpb.ExecutionPlan, error) {
	parentID := task.Params[taskjournal.TaskBlueprintParentParam]
	if resolver == nil || resolver.blueprints == nil || ctx == nil ||
		task.Type != taskjournal.TaskUpdate || task.Actor != taskjournal.TaskActorSystem ||
		task.Executor != taskjournal.TaskExecutorAgent || ids.Validate(ids.KindNetwork, task.Target) != nil ||
		task.Params[taskjournal.TaskBlueprintNetworkUnitParam] != task.Target ||
		task.Params[blueprints.EnvironmentDesiredRevisionParam] != parentID ||
		ids.Validate(ids.KindTask, parentID) != nil || parentID == task.ID ||
		task.Owner.EnvironmentID == "" || task.RenderGeneration <= 0 ||
		task.TimeoutSeconds <= 0 || task.TimeoutSeconds > math.MaxUint32 || len(task.Params) != 3 ||
		len(task.Steps) != 1 || task.Steps[0].Kind != taskjournal.TaskStepOperation {
		return nil, errs.New(errs.KindInternal, "durable Blueprint Network child shape is invalid")
	}
	expectedStepID, err := blueprintNetworkUnitStepID(task)
	if err != nil || task.Steps[0].ID != expectedStepID {
		return nil, errs.New(errs.KindInternal, "durable Blueprint Network child procedure changed")
	}
	environmentID := task.Owner.EnvironmentID
	authority, ok := resolver.blueprints.(blueprintNetworkUnitStateReader)
	if !ok {
		return nil, errs.New(errs.KindInternal, "Blueprint Network child desired authority is not configured")
	}
	desired, found, err := authority.GetEnvironmentDesiredInputRevision(ctx, environmentID, parentID)
	if err != nil {
		return nil, err
	}
	if !found || desired.Record.EnvironmentID != environmentID || desired.Record.RevisionID != parentID ||
		desired.Record.RenderGeneration != uint64(task.RenderGeneration) {
		return nil, errs.New(errs.KindStateConflict, "Blueprint Network child desired input changed")
	}
	identities, found, err := authority.GetEnvironmentOwnedIdentitiesRevision(ctx, environmentID, parentID)
	if err != nil {
		return nil, err
	}
	if !found || identities.Record.EnvironmentID != environmentID || identities.Record.RevisionID != parentID ||
		identities.Record.RenderGeneration != desired.Record.RenderGeneration {
		return nil, errs.New(errs.KindStateConflict, "Blueprint Network child identity authority changed")
	}
	var networkIdentity composeidentity.Resource
	for _, identity := range identities.Record.Networks {
		if identity.ID != task.Target {
			continue
		}
		if networkIdentity.ID != "" {
			return nil, errs.New(errs.KindInternal, "Blueprint Network child identity is duplicated")
		}
		networkIdentity = composeidentity.Resource{ID: identity.ID, Name: identity.Name}
	}
	if networkIdentity.ID == "" {
		return nil, errs.New(errs.KindStateConflict, "Blueprint Network child identity is absent")
	}
	projectInput, err := composerender.LoadNormalizedEnvironmentDesiredProject(
		ctx,
		desired.Record,
		identities.Record,
	)
	if err != nil {
		return nil, err
	}
	network, found := projectInput.Networks[networkIdentity.Name]
	if !found || bool(network.External) {
		return nil, errs.New(errs.KindStateConflict, "Blueprint Network child authored Zone changed")
	}
	environment, err := resolver.blueprints.GetEnvironment(ctx, environmentID)
	if err != nil {
		return nil, err
	}
	project, err := resolver.blueprints.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return nil, err
	}
	expectedOwner, err := taskjournal.EnvironmentTaskOwner(project.Record, environment.Record)
	if err != nil {
		return nil, err
	}
	if task.Owner != expectedOwner ||
		environment.Record.ProvisioningState != hierarchyrecord.EnvironmentProvisioningReady ||
		desired.Record.Input.NetworkPool != environment.Record.NetworkPool {
		return nil, errs.New(errs.KindStateConflict, "Blueprint Network child Environment hierarchy changed")
	}
	ownerKind := composerender.ComposeProjectOwnerBacking
	tenantID := ""
	if project.Record.Kind == hierarchyrecord.ProjectKindTenant {
		tenant, tenantErr := resolver.blueprints.GetTenant(ctx, project.Record.TenantID)
		if tenantErr != nil {
			return nil, tenantErr
		}
		if tenant.Record.ID != project.Record.TenantID {
			return nil, errs.New(errs.KindStateConflict, "Blueprint Network child Tenant changed")
		}
		ownerKind = composerender.ComposeProjectOwnerTenant
		tenantID = tenant.Record.ID
	} else if project.Record.Kind != hierarchyrecord.ProjectKindBacking || project.Record.TenantID != "" {
		return nil, errs.New(errs.KindStateConflict, "Blueprint Network child Project ownership changed")
	}
	artifactID, err := blueprintNetworkUnitArtifactID(task)
	if err != nil {
		return nil, err
	}
	artifact, err := composerender.RenderCompose(composerender.ComposeRenderInput{
		Project: &composetypes.Project{
			Name:     projectInput.Name,
			Networks: composetypes.Networks{networkIdentity.Name: network},
		},
		ArtifactID: artifactID, ProjectOwnerKind: ownerKind,
		TenantID: tenantID, ProjectID: project.Record.ID, EnvironmentID: environmentID,
		PlanID: task.PlanID, RenderGeneration: uint64(task.RenderGeneration),
		AuthorizedVolumeDir: environment.Record.VolumeDir,
		Identities:          composeidentity.Snapshot{Networks: []composeidentity.Resource{networkIdentity}},
	})
	if err != nil {
		return nil, err
	}
	if len(artifact.GetNetworks()) != 1 || artifact.GetNetworks()[0].GetNetworkId() != task.Target ||
		len(artifact.GetServices()) != 0 || len(artifact.GetVolumes()) != 0 {
		return nil, errs.New(errs.KindInternal, "Blueprint Network child artifact is not resource scoped")
	}
	step := &agentpb.ExecutionStep{
		StepId: task.Steps[0].ID, TimeoutSeconds: uint32(task.TimeoutSeconds),
		Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
		Payload: &agentpb.ExecutionStep_ManagedNetworkEnsure{ManagedNetworkEnsure: &agentpb.ManagedNetworkEnsure{
			ArtifactId: artifactID, NetworkId: task.Target,
		}},
	}
	return taskplan.Build(taskplan.BuildInput{
		VolumeRoot: resolver.volumeRoot, PlanID: task.PlanID,
		RenderGeneration: uint64(task.RenderGeneration),
		Operation:        agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY,
		TargetID:         environmentID, Artifacts: []*agentpb.ComposeArtifact{artifact},
		Steps: []*agentpb.ExecutionStep{step},
	})
}

func blueprintNetworkUnitStepID(task etcd.TaskRecord) (string, error) {
	planTime, err := ids.Timestamp(ids.KindPlan, task.PlanID)
	if err != nil {
		return "", err
	}
	return ids.DeriveAt(ids.KindStep, planTime, task.PlanID, "blueprint-network-unit:"+task.Target), nil
}

func blueprintNetworkUnitArtifactID(task etcd.TaskRecord) (string, error) {
	planTime, err := ids.Timestamp(ids.KindPlan, task.PlanID)
	if err != nil {
		return "", err
	}
	return ids.DeriveAt(ids.KindConfig, planTime, task.PlanID, "blueprint-network-unit:"+task.Target), nil
}
