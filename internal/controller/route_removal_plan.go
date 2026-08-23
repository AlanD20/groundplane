package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"math"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const RouteRemovalCaddyfilePath = "components/caddy/Caddyfile"

type routeRemovalPlanStateReader interface {
	GetRouteRemovalIntent(
		context.Context,
		string,
	) (etcd.Versioned[etcd.RouteRemovalIntent], bool, error)
}

// RouteRemovalTaskProcedureIDs are the immutable identities allocated before
// publication for one Caddy-backed Route removal procedure.
type RouteRemovalTaskProcedureIDs struct {
	ArtifactID        string
	MaterializationID string
	MaterializeStepID string
	ApplyStepID       string
}

// PrepareRouteRemovalTask renders the candidate only long enough to pin its
// metadata digest, then seals the same plan ResolveExecutionPlan rebuilds after
// restart. Generated Caddyfile bytes are never stored in the Task or intent.
func (resolver *TaskPlanResolver) PrepareRouteRemovalTask(
	ctx context.Context,
	task etcd.TaskRecord,
	intent etcd.RouteRemovalIntent,
	procedure RouteRemovalTaskProcedureIDs,
) (etcd.TaskRecord, error) {
	if ids.Validate(ids.KindConfig, procedure.ArtifactID) != nil ||
		ids.Validate(ids.KindConfig, procedure.MaterializationID) != nil ||
		ids.Validate(ids.KindStep, procedure.MaterializeStepID) != nil ||
		ids.Validate(ids.KindStep, procedure.ApplyStepID) != nil ||
		intent.CandidateProjection == nil || intent.CandidateProjection.RenderGeneration > math.MaxInt32 {
		return etcd.TaskRecord{}, errs.New(errs.KindValidationFailed, "Route removal procedure ids are invalid")
	}
	componentID, _, err := routeRemovalCaddyIdentity(intent)
	if err != nil {
		return etcd.TaskRecord{}, err
	}
	reference := etcd.TaskComponentFileValueReference{
		RevisionID:         intent.CandidateProjection.BlueprintRevisionID,
		ComponentID:        componentID,
		Path:               RouteRemovalCaddyfilePath,
		RouteRemovalTaskID: task.ID,
	}
	content, err := resolver.resolveComponentFileFromProjection(
		ctx,
		intent.EnvironmentID,
		reference,
		*intent.CandidateProjection,
	)
	if err != nil {
		return etcd.TaskRecord{}, err
	}
	digest := sha256.Sum256(content)
	length := len(content)
	clear(content)
	task.Params = map[string]string{
		etcd.TaskRouteEnvironmentParam:           intent.EnvironmentID,
		etcd.TaskMaterializationEnvironmentParam: intent.EnvironmentID,
		etcd.EnvironmentBlueprintRevisionParam:   intent.CandidateProjection.BlueprintRevisionID,
		EnvironmentBlueprintArtifactParam:        procedure.ArtifactID,
	}
	task.RenderGeneration = int32(intent.CandidateProjection.RenderGeneration)
	task.Steps = []etcd.TaskStepRecord{{ID: procedure.MaterializeStepID}, {ID: procedure.ApplyStepID}}
	task.Materializations = []etcd.TaskMaterializationRecord{{
		StepID: procedure.MaterializeStepID, MaterializationID: procedure.MaterializationID,
		EnvironmentID: intent.EnvironmentID, Destination: RouteRemovalCaddyfilePath,
		OutputKind: etcd.TaskMaterializationOutputPlainFile,
		Mode:       uint32(entrymaterialization.ModeReadOnly), Length: uint64(length),
		SHA256: hex.EncodeToString(digest[:]),
		Source: etcd.TaskMaterializationSource{
			Kind: etcd.TaskMaterializationSourceComponentFile, ComponentFile: &reference,
		},
	}}
	plan, err := resolver.buildRouteRemovalPlan(ctx, task, intent)
	if err != nil {
		return etcd.TaskRecord{}, err
	}
	task.PlanHash = hex.EncodeToString(plan.PlanHash)
	return task, nil
}

func (resolver *TaskPlanResolver) resolveRouteRemovalPlan(
	ctx context.Context,
	task etcd.TaskRecord,
) (*agentpb.ExecutionPlan, error) {
	reader, ok := resolver.blueprints.(routeRemovalPlanStateReader)
	if !ok {
		return nil, errs.New(errs.KindInternal, "Route removal plan state reader is unavailable")
	}
	stored, found, err := reader.GetRouteRemovalIntent(ctx, task.ID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errs.New(errs.KindInternal, "Route removal intent is missing")
	}
	return resolver.buildRouteRemovalPlan(ctx, task, stored.Record)
}

func (resolver *TaskPlanResolver) buildRouteRemovalPlan(
	ctx context.Context,
	task etcd.TaskRecord,
	intent etcd.RouteRemovalIntent,
) (*agentpb.ExecutionPlan, error) {
	if resolver == nil || resolver.blueprints == nil || task.Executor != etcd.TaskExecutorAgent ||
		task.Type != etcd.TaskRemove || ids.Validate(ids.KindRoute, task.Target) != nil ||
		task.ID != intent.TaskID || task.Target != intent.RouteID || !task.CreatedAt.Equal(intent.CreatedAt) ||
		intent.Status != etcd.TaskStatusPending || !intent.RequiresCaddy || intent.CandidateProjection == nil ||
		intent.CurrentProjection == nil || task.TimeoutSeconds <= 0 || task.TimeoutSeconds > math.MaxUint32 ||
		len(task.Params) != 4 || len(task.Steps) != 2 || len(task.Materializations) != 1 {
		return nil, errs.New(errs.KindInternal, "durable Route removal Task shape is invalid")
	}
	candidate := *intent.CandidateProjection
	revisionID := task.Params[etcd.EnvironmentBlueprintRevisionParam]
	artifactID := task.Params[EnvironmentBlueprintArtifactParam]
	if task.Params[etcd.TaskRouteEnvironmentParam] != intent.EnvironmentID ||
		task.Params[etcd.TaskMaterializationEnvironmentParam] != intent.EnvironmentID ||
		revisionID != candidate.BlueprintRevisionID || ids.Validate(ids.KindTask, revisionID) != nil ||
		ids.Validate(ids.KindConfig, artifactID) != nil || uint64(task.RenderGeneration) != candidate.RenderGeneration ||
		ids.Validate(ids.KindStep, task.Steps[0].ID) != nil || ids.Validate(ids.KindStep, task.Steps[1].ID) != nil {
		return nil, errs.New(errs.KindInternal, "durable Route removal Task parameters are invalid")
	}
	reference := task.Materializations[0]
	componentFile := reference.Source.ComponentFile
	if reference.StepID != task.Steps[0].ID || reference.EnvironmentID != intent.EnvironmentID ||
		reference.Destination != RouteRemovalCaddyfilePath || reference.ServiceID != "" || reference.ServiceName != "" ||
		reference.OutputKind != etcd.TaskMaterializationOutputPlainFile || reference.UID != 0 || reference.GID != 0 ||
		reference.Mode != uint32(entrymaterialization.ModeReadOnly) ||
		reference.Source.Kind != etcd.TaskMaterializationSourceComponentFile || componentFile == nil ||
		componentFile.RevisionID != revisionID || componentFile.Path != RouteRemovalCaddyfilePath ||
		componentFile.RouteRemovalTaskID != task.ID {
		return nil, errs.New(errs.KindInternal, "durable Route removal materialization is invalid")
	}
	componentID, serviceID, err := routeRemovalCaddyIdentity(intent)
	if err != nil || componentFile.ComponentID != componentID {
		return nil, errs.New(errs.KindInternal, "durable Route removal Caddy identity is invalid")
	}
	content, err := resolver.resolveComponentFileFromProjection(ctx, intent.EnvironmentID, *componentFile, candidate)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(content)
	lengthMatches := reference.Length == uint64(len(content))
	clear(content)
	storedDigest, err := hex.DecodeString(reference.SHA256)
	if err != nil || !lengthMatches || !bytes.Equal(storedDigest, digest[:]) {
		return nil, errs.New(errs.KindInternal, "durable Route removal Caddyfile digest changed")
	}

	environment, err := resolver.blueprints.GetEnvironment(ctx, intent.EnvironmentID)
	if err != nil {
		return nil, err
	}
	project, err := resolver.blueprints.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return nil, err
	}
	tenant, err := resolver.blueprints.GetTenant(ctx, project.Record.TenantID)
	if err != nil {
		return nil, err
	}
	if environment.Record.ID != intent.EnvironmentID ||
		environment.Record.ProvisioningState != etcd.EnvironmentProvisioningReady ||
		project.Record.ID != environment.Record.ProjectID || project.Record.Kind != etcd.ProjectKindTenant ||
		tenant.Record.ID != project.Record.TenantID {
		return nil, errs.New(errs.KindInternal, "Route removal Environment hierarchy is invalid")
	}
	identity := pinnedEnvironmentIdentity{
		TenantID: tenant.Record.ID, TenantSlug: tenant.Record.Slug,
		ProjectID: project.Record.ID, ProjectSlug: project.Record.Slug,
		EnvironmentID: environment.Record.ID, EnvironmentName: environment.Record.Name,
		AuthorizedVolumeDir: environment.Record.VolumeDir,
	}
	artifact, err := resolver.renderPinnedEnvironmentArtifact(
		ctx, task, identity, revisionID, artifactID, candidate, nil,
	)
	if err != nil {
		return nil, err
	}
	materializationStep, err := BuildTaskMaterializationStep(
		reference,
		artifactID,
		uint32(task.TimeoutSeconds),
	)
	if err != nil {
		return nil, err
	}
	return BuildPlan(PlanBuildInput{
		VolumeRoot: resolver.volumeRoot, PlanID: task.PlanID,
		RenderGeneration: uint64(task.RenderGeneration),
		Operation:        agentpb.PlanOperation_PLAN_OPERATION_REMOVE, TargetID: task.Target,
		Artifacts: []*agentpb.ComposeArtifact{artifact},
		Steps: []*agentpb.ExecutionStep{
			materializationStep,
			{
				StepId: task.Steps[1].ID, TimeoutSeconds: uint32(task.TimeoutSeconds),
				Payload: &agentpb.ExecutionStep_CaddyConfigApply{CaddyConfigApply: &agentpb.CaddyConfigApply{
					ArtifactId: artifactID, ServiceId: serviceID,
					CaddyfileSha256: append([]byte(nil), digest[:]...),
				}},
			},
		},
	})
}

func routeRemovalCaddyIdentity(intent etcd.RouteRemovalIntent) (string, string, error) {
	if !intent.RequiresCaddy || intent.CandidateProjection == nil {
		return "", "", errs.New(errs.KindInternal, "Route removal has no Caddy candidate")
	}
	componentID := ""
	serviceID := ""
	for _, component := range intent.CandidateProjection.Components {
		if component.Desired.Kind != core.ComponentKindIngressCaddy {
			continue
		}
		if componentID != "" || !component.Desired.Enabled || len(component.Runtime.GeneratedServices) != 1 ||
			ids.Validate(ids.KindComponent, component.Desired.ID) != nil ||
			ids.Validate(ids.KindService, component.Runtime.GeneratedServices[0]) != nil {
			return "", "", errs.New(errs.KindInternal, "Route removal Caddy projection is invalid")
		}
		componentID = component.Desired.ID
		serviceID = component.Runtime.GeneratedServices[0]
	}
	if componentID == "" {
		return "", "", errs.New(errs.KindInternal, "Route removal Caddy Component is missing")
	}
	return componentID, serviceID, nil
}
