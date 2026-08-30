package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type routeMutationPlanStateReader interface {
	GetRouteMutationIntent(context.Context, string) (etcd.Versioned[etcd.RouteMutationIntent], bool, error)
}

func (resolver *TaskPlanResolver) PrepareRouteMutationTask(ctx context.Context, task etcd.TaskRecord, intent etcd.RouteMutationIntent, procedure etcd.RouteMutationProcedureIDs) (etcd.RouteMutationTaskPreparation, error) {
	if intent.CurrentProjection == nil {
		return prepareNativeRouteMutation(task, intent)
	}
	candidate, err := etcd.ApplyEnvironmentRoute(*intent.CurrentProjection, intent.Route)
	if err != nil {
		return etcd.RouteMutationTaskPreparation{}, err
	}
	pin, err := resolver.pinRouteProvider(ctx, intent.EnvironmentID, intent.CurrentProjectionRevision, candidate, &intent.Route, "", intent.Route.DesiredGeneration)
	if err != nil {
		return etcd.RouteMutationTaskPreparation{}, err
	}
	if pin == nil {
		intent.CurrentProjection = nil
		intent.CurrentProjectionRevision = 0
		return prepareNativeRouteMutation(task, intent)
	}
	intent.CandidateProjection = &candidate
	intent.Provider = pin
	observation := etcd.RouteProviderObservation{ComponentID: pin.ComponentID, DefinitionDigest: pin.DefinitionDigest, CatalogDigest: pin.CatalogDigest, InputRevision: pin.InputRevision, InputGeneration: pin.InputGeneration}
	intent.Route, err = etcd.SetRouteObservation(intent.Route, etcd.RouteObservation{Status: etcd.RouteObservedPending, DesiredGeneration: intent.Route.DesiredGeneration, Provider: observation})
	if err != nil {
		return etcd.RouteMutationTaskPreparation{}, err
	}
	if ids.Validate(ids.KindConfig, procedure.ArtifactID) != nil || ids.Validate(ids.KindConfig, procedure.MaterializationID) != nil || ids.Validate(ids.KindStep, procedure.MaterializeStepID) != nil || ids.Validate(ids.KindStep, procedure.ApplyStepID) != nil || ids.Validate(ids.KindStep, procedure.ActivateStepID) != nil || candidate.RenderGeneration > math.MaxInt32 {
		return etcd.RouteMutationTaskPreparation{}, errs.New(errs.KindValidationFailed, "Route mutation procedure ids are invalid")
	}
	content, err := resolver.routeProviderFile(*pin, candidate)
	if err != nil {
		return etcd.RouteMutationTaskPreparation{}, err
	}
	digest := sha256.Sum256(content)
	length := len(content)
	clear(content)
	reference := etcd.TaskComponentFileValueReference{RevisionID: candidate.RevisionID, ComponentID: pin.ComponentID, Path: pin.Destination, RouteTaskID: task.ID}
	task.Executor = etcd.TaskExecutorAgent
	task.TimeoutSeconds = 120
	task.RenderGeneration = int32(candidate.RenderGeneration)
	task.Params = map[string]string{etcd.TaskResourceKindParam: etcd.TaskResourceRoute, etcd.TaskRouteEnvironmentParam: intent.EnvironmentID, etcd.TaskMaterializationEnvironmentParam: intent.EnvironmentID, etcd.EnvironmentDesiredRevisionParam: candidate.RevisionID, EnvironmentBlueprintArtifactParam: procedure.ArtifactID}
	task.Steps = []etcd.TaskStepRecord{{ID: procedure.MaterializeStepID}, {ID: procedure.ApplyStepID}, {ID: procedure.ActivateStepID}}
	task.Materializations = []etcd.TaskMaterializationRecord{{StepID: procedure.MaterializeStepID, MaterializationID: procedure.MaterializationID, EnvironmentID: intent.EnvironmentID, Destination: pin.Destination, OutputKind: etcd.TaskMaterializationOutputPlainFile, Mode: uint32(entrymaterialization.ModeReadOnly), Length: uint64(length), SHA256: hex.EncodeToString(digest[:]), Source: etcd.TaskMaterializationSource{Kind: etcd.TaskMaterializationSourceComponentFile, ComponentFile: &reference}}}
	plan, err := resolver.buildRouteMutationPlan(ctx, task, intent)
	if err != nil {
		return etcd.RouteMutationTaskPreparation{}, err
	}
	task.PlanHash = hex.EncodeToString(plan.PlanHash)
	return etcd.RouteMutationTaskPreparation{Intent: intent, Task: task}, nil
}

func prepareNativeRouteMutation(task etcd.TaskRecord, intent etcd.RouteMutationIntent) (etcd.RouteMutationTaskPreparation, error) {
	task.Executor = etcd.TaskExecutorController
	task.TimeoutSeconds = 30
	task.RenderGeneration = int32(intent.Route.DesiredGeneration)
	task.Params = map[string]string{etcd.TaskResourceKindParam: etcd.TaskResourceRoute, etcd.TaskRouteEnvironmentParam: intent.EnvironmentID}
	task.Steps = []etcd.TaskStepRecord{{ID: ids.New(ids.KindStep)}}
	value, err := json.Marshal(struct {
		Version    int                    `json:"version"`
		Kind       etcd.RouteMutationKind `json:"kind"`
		RouteID    string                 `json:"route_id"`
		Generation uint64                 `json:"generation"`
	}{1, intent.Kind, intent.RouteID, intent.Route.DesiredGeneration})
	if err != nil {
		return etcd.RouteMutationTaskPreparation{}, err
	}
	digest := sha256.Sum256(value)
	clear(value)
	task.PlanHash = hex.EncodeToString(digest[:])
	return etcd.RouteMutationTaskPreparation{Intent: intent, Task: task}, nil
}

func (resolver *TaskPlanResolver) resolveRouteMutationPlan(ctx context.Context, task etcd.TaskRecord) (*agentpb.ExecutionPlan, error) {
	reader, ok := resolver.blueprints.(routeMutationPlanStateReader)
	if !ok {
		return nil, errs.New(errs.KindInternal, "Route mutation plan state reader is unavailable")
	}
	stored, found, err := reader.GetRouteMutationIntent(ctx, task.ID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errs.New(errs.KindInternal, "Route mutation intent is missing")
	}
	return resolver.buildRouteMutationPlan(ctx, task, stored.Record)
}

func (resolver *TaskPlanResolver) buildRouteMutationPlan(ctx context.Context, task etcd.TaskRecord, intent etcd.RouteMutationIntent) (*agentpb.ExecutionPlan, error) {
	if resolver == nil || resolver.blueprints == nil || task.Executor != etcd.TaskExecutorAgent || (task.Type != etcd.TaskCreate && task.Type != etcd.TaskUpdate) || task.ID != intent.TaskID || task.Target != intent.RouteID || intent.Status != etcd.TaskStatusPending || intent.Provider == nil || intent.CandidateProjection == nil || intent.CurrentProjection == nil || len(task.Params) != 5 || len(task.Steps) != 3 || len(task.Materializations) != 1 {
		return nil, errs.New(errs.KindInternal, "durable Route mutation Task shape is invalid")
	}
	pin := *intent.Provider
	candidate := *intent.CandidateProjection
	revisionID := task.Params[etcd.EnvironmentDesiredRevisionParam]
	artifactID := task.Params[EnvironmentBlueprintArtifactParam]
	if task.Params[etcd.TaskResourceKindParam] != etcd.TaskResourceRoute || task.Params[etcd.TaskRouteEnvironmentParam] != intent.EnvironmentID || task.Params[etcd.TaskMaterializationEnvironmentParam] != intent.EnvironmentID || revisionID != candidate.RevisionID || uint64(task.RenderGeneration) != candidate.RenderGeneration {
		return nil, errs.New(errs.KindInternal, "durable Route mutation Task parameters are invalid")
	}
	reference := task.Materializations[0]
	componentFile := reference.Source.ComponentFile
	if componentFile == nil || reference.Destination != pin.Destination || componentFile.RouteTaskID != task.ID || componentFile.ComponentID != pin.ComponentID || componentFile.Path != pin.Destination {
		return nil, errs.New(errs.KindInternal, "durable Route mutation materialization is invalid")
	}
	content, err := resolver.routeProviderFile(pin, candidate)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(content)
	lengthMatches := reference.Length == uint64(len(content))
	clear(content)
	storedDigest, err := hex.DecodeString(reference.SHA256)
	if err != nil || !lengthMatches || !bytes.Equal(storedDigest, digest[:]) {
		return nil, errs.New(errs.KindInternal, "durable Route managed configuration changed")
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
	identity := pinnedEnvironmentIdentity{TenantID: tenant.Record.ID, TenantSlug: tenant.Record.Slug, ProjectID: project.Record.ID, ProjectSlug: project.Record.Slug, EnvironmentID: environment.Record.ID, EnvironmentName: environment.Record.Name, AuthorizedVolumeDir: environment.Record.VolumeDir}
	artifact, err := resolver.renderPinnedEnvironmentArtifact(ctx, task, identity, revisionID, artifactID, candidate, nil)
	if err != nil {
		return nil, err
	}
	materializationStep, err := BuildTaskMaterializationStep(reference, artifactID, uint32(task.TimeoutSeconds))
	if err != nil {
		return nil, err
	}
	definitionBytes, _ := hex.DecodeString(pin.DefinitionDigest)
	catalogBytes, _ := hex.DecodeString(pin.CatalogDigest)
	var definitionDigest, catalogDigest [sha256.Size]byte
	copy(definitionDigest[:], definitionBytes)
	copy(catalogDigest[:], catalogBytes)
	action, err := BuildPinnedEnvironmentComponentAction(resolver.componentCatalog, pin.ComponentID, definitionDigest, catalogDigest, componentsdk.ActionID(pin.ActionID), reference.MaterializationID, digest, uint64(task.RenderGeneration))
	if err != nil {
		return nil, err
	}
	operation := agentpb.PlanOperation_PLAN_OPERATION_RECONCILE
	return BuildPlan(PlanBuildInput{VolumeRoot: resolver.volumeRoot, PlanID: task.PlanID, RenderGeneration: uint64(task.RenderGeneration), Operation: operation, TargetID: task.Target, Artifacts: []*agentpb.ComposeArtifact{artifact}, Steps: []*agentpb.ExecutionStep{materializationStep, {StepId: task.Steps[1].ID, TimeoutSeconds: uint32(task.TimeoutSeconds), PrerequisiteStepId: task.Steps[0].ID, Payload: &agentpb.ExecutionStep_ComposeApply{ComposeApply: &agentpb.ComposeApply{ArtifactId: artifactID, ServiceIds: []string{pin.ServiceID}, ForceRecreate: true, NoDependencies: true}}}, {StepId: task.Steps[2].ID, TimeoutSeconds: uint32(task.TimeoutSeconds), PrerequisiteStepId: task.Steps[1].ID, Payload: &agentpb.ExecutionStep_ComponentApply{ComponentApply: action}}}})
}
