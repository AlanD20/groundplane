package taskplanning

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	materializationrecord "github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	componentrender "github.com/AlanD20/groundplane/internal/controller/componentrender"
	"github.com/AlanD20/groundplane/internal/controller/taskcontract"
	taskmaterialization "github.com/AlanD20/groundplane/internal/controller/taskmaterialization"
	taskplan "github.com/AlanD20/groundplane/internal/controller/taskplan"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	environmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	routerecord "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"math"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type routeMutationPlanStateReader interface {
	GetRouteMutationIntent(
		context.Context,
		string,
	) (etcdstore.Versioned[environmentchanges.RouteMutationIntent], bool, error)
}

func (resolver *TaskPlanResolver) PrepareRouteMutationTask(
	ctx context.Context,
	task etcd.TaskRecord,
	intent environmentchanges.RouteMutationIntent,
	procedure environmentchanges.RouteMutationProcedureIDs,
) (etcd.RouteMutationTaskPreparation, error) {
	if intent.CurrentProjection == nil {
		return prepareNativeRouteMutation(task, intent)
	}
	candidate, err := projectionrecord.ApplyEnvironmentRoute(*intent.CurrentProjection, intent.Route)
	if err != nil {
		return etcd.RouteMutationTaskPreparation{}, err
	}
	candidate.RevisionID = task.ID
	pin, err := resolver.pinRouteProvider(
		ctx,
		intent.EnvironmentID,
		intent.CurrentProjectionRevision,
		candidate,
		&intent.Route,
		"",
		intent.Route.DesiredGeneration,
	)
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
	observation := routerecord.ProviderObservation{
		ComponentID:      pin.ComponentID,
		DefinitionDigest: pin.DefinitionDigest,
		CatalogDigest:    pin.CatalogDigest,
		InputRevision:    pin.InputRevision,
		InputGeneration:  pin.InputGeneration,
	}
	intent.Route, err = routerecord.SetObservation(
		intent.Route,
		routerecord.Observation{
			Status:            routerecord.ObservedPending,
			DesiredGeneration: intent.Route.DesiredGeneration,
			Provider:          observation,
		},
	)
	if err != nil {
		return etcd.RouteMutationTaskPreparation{}, err
	}
	if ids.Validate(ids.KindConfig, procedure.ArtifactID) != nil ||
		ids.Validate(ids.KindConfig, procedure.MaterializationID) != nil ||
		ids.Validate(ids.KindStep, procedure.MaterializeStepID) != nil ||
		ids.Validate(ids.KindStep, procedure.ApplyStepID) != nil ||
		ids.Validate(ids.KindStep, procedure.ActivateStepID) != nil ||
		candidate.RenderGeneration > math.MaxInt32 {
		return etcd.RouteMutationTaskPreparation{}, errs.New(
			errs.KindValidationFailed,
			"Route mutation procedure ids are invalid",
		)
	}
	content, err := resolver.routeProviderFile(*pin, candidate)
	if err != nil {
		return etcd.RouteMutationTaskPreparation{}, err
	}
	digest := sha256.Sum256(content)
	length := len(content)
	reference := materializationrecord.ComponentFileValueReference{
		RevisionID:  candidate.RevisionID,
		ComponentID: pin.ComponentID,
		Path:        pin.Destination,
		RouteTaskID: task.ID,
	}
	task.Executor = taskjournal.TaskExecutorAgent
	task.TimeoutSeconds = 120
	task.RenderGeneration = int32(candidate.RenderGeneration)
	task.Params = map[string]string{
		taskjournal.TaskResourceKindParam:               taskjournal.TaskResourceRoute,
		taskjournal.TaskRouteEnvironmentParam:           intent.EnvironmentID,
		taskjournal.TaskMaterializationEnvironmentParam: intent.EnvironmentID,
		blueprints.EnvironmentDesiredRevisionParam:      candidate.RevisionID,
		taskcontract.EnvironmentBlueprintArtifactParam:  procedure.ArtifactID,
	}
	task.Steps = []taskjournal.TaskStepRecord{
		{Kind: taskjournal.TaskStepOperation, ID: procedure.MaterializeStepID},
		{Kind: taskjournal.TaskStepOperation, ID: procedure.ApplyStepID},
		{Kind: taskjournal.TaskStepOperation, ID: procedure.ActivateStepID},
	}
	materialization := materializationrecord.Record{
		StepID:            procedure.MaterializeStepID,
		MaterializationID: procedure.MaterializationID,
		EnvironmentID:     intent.EnvironmentID,
		Destination:       pin.Destination,
		OutputKind:        materializationrecord.OutputPlainFile,
		Mode:              uint32(entrymaterialization.ModeReadOnly),
		Length:            uint64(length),
		SHA256:            hex.EncodeToString(digest[:]),
		Source: materializationrecord.Source{
			Kind:          materializationrecord.SourceComponentFile,
			ComponentFile: &reference,
		},
	}
	err = resolver.retainComponentMaterialization(ctx, materialization, candidate.RenderGeneration, content)
	clear(content)
	if err != nil {
		return etcd.RouteMutationTaskPreparation{}, err
	}
	task.Materializations = []materializationrecord.Record{materialization}
	task, err = resolver.prepareRouteContainerConfiguration(ctx, task, pin.InputRevision)
	if err != nil {
		return etcd.RouteMutationTaskPreparation{}, err
	}
	input, err := resolver.buildRouteMutationInput(ctx, task, intent)
	if err != nil {
		return etcd.RouteMutationTaskPreparation{}, err
	}
	task, plan, err := resolver.sealRouteRecoveryPlan(ctx, task, intent.Provider, input)
	if err != nil {
		return etcd.RouteMutationTaskPreparation{}, err
	}
	task.PlanHash = hex.EncodeToString(plan.PlanHash)
	intent.Plan, err = (proto.MarshalOptions{Deterministic: true}).Marshal(plan)
	if err != nil {
		return etcd.RouteMutationTaskPreparation{}, errs.Wrap(errs.KindInternal, err)
	}
	task.Steps = taskjournal.CaptureStepDescriptions(task.Steps, plan)
	return etcd.RouteMutationTaskPreparation{Intent: intent, Task: task}, nil
}

func prepareNativeRouteMutation(
	task etcd.TaskRecord,
	intent environmentchanges.RouteMutationIntent,
) (etcd.RouteMutationTaskPreparation, error) {
	task.Executor = taskjournal.TaskExecutorController
	task.TimeoutSeconds = 30
	task.RenderGeneration = int32(intent.Route.DesiredGeneration)
	task.Params = map[string]string{
		taskjournal.TaskResourceKindParam:     taskjournal.TaskResourceRoute,
		taskjournal.TaskRouteEnvironmentParam: intent.EnvironmentID,
	}
	task.Steps = []taskjournal.TaskStepRecord{
		{
			Kind:           taskjournal.TaskStepOperation,
			ID:             ids.New(ids.KindStep),
			Action:         "Publish Route configuration",
			Description:    "Commit the selected Route's desired configuration without changing a running workload.",
			Target:         intent.RouteID,
			TimeoutSeconds: 30,
		},
	}
	value, err := json.Marshal(struct {
		Version    int                                  `json:"version"`
		Kind       environmentchanges.RouteMutationKind `json:"kind"`
		RouteID    string                               `json:"route_id"`
		Generation uint64                               `json:"generation"`
	}{1, intent.Kind, intent.RouteID, intent.Route.DesiredGeneration})
	if err != nil {
		return etcd.RouteMutationTaskPreparation{}, err
	}
	digest := sha256.Sum256(value)
	clear(value)
	task.PlanHash = hex.EncodeToString(digest[:])
	return etcd.RouteMutationTaskPreparation{Intent: intent, Task: task}, nil
}

func (resolver *TaskPlanResolver) resolveRouteMutationPlan(
	ctx context.Context,
	task etcd.TaskRecord,
) (*agentpb.ExecutionPlan, error) {
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
	intent := stored.Record
	expectedType := taskjournal.TaskCreate
	if intent.Kind == environmentchanges.RouteMutationEdit {
		expectedType = taskjournal.TaskUpdate
	}
	if task.ID != intent.TaskID || task.OperationID != intent.OperationID || task.Type != expectedType ||
		task.Target != intent.RouteID || !task.CreatedAt.Equal(intent.CreatedAt) ||
		intent.Status != taskjournal.TaskStatusPending {
		return nil, errs.New(errs.KindStateConflict, "Route mutation intent does not belong to its Task")
	}
	plan, err := environmentchanges.OpenRouteExecutionPlan(stored.Record.Plan, stored.Record.Provider,
		stored.Record.EnvironmentID, stored.Record.RouteID, intent.CandidateProjection.RenderGeneration, false)
	if err != nil {
		return nil, err
	}
	return resolver.validateSealedRouteTask(task, plan, intent.Provider, intent.CandidateProjection, false)
}

func (resolver *TaskPlanResolver) buildRouteMutationInput(
	ctx context.Context,
	task etcd.TaskRecord,
	intent environmentchanges.RouteMutationIntent,
) (taskplan.BuildInput, error) {
	if resolver == nil || resolver.blueprints == nil || task.Executor != taskjournal.TaskExecutorAgent ||
		(task.Type != taskjournal.TaskCreate && task.Type != taskjournal.TaskUpdate) ||
		task.ID != intent.TaskID ||
		task.Target != intent.RouteID ||
		intent.Status != taskjournal.TaskStatusPending ||
		intent.Provider == nil ||
		intent.CandidateProjection == nil ||
		intent.CurrentProjection == nil ||
		len(task.Params) != 5 ||
		len(task.Steps) != 3 ||
		len(task.Materializations) != 1 {
		return taskplan.BuildInput{}, errs.New(errs.KindInternal, "durable Route mutation Task shape is invalid")
	}
	pin := *intent.Provider
	candidate := *intent.CandidateProjection
	revisionID := task.Params[blueprints.EnvironmentDesiredRevisionParam]
	artifactID := task.Params[taskcontract.EnvironmentBlueprintArtifactParam]
	if task.Params[taskjournal.TaskResourceKindParam] != taskjournal.TaskResourceRoute ||
		task.Params[taskjournal.TaskRouteEnvironmentParam] != intent.EnvironmentID ||
		task.Params[taskjournal.TaskMaterializationEnvironmentParam] != intent.EnvironmentID ||
		revisionID != candidate.RevisionID ||
		uint64(task.RenderGeneration) != candidate.RenderGeneration {
		return taskplan.BuildInput{}, errs.New(errs.KindInternal, "durable Route mutation Task parameters are invalid")
	}
	reference := task.Materializations[0]
	componentFile := reference.Source.ComponentFile
	if componentFile == nil || reference.Destination != pin.Destination || componentFile.RouteTaskID != task.ID ||
		componentFile.ComponentID != pin.ComponentID ||
		componentFile.Path != pin.Destination {
		return taskplan.BuildInput{}, errs.New(errs.KindInternal, "durable Route mutation materialization is invalid")
	}
	content, err := resolver.loadComponentMaterialization(ctx, reference, uint64(task.RenderGeneration))
	if err != nil {
		return taskplan.BuildInput{}, err
	}
	digest := sha256.Sum256(content)
	lengthMatches := reference.Length == uint64(len(content))
	clear(content)
	storedDigest, err := hex.DecodeString(reference.SHA256)
	if err != nil || !lengthMatches || !bytes.Equal(storedDigest, digest[:]) {
		return taskplan.BuildInput{}, errs.New(errs.KindInternal, "durable Route managed configuration changed")
	}
	environment, err := resolver.blueprints.GetEnvironment(ctx, intent.EnvironmentID)
	if err != nil {
		return taskplan.BuildInput{}, err
	}
	project, err := resolver.blueprints.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return taskplan.BuildInput{}, err
	}
	tenant, err := resolver.blueprints.GetTenant(ctx, project.Record.TenantID)
	if err != nil {
		return taskplan.BuildInput{}, err
	}
	identity := pinnedEnvironmentIdentity{
		TenantID:            tenant.Record.ID,
		TenantSlug:          tenant.Record.Slug,
		ProjectID:           project.Record.ID,
		ProjectSlug:         project.Record.Slug,
		EnvironmentID:       environment.Record.ID,
		EnvironmentName:     environment.Record.Name,
		AuthorizedVolumeDir: environment.Record.VolumeDir,
	}
	artifact, err := resolver.renderPinnedEnvironmentArtifact(
		ctx,
		task,
		identity,
		revisionID,
		artifactID,
		candidate,
		routeProviderCompose(pin),
	)
	if err != nil {
		return taskplan.BuildInput{}, err
	}
	materializationStep, err := taskmaterialization.BuildTaskMaterializationStep(
		reference,
		artifactID,
		uint32(task.TimeoutSeconds),
	)
	if err != nil {
		return taskplan.BuildInput{}, err
	}
	definitionBytes, _ := hex.DecodeString(pin.DefinitionDigest)
	catalogBytes, _ := hex.DecodeString(pin.CatalogDigest)
	var definitionDigest, catalogDigest [sha256.Size]byte
	copy(definitionDigest[:], definitionBytes)
	copy(catalogDigest[:], catalogBytes)
	action, err := componentrender.BuildPinnedEnvironmentComponentAction(
		resolver.componentCatalog,
		pin.ComponentID,
		definitionDigest,
		catalogDigest,
		componentsdk.ActionID(pin.ActionID),
		reference.MaterializationID,
		digest,
		uint64(task.RenderGeneration),
	)
	if err != nil {
		return taskplan.BuildInput{}, err
	}
	operation := agentpb.PlanOperation_PLAN_OPERATION_RECONCILE
	if err := resolver.bindContainerConfigTransition(ctx, task, action); err != nil {
		return taskplan.BuildInput{}, err
	}
	return taskplan.BuildInput{
		VolumeRoot:       resolver.volumeRoot,
		PlanID:           task.PlanID,
		RenderGeneration: uint64(task.RenderGeneration),
		Operation:        operation,
		TargetID:         task.Target,
		Artifacts:        []*agentpb.ComposeArtifact{artifact},
		Steps: []*agentpb.ExecutionStep{
			materializationStep,
			{
				StepId:             task.Steps[1].ID,
				TimeoutSeconds:     uint32(task.TimeoutSeconds),
				PrerequisiteStepId: task.Steps[0].ID,
				Payload: &agentpb.ExecutionStep_ComposeApply{
					ComposeApply: &agentpb.ComposeApply{
						ArtifactId:     artifactID,
						ServiceIds:     []string{pin.ServiceID},
						NoDependencies: true,
					},
				},
			},
			{
				StepId:             task.Steps[2].ID,
				TimeoutSeconds:     uint32(task.TimeoutSeconds),
				PrerequisiteStepId: task.Steps[1].ID,
				Payload:            &agentpb.ExecutionStep_ComponentApply{ComponentApply: action},
			},
		},
	}, nil
}
