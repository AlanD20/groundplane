package taskplanning

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	componentrender "github.com/AlanD20/groundplane/internal/controller/componentrender"
	taskmaterialization "github.com/AlanD20/groundplane/internal/controller/taskmaterialization"
	taskplan "github.com/AlanD20/groundplane/internal/controller/taskplan"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"math"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type routeRemovalPlanStateReader interface {
	GetRouteRemovalIntent(context.Context, string) (etcd.Versioned[etcd.RouteRemovalIntent], bool, error)
}

type RouteRemovalTaskProcedureIDs struct {
	ArtifactID         string
	MaterializationID  string
	MaterializeStepID  string
	ComposeApplyStepID string
	ActivateStepID     string
}

func (resolver *TaskPlanResolver) PrepareRouteRemovalTask(
	ctx context.Context,
	task etcd.TaskRecord,
	intent etcd.RouteRemovalIntent,
	procedure RouteRemovalTaskProcedureIDs,
) (etcd.RouteRemovalTaskPreparation, error) {
	if intent.CandidateProjection == nil {
		return etcd.RouteRemovalTaskPreparation{Intent: intent, Task: task}, nil
	}
	pin, err := resolver.pinRouteProvider(
		ctx,
		intent.EnvironmentID,
		intent.CurrentProjectionRevision,
		*intent.CandidateProjection,
		nil,
		intent.RouteID,
		intent.CandidateProjection.RenderGeneration,
	)
	if err != nil {
		return etcd.RouteRemovalTaskPreparation{}, err
	}
	if pin == nil {
		return etcd.RouteRemovalTaskPreparation{Intent: intent, Task: task}, nil
	}
	intent.Provider = pin
	if ids.Validate(ids.KindConfig, procedure.ArtifactID) != nil ||
		ids.Validate(ids.KindConfig, procedure.MaterializationID) != nil ||
		ids.Validate(ids.KindStep, procedure.MaterializeStepID) != nil ||
		ids.Validate(ids.KindStep, procedure.ComposeApplyStepID) != nil ||
		ids.Validate(ids.KindStep, procedure.ActivateStepID) != nil ||
		intent.CandidateProjection.RenderGeneration > math.MaxInt32 {
		return etcd.RouteRemovalTaskPreparation{}, errs.New(
			errs.KindValidationFailed,
			"Route removal procedure ids are invalid",
		)
	}
	content, err := resolver.routeProviderFile(*pin, *intent.CandidateProjection)
	if err != nil {
		return etcd.RouteRemovalTaskPreparation{}, err
	}
	digest := sha256.Sum256(content)
	length := len(content)
	reference := etcd.TaskComponentFileValueReference{
		RevisionID:  intent.CandidateProjection.RevisionID,
		ComponentID: pin.ComponentID,
		Path:        pin.Destination,
		RouteTaskID: task.ID,
	}
	task.Executor = etcd.TaskExecutorAgent
	task.Params = map[string]string{
		etcd.TaskRouteEnvironmentParam:           intent.EnvironmentID,
		etcd.TaskMaterializationEnvironmentParam: intent.EnvironmentID,
		etcd.EnvironmentDesiredRevisionParam:     intent.CandidateProjection.RevisionID,
		EnvironmentBlueprintArtifactParam:        procedure.ArtifactID,
	}
	task.RenderGeneration = int32(intent.CandidateProjection.RenderGeneration)
	task.Steps = []etcd.TaskStepRecord{
		{Kind: etcd.TaskStepOperation, ID: procedure.MaterializeStepID},
		{Kind: etcd.TaskStepOperation, ID: procedure.ComposeApplyStepID},
		{Kind: etcd.TaskStepOperation, ID: procedure.ActivateStepID},
	}
	materialization := etcd.TaskMaterializationRecord{
		StepID:            procedure.MaterializeStepID,
		MaterializationID: procedure.MaterializationID,
		EnvironmentID:     intent.EnvironmentID,
		Destination:       pin.Destination,
		OutputKind:        etcd.TaskMaterializationOutputPlainFile,
		Mode:              uint32(entrymaterialization.ModeReadOnly),
		Length:            uint64(length),
		SHA256:            hex.EncodeToString(digest[:]),
		Source: etcd.TaskMaterializationSource{
			Kind:          etcd.TaskMaterializationSourceComponentFile,
			ComponentFile: &reference,
		},
	}
	err = resolver.retainComponentMaterialization(
		ctx, materialization, intent.CandidateProjection.RenderGeneration, content,
	)
	clear(content)
	if err != nil {
		return etcd.RouteRemovalTaskPreparation{}, err
	}
	task.Materializations = []etcd.TaskMaterializationRecord{materialization}
	plan, err := resolver.buildRouteRemovalPlan(ctx, task, intent)
	if err != nil {
		return etcd.RouteRemovalTaskPreparation{}, err
	}
	task.PlanHash = hex.EncodeToString(plan.PlanHash)
	return etcd.RouteRemovalTaskPreparation{Intent: intent, Task: task}, nil
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
		task.Type != etcd.TaskRemove ||
		ids.Validate(ids.KindRoute, task.Target) != nil ||
		task.ID != intent.TaskID ||
		task.Target != intent.RouteID ||
		!task.CreatedAt.Equal(intent.CreatedAt) ||
		intent.Status != etcd.TaskStatusPending ||
		intent.Provider == nil ||
		intent.CandidateProjection == nil ||
		intent.CurrentProjection == nil ||
		task.TimeoutSeconds <= 0 ||
		task.TimeoutSeconds > math.MaxUint32 ||
		len(task.Params) != 4 ||
		len(task.Steps) != 3 ||
		len(task.Materializations) != 1 {
		return nil, errs.New(errs.KindInternal, "durable Route removal Task shape is invalid")
	}
	pin := *intent.Provider
	candidate := *intent.CandidateProjection
	revisionID := task.Params[etcd.EnvironmentDesiredRevisionParam]
	artifactID := task.Params[EnvironmentBlueprintArtifactParam]
	if task.Params[etcd.TaskRouteEnvironmentParam] != intent.EnvironmentID ||
		task.Params[etcd.TaskMaterializationEnvironmentParam] != intent.EnvironmentID ||
		revisionID != candidate.RevisionID ||
		ids.Validate(ids.KindTask, revisionID) != nil ||
		ids.Validate(ids.KindConfig, artifactID) != nil ||
		uint64(task.RenderGeneration) != candidate.RenderGeneration ||
		ids.Validate(ids.KindStep, task.Steps[0].ID) != nil ||
		ids.Validate(ids.KindStep, task.Steps[1].ID) != nil ||
		ids.Validate(ids.KindStep, task.Steps[2].ID) != nil {
		return nil, errs.New(errs.KindInternal, "durable Route removal Task parameters are invalid")
	}
	reference := task.Materializations[0]
	componentFile := reference.Source.ComponentFile
	if reference.StepID != task.Steps[0].ID || reference.EnvironmentID != intent.EnvironmentID ||
		reference.Destination != pin.Destination ||
		reference.ServiceID != "" ||
		reference.ServiceName != "" ||
		reference.OutputKind != etcd.TaskMaterializationOutputPlainFile ||
		reference.UID != 0 ||
		reference.GID != 0 ||
		reference.Mode != uint32(entrymaterialization.ModeReadOnly) ||
		reference.Source.Kind != etcd.TaskMaterializationSourceComponentFile ||
		componentFile == nil ||
		componentFile.RevisionID != revisionID ||
		componentFile.ComponentID != pin.ComponentID ||
		componentFile.Path != pin.Destination ||
		componentFile.RouteTaskID != task.ID {
		return nil, errs.New(errs.KindInternal, "durable Route removal materialization is invalid")
	}
	content, err := resolver.loadComponentMaterialization(ctx, reference, uint64(task.RenderGeneration))
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
	if environment.Record.ID != intent.EnvironmentID ||
		environment.Record.ProvisioningState != hierarchyrecord.EnvironmentProvisioningReady ||
		project.Record.ID != environment.Record.ProjectID ||
		project.Record.Kind != hierarchyrecord.ProjectKindTenant ||
		tenant.Record.ID != project.Record.TenantID {
		return nil, errs.New(errs.KindInternal, "Route removal Environment hierarchy is invalid")
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
		nil,
	)
	if err != nil {
		return nil, err
	}
	materializationStep, err := taskmaterialization.BuildTaskMaterializationStep(reference, artifactID, uint32(task.TimeoutSeconds))
	if err != nil {
		return nil, err
	}
	definitionDigest, _ := hex.DecodeString(pin.DefinitionDigest)
	catalogDigest, _ := hex.DecodeString(pin.CatalogDigest)
	var definitionPin, catalogPin [sha256.Size]byte
	copy(definitionPin[:], definitionDigest)
	copy(catalogPin[:], catalogDigest)
	action, err := componentrender.BuildPinnedEnvironmentComponentAction(
		resolver.componentCatalog,
		pin.ComponentID,
		definitionPin,
		catalogPin,
		componentsdk.ActionID(pin.ActionID),
		reference.MaterializationID,
		digest,
		uint64(task.RenderGeneration),
	)
	if err != nil {
		return nil, err
	}
	return taskplan.Build(
		taskplan.BuildInput{
			VolumeRoot:       resolver.volumeRoot,
			PlanID:           task.PlanID,
			RenderGeneration: uint64(task.RenderGeneration),
			Operation:        agentpb.PlanOperation_PLAN_OPERATION_REMOVE,
			TargetID:         task.Target,
			Artifacts:        []*agentpb.ComposeArtifact{artifact},
			Steps: []*agentpb.ExecutionStep{
				materializationStep,
				{
					StepId:             task.Steps[1].ID,
					TimeoutSeconds:     uint32(task.TimeoutSeconds),
					PrerequisiteStepId: materializationStep.GetStepId(),
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
		},
	)
}

func (resolver *TaskPlanResolver) routeProviderFile(
	pin etcd.RouteProviderPin,
	projection etcd.EnvironmentComposeProjection,
) ([]byte, error) {
	plan, _, _, _, err := resolver.renderPinnedRouteProvider(pin, projection)
	if err != nil {
		return nil, err
	}
	for _, file := range plan.Files {
		if file.Path == pin.Destination {
			return append([]byte(nil), file.Content...), nil
		}
	}
	return nil, errs.New(errs.KindStateConflict, "pinned Route provider did not emit its managed configuration")
}
