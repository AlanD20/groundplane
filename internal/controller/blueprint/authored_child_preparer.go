package blueprint

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/blueprintcoordinator"
	"github.com/AlanD20/groundplane/internal/controller/blueprintrelease"
	"github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	"github.com/AlanD20/groundplane/internal/controller/taskcontract"
	"github.com/AlanD20/groundplane/internal/controller/taskplan"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	attachinputs "github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	taskconfiguration "github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpinrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Prepare constructs one private Service child from the parent's exact sealed
// runtime projection. Publication remains the coordinator's responsibility;
// it rechecks the desired unit and execution epoch atomically.
func (service *Service) Prepare(
	ctx context.Context,
	parent etcd.TaskRecord,
	unit blueprintunits.Unit,
) (blueprintcoordinator.PreparedChild, error) {
	if service == nil || ctx == nil || parent.Executor != taskjournal.TaskExecutorBlueprint ||
		parent.Status != taskjournal.TaskStatusRunning || unit.Removal {
		return blueprintcoordinator.PreparedChild{}, errs.New(
			errs.KindValidationFailed, "Blueprint child request is invalid",
		)
	}
	if unit.Target.Kind == ids.KindNetwork {
		return service.prepareAuthoredNetworkChild(ctx, parent, unit)
	}
	if unit.Target.Kind == ids.KindVolume {
		return service.prepareAuthoredVolumeChild(ctx, parent, unit)
	}
	if unit.Target.Kind == ids.KindAttach {
		return service.prepareAuthoredAttachChild(ctx, parent, unit)
	}
	if unit.Target.Kind != ids.KindService {
		return blueprintcoordinator.PreparedChild{}, errs.New(
			errs.KindStateConflict, "Blueprint unit has no child preparer",
		)
	}
	input, err := service.loadAuthoredParentInput(ctx, parent)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	// The runtime projection is sealed only after Entry and Attach unit receipts
	// match this parent. Other unit effects are still unsupported here.
	if len(input.desired.Input.Components) != 0 || len(input.desired.Input.Scripts) != 0 ||
		len(input.desired.Input.Requires) != 0 {
		return blueprintcoordinator.PreparedChild{}, errs.New(
			errs.KindStateConflict, "Blueprint Service child prerequisites are not prepared",
		)
	}
	projection, found, err := service.repository.GetEnvironmentComposeProjectionRevision(
		ctx, parent.Owner.EnvironmentID, parent.ID,
	)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	if !found || projection.Record.EnvironmentID != parent.Owner.EnvironmentID ||
		projection.Record.RevisionID != parent.ID ||
		projection.Record.RenderGeneration != input.desired.RenderGeneration {
		return blueprintcoordinator.PreparedChild{}, errs.New(
			errs.KindStateConflict, "Blueprint Service child runtime projection is unavailable",
		)
	}
	artifact := &agentpb.ComposeArtifact{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(
		projection.Record.ComposeArtifact, artifact,
	); err != nil || ids.Validate(ids.KindConfig, artifact.GetArtifactId()) != nil {
		return blueprintcoordinator.PreparedChild{}, errs.New(
			errs.KindStateConflict, "Blueprint Service child artifact is invalid",
		)
	}
	createdAt := service.now().UTC()
	child := etcd.TaskRecord{
		ID: ids.New(ids.KindTask), OperationID: ids.New(ids.KindOperation),
		Owner: parent.Owner, Actor: taskjournal.TaskActorSystem,
		Executor: taskjournal.TaskExecutorAgent, PlanID: ids.New(ids.KindPlan),
		RenderGeneration: parent.RenderGeneration, Type: taskjournal.TaskUpdate,
		Target: parent.Owner.EnvironmentID,
		Params: map[string]string{
			blueprints.EnvironmentDesiredRevisionParam:      parent.ID,
			taskjournal.TaskBlueprintParentParam:            parent.ID,
			taskjournal.TaskMaterializationEnvironmentParam: parent.Owner.EnvironmentID,
			taskcontract.EnvironmentBlueprintArtifactParam:  artifact.GetArtifactId(),
			taskcontract.EnvironmentBlueprintProcedureParam: string(taskcontract.BlueprintComposeProcedureNone),
		},
		TimeoutSeconds: desiredrevision.TaskTimeoutSeconds,
		Status:         taskjournal.TaskStatusPending, NextEventSequence: 1,
		CreatedAt: createdAt, UpdatedAt: createdAt,
	}
	prepared, err := service.blueprintReleases.PrepareServiceUnit(
		ctx,
		blueprintrelease.PrepareServiceUnitInput{
			ServiceID: unit.Target.ID, Projection: projection.Record,
			Task: child, CreatedAt: createdAt,
			AllocateNamed: func(kind ids.Kind, _ string) string { return ids.New(kind) },
		},
	)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	return blueprintcoordinator.PreparedChild{
		Task: prepared.Task, ReleasePublication: prepared.Publication,
	}, nil
}

func (service *Service) prepareAuthoredAttachChild(
	ctx context.Context,
	parent etcd.TaskRecord,
	unit blueprintunits.Unit,
) (blueprintcoordinator.PreparedChild, error) {
	reader, ok := service.repository.(authoredAttachGenerationReader)
	if !ok || service.attachFacts == nil || ids.Validate(ids.KindAttach, unit.Target.ID) != nil {
		return blueprintcoordinator.PreparedChild{}, errs.New(
			errs.KindInternal, "Blueprint Attach child authority is not configured",
		)
	}
	versioned, found, err := reader.GetBlueprintAttachInputGeneration(ctx, parent.ID, unit.Target.ID)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	if !found {
		return blueprintcoordinator.PreparedChild{}, errs.New(
			errs.KindStateConflict, "Blueprint Attach input generation is unavailable",
		)
	}
	defer attachinputs.Clear(&versioned.Record)
	createdAt := service.now().UTC()
	child := etcd.TaskRecord{
		ID: ids.New(ids.KindTask), OperationID: versioned.Record.OperationID,
		Owner: parent.Owner, Actor: taskjournal.TaskActorSystem,
		Executor: taskjournal.TaskExecutorAgent, PlanID: ids.New(ids.KindPlan),
		RenderGeneration: parent.RenderGeneration, Type: taskjournal.TaskUpdate,
		Target: parent.Owner.EnvironmentID,
		Params: map[string]string{
			blueprints.EnvironmentDesiredRevisionParam: parent.ID,
			taskjournal.TaskBlueprintParentParam:       parent.ID,
			attachinputs.TaskAttachIDParam:             unit.Target.ID,
		},
		TimeoutSeconds: desiredrevision.TaskTimeoutSeconds,
		Status:         taskjournal.TaskStatusPending, NextEventSequence: 1,
		CreatedAt: createdAt, UpdatedAt: createdAt,
	}
	child.TimeoutSeconds = max(
		child.TimeoutSeconds,
		int64(versioned.Record.Hook.Attach.TimeoutSeconds)+15,
	)
	transferred, err := attachinputs.TransferToChild(versioned.Record, child.ID)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	if transferred.ResolvedInputs != nil {
		child.Configuration = &taskconfiguration.TaskConfiguration{
			BackingHookInputs: &taskconfiguration.TaskBackingHookInputSet{
				ProjectID:        transferred.BackingProjectID,
				CiphertextSHA256: transferred.ResolvedInputs.CiphertextSHA256,
				SecretSources: append(
					[]tasksecretpinrecord.Record(nil), transferred.SecretSources...,
				),
			},
		}
		if transferred.SecretPins != nil {
			pins := *transferred.SecretPins
			child.Configuration.SecretPins = &pins
		}
	}
	prepared, plan, err := taskplanning.PrepareBlueprintAttachUnit(
		ctx, service.volumeRoot, service.attachFacts, transferred, child,
	)
	if plan != nil {
		for _, step := range plan.GetSteps() {
			taskplan.ClearBackingHookProcedure(step.GetBackingHookProcedure())
		}
	}
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	return blueprintcoordinator.PreparedChild{Task: prepared}, nil
}

type authoredParentVolumeIntentReader interface {
	BlueprintParentProtectedIntentSHA256(context.Context, string) (string, error)
	BlueprintVolumeNeedsCreate(context.Context, string, string, blueprintunits.Unit) (bool, error)
}

func (service *Service) prepareAuthoredVolumeChild(
	ctx context.Context,
	parent etcd.TaskRecord,
	unit blueprintunits.Unit,
) (blueprintcoordinator.PreparedChild, error) {
	if service.plans == nil || ids.Validate(ids.KindVolume, unit.Target.ID) != nil {
		return blueprintcoordinator.PreparedChild{}, errs.New(
			errs.KindValidationFailed, "Blueprint Volume child request is invalid",
		)
	}
	authority, ok := service.repository.(authoredParentVolumeIntentReader)
	if !ok {
		return blueprintcoordinator.PreparedChild{}, errs.New(
			errs.KindInternal, "Blueprint Volume child intent authority is not configured",
		)
	}
	intentSHA256, err := authority.BlueprintParentProtectedIntentSHA256(ctx, parent.ID)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	create, err := authority.BlueprintVolumeNeedsCreate(
		ctx, parent.Owner.EnvironmentID, parent.ID, unit,
	)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	mode := taskjournal.TaskBlueprintVolumeModeVerify
	if create {
		mode = taskjournal.TaskBlueprintVolumeModeCreate
	}
	createdAt := service.now().UTC()
	child := etcd.TaskRecord{
		ID: ids.New(ids.KindTask), OperationID: ids.New(ids.KindOperation),
		Owner: parent.Owner, Actor: taskjournal.TaskActorSystem,
		Executor: taskjournal.TaskExecutorAgent, PlanID: ids.New(ids.KindPlan),
		RenderGeneration: parent.RenderGeneration, Type: taskjournal.TaskUpdate,
		Target: unit.Target.ID,
		Params: map[string]string{
			blueprints.EnvironmentDesiredRevisionParam: parent.ID,
			taskjournal.TaskBlueprintParentParam:       parent.ID,
			taskjournal.TaskBlueprintVolumeUnitParam:   unit.Target.ID,
			taskjournal.TaskBlueprintVolumeModeParam:   mode,
			taskjournal.TaskBlueprintVolumeIntentParam: intentSHA256,
		},
		TimeoutSeconds: desiredrevision.TaskTimeoutSeconds,
		Status:         taskjournal.TaskStatusPending, NextEventSequence: 1,
		CreatedAt: createdAt, UpdatedAt: createdAt,
	}
	prepared, _, err := service.plans.PrepareBlueprintVolumeUnit(ctx, child)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	return blueprintcoordinator.PreparedChild{Task: prepared}, nil
}

func (service *Service) prepareAuthoredNetworkChild(
	ctx context.Context,
	parent etcd.TaskRecord,
	unit blueprintunits.Unit,
) (blueprintcoordinator.PreparedChild, error) {
	if service.plans == nil || ids.Validate(ids.KindNetwork, unit.Target.ID) != nil {
		return blueprintcoordinator.PreparedChild{}, errs.New(
			errs.KindValidationFailed, "Blueprint Network child request is invalid",
		)
	}
	createdAt := service.now().UTC()
	child := etcd.TaskRecord{
		ID: ids.New(ids.KindTask), OperationID: ids.New(ids.KindOperation),
		Owner: parent.Owner, Actor: taskjournal.TaskActorSystem,
		Executor: taskjournal.TaskExecutorAgent, PlanID: ids.New(ids.KindPlan),
		RenderGeneration: parent.RenderGeneration, Type: taskjournal.TaskUpdate,
		Target: unit.Target.ID,
		Params: map[string]string{
			blueprints.EnvironmentDesiredRevisionParam: parent.ID,
			taskjournal.TaskBlueprintParentParam:       parent.ID,
			taskjournal.TaskBlueprintNetworkUnitParam:  unit.Target.ID,
		},
		TimeoutSeconds: desiredrevision.TaskTimeoutSeconds,
		Status:         taskjournal.TaskStatusPending, NextEventSequence: 1,
		CreatedAt: createdAt, UpdatedAt: createdAt,
	}
	prepared, _, err := service.plans.PrepareBlueprintNetworkUnit(ctx, child)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	return blueprintcoordinator.PreparedChild{Task: prepared}, nil
}

var _ blueprintcoordinator.ChildPreparer = (*Service)(nil)
