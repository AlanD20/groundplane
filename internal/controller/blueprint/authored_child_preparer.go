package blueprint

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/blueprintcoordinator"
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
)

func (service *Service) prepareAuthoredAttachChild(
	ctx context.Context,
	parent etcd.TaskRecord,
	unit blueprintunits.Unit,
	timeoutSeconds int64,
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
		TargetName:     "Attach " + versioned.Record.AttachName,
		TimeoutSeconds: timeoutSeconds,
		Status:         taskjournal.TaskStatusPending, NextEventSequence: 1,
		CreatedAt: createdAt, UpdatedAt: createdAt,
	}
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
	var prepared etcd.TaskRecord
	var plan *agentpb.ExecutionPlan
	if transferred.Provision != nil {
		prepared, plan, err = taskplanning.PrepareBlueprintAttachProvisionUnit(
			ctx, service.volumeRoot, service.attachFacts, transferred, child,
		)
	} else {
		prepared, plan, err = taskplanning.PrepareBlueprintAttachUnit(
			ctx, service.volumeRoot, service.attachFacts, transferred, child,
		)
	}
	if plan != nil {
		for _, step := range plan.GetSteps() {
			taskplan.ClearBackingHookProcedure(step.GetBackingHookProcedure())
			clear(step.GetAdapterProcedure().GetPassword())
		}
	}
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	return blueprintcoordinator.PreparedChild{Task: prepared}, nil
}

func (service *Service) prepareAuthoredNetworkChild(
	ctx context.Context,
	parent etcd.TaskRecord,
	unit blueprintunits.Unit,
	timeoutSeconds int64,
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
		TimeoutSeconds: timeoutSeconds,
		Status:         taskjournal.TaskStatusPending, NextEventSequence: 1,
		CreatedAt: createdAt, UpdatedAt: createdAt,
	}
	prepared, _, err := service.plans.PrepareBlueprintNetworkUnit(ctx, child)
	if err != nil {
		return blueprintcoordinator.PreparedChild{}, err
	}
	return blueprintcoordinator.PreparedChild{Task: prepared}, nil
}
