package taskplanning

import (
	"context"
	"encoding/hex"
	"math"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/taskplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	attachinputs "github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type blueprintAttachProvisionIdentityResolver interface {
	ResolveBlueprintProvisionIdentity(context.Context, attachinputs.Generation, AttachPlanIdentityConsumer) error
}

func PrepareBlueprintAttachProvisionUnit(
	ctx context.Context, volumeRoot string, inputs blueprintAttachProvisionIdentityResolver,
	generation attachinputs.Generation, task etcd.TaskRecord,
) (etcd.TaskRecord, *agentpb.ExecutionPlan, error) {
	if len(task.Steps) != 0 || task.PlanHash != "" || generation.Provision == nil {
		return etcd.TaskRecord{}, nil, errs.New(
			errs.KindValidationFailed,
			"Attach provisioning child is already prepared",
		)
	}
	prepared := task
	prepared.Steps = make([]taskjournal.TaskStepRecord, 1+len(generation.Provision.Candidate.GrantAttachIDs))
	for index := range prepared.Steps {
		prepared.Steps[index] = taskjournal.TaskStepRecord{
			Kind: taskjournal.TaskStepOperation,
			ID:   ids.New(ids.KindStep),
		}
	}
	plan, err := buildBlueprintAttachProvisionPlan(ctx, volumeRoot, inputs, generation, prepared)
	if err != nil {
		return etcd.TaskRecord{}, nil, err
	}
	prepared.PlanHash = hex.EncodeToString(plan.GetPlanHash())
	prepared.Steps = taskjournal.CaptureStepDescriptions(prepared.Steps, plan)
	return prepared, plan, nil
}

func buildBlueprintAttachProvisionPlan(
	ctx context.Context, volumeRoot string, inputs blueprintAttachProvisionIdentityResolver,
	generation attachinputs.Generation, task etcd.TaskRecord,
) (*agentpb.ExecutionPlan, error) {
	if ctx == nil || inputs == nil || attachinputs.Validate(generation) != nil || generation.Provision == nil ||
		!generation.Provision.RequiresProvisioning || task.Actor != taskjournal.TaskActorSystem ||
		task.Params[taskjournal.TaskBlueprintParentParam] != generation.ParentTaskID ||
		task.Params[blueprints.EnvironmentDesiredRevisionParam] != generation.ParentTaskID ||
		task.Params[attachinputs.TaskAttachIDParam] != generation.AttachID || len(task.Params) != 3 ||
		task.RenderGeneration <= 0 || len(task.Materializations) != 0 || task.EntryRuntime != nil ||
		len(task.ComponentActionStepIDs) != 0 || len(task.ManagedComponentTeardownSources) != 0 ||
		generation.OwnerKind != attachinputs.OwnerChild || generation.OwnerTaskID != task.ID ||
		generation.OperationID != task.OperationID || task.Type != taskjournal.TaskUpdate ||
		task.Executor != taskjournal.TaskExecutorAgent || task.Target != generation.EnvironmentID ||
		task.Owner.EnvironmentID != generation.EnvironmentID || task.TimeoutSeconds <= 0 || task.TimeoutSeconds > math.MaxUint32 ||
		generation.CredentialOwnerID != generation.AttachID ||
		len(task.Steps) != 1+len(generation.Provision.Candidate.GrantAttachIDs) || task.Configuration != nil {
		return nil, errs.New(errs.KindStateConflict, "Blueprint Attach provisioning child authority changed")
	}
	record, err := generation.PendingRecord(task.ID, task.CreatedAt)
	if err != nil {
		return nil, err
	}
	var plan *agentpb.ExecutionPlan
	err = inputs.ResolveBlueprintProvisionIdentity(ctx, generation, func(identity AttachPlanIdentity) error {
		procedureTask := task
		procedureTask.Type, procedureTask.Target = taskjournal.TaskAttach, record.ID
		steps, err := BuildAttachProvisionSteps(procedureTask, record, generation.AdapterKey, identity)
		if err != nil {
			return err
		}
		defer func() {
			for _, step := range steps {
				clear(step.GetAdapterProcedure().GetPassword())
			}
		}()
		plan, err = taskplan.Build(taskplan.BuildInput{
			VolumeRoot: volumeRoot, PlanID: task.PlanID, RenderGeneration: uint64(task.RenderGeneration),
			Operation: agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY,
			TargetID:  task.Owner.EnvironmentID, Steps: steps,
		})
		return err
	})
	return plan, err
}
