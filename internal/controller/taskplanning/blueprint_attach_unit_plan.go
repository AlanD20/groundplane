package taskplanning

import (
	"context"
	"encoding/hex"
	"math"

	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/taskplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	attachinputs "github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskconfiguration "github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type blueprintAttachInputGenerationReader interface {
	GetBlueprintAttachInputGeneration(
		context.Context,
		string,
		string,
	) (etcdstore.Versioned[attachinputs.Generation], bool, error)
}

type blueprintAttachInputResolver interface {
	ResolveDraftHookInput(
		context.Context,
		etcdstore.Versioned[attachrecord.Record],
		*attachrecord.EncryptedFacts,
		etcd.TaskRecord,
		*taskconfiguration.BackingHookEncryptedInputs,
		backinghook.Context,
		BackingHookInputConsumer,
	) error
}

// PrepareBlueprintAttachUnit seals one hook-only child from the already
// transferred immutable generation. It performs no backing topology reads.
func PrepareBlueprintAttachUnit(
	ctx context.Context,
	volumeRoot string,
	inputs blueprintAttachInputResolver,
	generation attachinputs.Generation,
	task etcd.TaskRecord,
) (etcd.TaskRecord, *agentpb.ExecutionPlan, error) {
	if len(task.Steps) != 0 || task.PlanHash != "" {
		return etcd.TaskRecord{}, nil, errs.New(
			errs.KindValidationFailed, "Blueprint Attach child is already prepared",
		)
	}
	stepID, err := blueprintAttachUnitStepID(task)
	if err != nil {
		return etcd.TaskRecord{}, nil, err
	}
	prepared := task
	prepared.Steps = []taskjournal.TaskStepRecord{{Kind: taskjournal.TaskStepOperation, ID: stepID}}
	plan, err := buildBlueprintAttachUnitPlan(ctx, volumeRoot, inputs, generation, prepared)
	if err != nil {
		return etcd.TaskRecord{}, nil, err
	}
	prepared.PlanHash = hex.EncodeToString(plan.GetPlanHash())
	return prepared, plan, nil
}

func (resolver *TaskPlanResolver) resolveBlueprintAttachUnitPlan(
	ctx context.Context,
	task etcd.TaskRecord,
) (*agentpb.ExecutionPlan, error) {
	reader, ok := resolver.blueprints.(blueprintAttachInputGenerationReader)
	if !ok {
		return nil, errs.New(errs.KindInternal, "Blueprint Attach input authority is not configured")
	}
	inputs, ok := resolver.attachIdentities.(blueprintAttachInputResolver)
	if !ok {
		return nil, errs.New(errs.KindInternal, "Blueprint Attach input resolver is not configured")
	}
	parentID := task.Params[taskjournal.TaskBlueprintParentParam]
	attachID := task.Params[attachinputs.TaskAttachIDParam]
	generation, found, err := reader.GetBlueprintAttachInputGeneration(ctx, parentID, attachID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errs.New(errs.KindStateConflict, "Blueprint Attach input generation is unavailable")
	}
	defer attachinputs.Clear(&generation.Record)
	return buildBlueprintAttachUnitPlan(ctx, resolver.volumeRoot, inputs, generation.Record, task)
}

func buildBlueprintAttachUnitPlan(
	ctx context.Context,
	volumeRoot string,
	inputs blueprintAttachInputResolver,
	generation attachinputs.Generation,
	task etcd.TaskRecord,
) (*agentpb.ExecutionPlan, error) {
	parentID := task.Params[taskjournal.TaskBlueprintParentParam]
	attachID := task.Params[attachinputs.TaskAttachIDParam]
	if ctx == nil || inputs == nil || task.Type != taskjournal.TaskUpdate ||
		task.Actor != taskjournal.TaskActorSystem || task.Executor != taskjournal.TaskExecutorAgent ||
		ids.Validate(ids.KindEnvironment, task.Target) != nil || task.Target != task.Owner.EnvironmentID ||
		ids.Validate(ids.KindTask, parentID) != nil || parentID == task.ID ||
		task.Params[blueprints.EnvironmentDesiredRevisionParam] != parentID ||
		ids.Validate(ids.KindAttach, attachID) != nil || len(task.Params) != 3 ||
		task.RenderGeneration <= 0 || task.TimeoutSeconds <= 0 || task.TimeoutSeconds > math.MaxUint32 ||
		len(task.Steps) != 1 || task.Steps[0].Kind != taskjournal.TaskStepOperation ||
		len(task.Materializations) != 0 || task.EntryRuntime != nil ||
		len(task.ComponentActionStepIDs) != 0 || len(task.ManagedComponentTeardownSources) != 0 ||
		generation.RevisionID != parentID || generation.ParentTaskID != parentID ||
		generation.EnvironmentID != task.Owner.EnvironmentID || generation.AttachID != attachID ||
		generation.OperationID != task.OperationID || generation.OwnerKind != attachinputs.OwnerChild ||
		generation.OwnerTaskID != task.ID || generation.Transfer == nil ||
		generation.Transfer.ParentTaskID != parentID || generation.Transfer.ChildTaskID != task.ID {
		return nil, errs.New(errs.KindInternal, "durable Blueprint Attach child shape is invalid")
	}
	expectedStepID, err := blueprintAttachUnitStepID(task)
	if err != nil || task.Steps[0].ID != expectedStepID || !blueprintAttachTaskConfigurationMatches(task, generation) {
		return nil, errs.New(errs.KindInternal, "durable Blueprint Attach child authority changed")
	}
	current, err := attachrecord.NewPendingAttachRecord(
		generation.AttachID,
		generation.EnvironmentID,
		generation.AttachName,
		generation.BackingProjectID,
		generation.BackingEnvironmentID,
		generation.BackingServiceID,
		generation.BackingNetworkID,
		generation.ConsumerServiceID,
		generation.CredentialOwnerID,
		nil,
		generation.FactSets,
		task.ID,
		task.CreatedAt,
	)
	if err != nil {
		return nil, err
	}
	current.HookBundle = true
	hookContext := backinghook.Context{
		Event: backinghook.Attach, BackingServiceID: generation.BackingServiceID,
		AttachID: generation.AttachID, TenantID: task.Owner.TenantID,
		ProjectID: task.Owner.ProjectID, EnvironmentID: generation.EnvironmentID,
		ServiceID: generation.ConsumerServiceID,
	}
	var plan *agentpb.ExecutionPlan
	err = inputs.ResolveDraftHookInput(
		ctx,
		etcdstore.Versioned[attachrecord.Record]{Record: current, Revision: 1, ReadRevision: 1},
		&generation.GeneratedInputs,
		task,
		generation.ResolvedInputs,
		hookContext,
		func(input backinghook.Input) error {
			step, buildErr := backingHookStep(task.Steps[0], *generation.Hook.Attach, input, generation.Hook.Facts)
			if buildErr != nil {
				return buildErr
			}
			defer taskplan.ClearBackingHookProcedure(step.GetBackingHookProcedure())
			plan, buildErr = taskplan.Build(taskplan.BuildInput{
				VolumeRoot: volumeRoot, PlanID: task.PlanID,
				RenderGeneration: uint64(task.RenderGeneration),
				Operation:        agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY,
				TargetID:         generation.EnvironmentID, Steps: []*agentpb.ExecutionStep{step},
			})
			return buildErr
		},
	)
	return plan, err
}

func blueprintAttachTaskConfigurationMatches(task etcd.TaskRecord, generation attachinputs.Generation) bool {
	if generation.ResolvedInputs == nil {
		return task.Configuration == nil && generation.SecretPins == nil
	}
	if task.Configuration == nil || task.Configuration.BackingHookInputs == nil ||
		task.Configuration.Current != (taskconfiguration.TaskConfiguration{}).Current ||
		task.Configuration.Prior != nil || task.Configuration.PriorRevision != 0 {
		return false
	}
	expected := &taskconfiguration.TaskBackingHookInputSet{
		ProjectID:        generation.BackingProjectID,
		CiphertextSHA256: generation.ResolvedInputs.CiphertextSHA256,
		SecretSources:    generation.SecretSources,
	}
	if !taskconfiguration.SameTaskBackingHookInputSet(task.Configuration.BackingHookInputs, expected) {
		return false
	}
	if generation.SecretPins == nil {
		return task.Configuration.SecretPins == nil
	}
	return task.Configuration.SecretPins != nil && *task.Configuration.SecretPins == *generation.SecretPins
}

func blueprintAttachUnitStepID(task etcd.TaskRecord) (string, error) {
	planTime, err := ids.Timestamp(ids.KindPlan, task.PlanID)
	if err != nil {
		return "", err
	}
	return ids.DeriveAt(
		ids.KindStep,
		planTime,
		task.PlanID,
		"blueprint-attach-unit:"+task.Params[attachinputs.TaskAttachIDParam],
	), nil
}
