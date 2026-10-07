// Package hierarchyattachplan seals backing deprovisioning from the hierarchy
// journal's captured inputs. Record finalization remains with that journal.
package hierarchyattachplan

import (
	"context"
	"encoding/hex"
	"math"
	"strconv"

	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/attachments"
	"github.com/AlanD20/groundplane/internal/controller/taskplan"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	hierarchyattach "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletionattach"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type Planner struct {
	volumeRoot string
	journal    *etcd.HierarchyDeletionRepository
	facts      *attachments.FactService
}

func New(
	volumeRoot string,
	journal *etcd.HierarchyDeletionRepository,
	facts *attachments.FactService,
) (*Planner, error) {
	if volumeRoot == "" || journal == nil || facts == nil {
		return nil, errs.New(errs.KindInternal, "hierarchy Attach planner dependencies are required")
	}
	return &Planner{volumeRoot: volumeRoot, journal: journal, facts: facts}, nil
}

func (planner *Planner) BuildHierarchyDeletionAttachPlan(
	ctx context.Context,
	draft etcd.TaskRecord,
	input hierarchyattach.Input,
) (etcd.HierarchyDeletionAttachPlan, error) {
	if len(draft.Steps) != 0 || draft.PlanHash != "" {
		return etcd.HierarchyDeletionAttachPlan{}, errs.New(
			errs.KindInternal,
			"hierarchy Attach child is already sealed",
		)
	}
	prepared, err := planner.prepareTask(draft, input)
	if err != nil {
		return etcd.HierarchyDeletionAttachPlan{}, err
	}
	inputs := input.HookInputs
	if inputs != nil {
		owned := *inputs
		owned.Ciphertext = append([]byte(nil), inputs.Ciphertext...)
		inputs = &owned
	}
	if input.Backing.Desired.Hooks != nil && inputs == nil {
		inputs, err = planner.facts.SealBackingHookTaskInputs(
			ctx, draft.OperationID, input.Attach.BackingProjectID, *input.Backing.Desired.Hooks,
		)
		if err != nil {
			return etcd.HierarchyDeletionAttachPlan{}, err
		}
	}
	if inputs != nil {
		prepared, err = etcd.BindBackingHookTaskInputs(prepared, input.Attach.BackingProjectID, *inputs)
		if err != nil {
			clear(inputs.Ciphertext)
			return etcd.HierarchyDeletionAttachPlan{}, err
		}
	}
	plan, err := planner.build(ctx, prepared, input, inputs)
	if err != nil {
		if inputs != nil {
			clear(inputs.Ciphertext)
		}
		return etcd.HierarchyDeletionAttachPlan{}, err
	}
	defer clearPlanSecrets(plan)
	return etcd.HierarchyDeletionAttachPlan{
		PlanHash: hex.EncodeToString(plan.GetPlanHash()), Steps: taskjournal.CaptureStepDescriptions(prepared.Steps, plan.Steps),
		Configuration: prepared.Configuration, HookInputs: inputs,
	}, nil
}

func (planner *Planner) ResolveExecutionPlan(
	ctx context.Context,
	task etcd.TaskRecord,
) (*agentpb.ExecutionPlan, error) {
	input, err := planner.journal.ReadHierarchyDeletionAttachInput(ctx, task.PlanID)
	if err != nil {
		return nil, err
	}
	defer hierarchyattach.Clear(&input)
	prepared, err := planner.prepareTask(task, input)
	if err != nil {
		return nil, err
	}
	if len(prepared.Steps) != len(task.Steps) {
		return nil, errs.New(errs.KindStateConflict, "hierarchy Attach steps changed")
	}
	for index, step := range prepared.Steps {
		if step.ID != task.Steps[index].ID || step.Kind != task.Steps[index].Kind {
			return nil, errs.New(errs.KindStateConflict, "hierarchy Attach steps changed")
		}
	}
	if input.HookInputs == nil && input.Backing.Desired.Hooks != nil {
		for _, value := range input.Backing.Desired.Hooks.Inputs {
			if value.Generate == "" {
				return nil, errs.New(errs.KindStateConflict, "hierarchy Attach hook inputs were not captured")
			}
		}
	}
	return planner.build(ctx, task, input, input.HookInputs)
}

func (planner *Planner) prepareTask(task etcd.TaskRecord, input hierarchyattach.Input) (etcd.TaskRecord, error) {
	ordinal, ordinalErr := strconv.ParseInt(task.Params[taskjournal.TaskHierarchyDeletionOrdinalParam], 10, 64)
	if hierarchyattach.Validate(input) != nil || task.Type != taskjournal.TaskRemove ||
		task.Executor != taskjournal.TaskExecutorAgent || task.Actor != taskjournal.TaskActorSystem ||
		task.Params[taskjournal.TaskResourceKindParam] != taskjournal.TaskResourceHierarchyDeletion ||
		task.Params[taskjournal.TaskHierarchyDeletionProcedureParam] != "attach.deprovision" ||
		task.Params[taskjournal.TaskHierarchyDeletionParentParam] != input.ParentOperationID ||
		len(task.Params) != 9 || task.Target != input.Attach.ID || !input.Attach.OwnsCredential() ||
		task.RenderGeneration != 1 || task.TimeoutSeconds <= 0 || task.TimeoutSeconds > math.MaxUint32 ||
		ordinalErr != nil || ordinal < 0 ||
		(input.PlanID != "" && (input.PlanID != task.PlanID || input.TaskID != task.ID || input.ActionOrdinal != ordinal)) {
		return etcd.TaskRecord{}, errs.New(errs.KindInternal, "hierarchy Attach child authority is invalid")
	}
	adapter, found := adapters.Get(input.Backing.Desired.Adapter)
	if !found {
		return etcd.TaskRecord{}, errs.New(errs.KindValidationFailed, "captured backing adapter is not registered")
	}
	authentication, err := core.ResolveBackingAuthentication(
		adapter.SupportsAuthenticationModes(),
		input.Backing.Desired.Authentication,
	)
	if err != nil || authentication != input.Backing.Desired.Authentication {
		return etcd.TaskRecord{}, errs.New(errs.KindStateConflict, "captured backing authentication is invalid")
	}
	count := len(input.Attach.GrantAttachIDs) + 1
	if adapter.Custom() {
		if !input.Attach.HookBundle || input.Backing.Desired.Hooks == nil || input.Backing.Desired.Hooks.Detach == nil {
			return etcd.TaskRecord{}, errs.New(errs.KindInternal, "network-only Attach must not publish an Agent child")
		}
		count = 1
	} else if authentication == core.BackingAuthenticationNone {
		return etcd.TaskRecord{}, errs.New(errs.KindInternal, "no-auth Attach must not publish an Agent child")
	}
	planTime, err := ids.Timestamp(ids.KindPlan, task.PlanID)
	if err != nil {
		return etcd.TaskRecord{}, err
	}
	task.Steps = make([]taskjournal.TaskStepRecord, count)
	for index := range task.Steps {
		stepID := ids.DeriveAt(
			ids.KindStep,
			planTime,
			task.PlanID,
			"hierarchy-attach-deprovision/"+strconv.Itoa(index),
		)
		task.Steps[index] = taskjournal.TaskStepRecord{Kind: taskjournal.TaskStepOperation, ID: stepID}
	}
	return task, nil
}

func (planner *Planner) build(
	ctx context.Context, task etcd.TaskRecord, input hierarchyattach.Input,
	hookInputs *taskconfiguration.BackingHookEncryptedInputs,
) (*agentpb.ExecutionPlan, error) {
	var plan *agentpb.ExecutionPlan
	if input.Backing.Desired.Adapter == "custom" {
		draft := input.Attach
		draft.TaskID = task.ID
		draft.Operation = attachrecord.AttachOperationDetach
		draft.Status = core.AttachDetaching
		err := planner.facts.ResolveDraftHookInput(
			ctx,
			etcdstore.Versioned[attachrecord.Record]{
				Record:       draft,
				Revision:     input.AttachRevision,
				ReadRevision: input.SnapshotRevision,
			},
			input.Facts,
			task,
			hookInputs,
			backinghook.Context{Event: backinghook.Detach, BackingServiceID: draft.BackingServiceID,
				AttachID: draft.ID, TenantID: input.TenantID, ProjectID: input.ProjectID,
				EnvironmentID: draft.EnvironmentID, ServiceID: draft.ServiceID},
			func(values backinghook.Input) error {
				step, buildErr := taskplanning.BuildDetachHookStep(
					task.Steps[0],
					*input.Backing.Desired.Hooks.Detach,
					values,
				)
				if buildErr != nil {
					return buildErr
				}
				defer taskplan.ClearBackingHookProcedure(step.GetBackingHookProcedure())
				plan, buildErr = taskplan.Build(taskplan.BuildInput{VolumeRoot: planner.volumeRoot,
					PlanID: task.PlanID, RenderGeneration: 1, Operation: agentpb.PlanOperation_PLAN_OPERATION_DETACH,
					TargetID: task.Target, Steps: []*agentpb.ExecutionStep{step}})
				return buildErr
			},
		)
		return plan, err
	}
	err := planner.facts.ResolveDeletionIdentity(ctx, input, func(identity taskplanning.AttachPlanIdentity) error {
		if identity.Authentication != input.Backing.Desired.Authentication {
			return errs.New(errs.KindStateConflict, "captured Attach authentication changed")
		}
		steps, buildErr := taskplanning.BuildAttachDeprovisionSteps(
			task,
			input.Attach,
			input.Backing.Desired.Adapter,
			identity,
		)
		if buildErr != nil {
			return buildErr
		}
		defer func() {
			for _, step := range steps {
				clear(step.GetAdapterProcedure().Password)
			}
		}()
		plan, buildErr = taskplan.Build(taskplan.BuildInput{VolumeRoot: planner.volumeRoot,
			PlanID: task.PlanID, RenderGeneration: 1, Operation: agentpb.PlanOperation_PLAN_OPERATION_DETACH,
			TargetID: task.Target, Steps: steps})
		return buildErr
	})
	return plan, err
}

func clearPlanSecrets(plan *agentpb.ExecutionPlan) {
	for _, step := range plan.GetSteps() {
		if procedure := step.GetAdapterProcedure(); procedure != nil {
			clear(procedure.Password)
		}
		taskplan.ClearBackingHookProcedure(step.GetBackingHookProcedure())
	}
}
