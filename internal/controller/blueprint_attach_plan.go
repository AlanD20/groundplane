package controller

import (
	"context"
	"reflect"

	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type blueprintAttachPlanCandidate struct {
	current    etcd.Versioned[etcd.AttachRecord]
	adapterKey string
	stepCount  int
}

func (resolver *TaskPlanResolver) blueprintAttachPlanCandidates(
	ctx context.Context,
	task etcd.TaskRecord,
) ([]blueprintAttachPlanCandidate, int, error) {
	if resolver.attaches == nil {
		return nil, 0, nil
	}
	versionedIntent, found, err := resolver.attaches.GetBlueprintAttachTaskIntent(ctx, task.ID)
	if err != nil || !found {
		return nil, 0, err
	}
	intent := versionedIntent.Record
	if intent.TaskID != task.ID || intent.EnvironmentID != task.Target || intent.Status != etcd.TaskStatusPending {
		return nil, 0, errs.New(errs.KindStateConflict, "Blueprint Attach intent does not own the active Task")
	}
	if resolver.services == nil || resolver.attachIdentities == nil {
		return nil, 0, errs.New(errs.KindInternal, "Blueprint Attach plan dependencies are unavailable")
	}
	candidates := make([]blueprintAttachPlanCandidate, 0, len(intent.Candidates))
	totalSteps := 0
	for _, staged := range intent.Candidates {
		current, getErr := resolver.attaches.GetAttach(ctx, staged.ID)
		if getErr != nil {
			return nil, 0, getErr
		}
		expected := staged
		expected.Status = current.Record.Status
		if !reflect.DeepEqual(expected, current.Record) ||
			(current.Record.Status != core.AttachPending && current.Record.Status != core.AttachProvisioning) {
			return nil, 0, errs.New(errs.KindStateConflict, "Blueprint Attach candidate changed after publication")
		}
		candidate := blueprintAttachPlanCandidate{current: current}
		if current.Record.OwnsCredential() {
			backing, serviceErr := resolver.services.GetService(ctx, current.Record.BackingServiceID)
			if serviceErr != nil {
				return nil, 0, serviceErr
			}
			if backing.Record.EnvironmentID != current.Record.BackingEnvironmentID ||
				backing.Record.BackingNetworkID != current.Record.BackingNetworkID ||
				backing.Record.Runtime.RuntimeIntent != core.ServiceRuntimeIntentRunning {
				return nil, 0, errs.New(errs.KindStateConflict, "Blueprint Attach backing Service is not runnable")
			}
			adapter, registered := adapters.Get(backing.Record.Desired.Adapter)
			if !registered {
				return nil, 0, errs.New(errs.KindValidationFailed, "Blueprint Attach adapter is not registered")
			}
			candidate.adapterKey = adapter.Key()
			if !adapter.Manual() {
				candidate.stepCount = len(current.Record.GrantAttachIDs) + 1
				totalSteps += candidate.stepCount
			}
		}
		candidates = append(candidates, candidate)
	}
	return candidates, totalSteps, nil
}

func (resolver *TaskPlanResolver) blueprintAttachProcedureSteps(
	ctx context.Context,
	task etcd.TaskRecord,
	candidates []blueprintAttachPlanCandidate,
	stepIndex int,
) ([]*agentpb.ExecutionStep, int, error) {
	steps := make([]*agentpb.ExecutionStep, 0)
	for _, candidate := range candidates {
		if candidate.stepCount == 0 {
			continue
		}
		end := stepIndex + candidate.stepCount
		if end > len(task.Steps) {
			clearAdapterProcedurePasswords(steps)
			return nil, stepIndex, errs.New(errs.KindInternal, "Blueprint Attach procedure steps are incomplete")
		}
		procedureTask := task
		procedureTask.Type = etcd.TaskAttach
		procedureTask.Target = candidate.current.Record.ID
		procedureTask.Steps = append([]etcd.TaskStepRecord(nil), task.Steps[stepIndex:end]...)
		var procedures []*agentpb.ExecutionStep
		err := resolver.attachIdentities.ResolveTaskIdentity(
			ctx,
			candidate.current,
			task.ID,
			func(identity AttachPlanIdentity) error {
				var buildErr error
				procedures, buildErr = BuildAttachProvisionSteps(
					procedureTask, candidate.current.Record, candidate.adapterKey, identity,
				)
				return buildErr
			},
		)
		if err != nil {
			clearAdapterProcedurePasswords(steps)
			return nil, stepIndex, err
		}
		steps = append(steps, procedures...)
		stepIndex = end
	}
	return steps, stepIndex, nil
}
