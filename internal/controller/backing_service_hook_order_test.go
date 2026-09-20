package controller

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type backingHookPlanInputResolver struct{}

func (state *attachPlanTestState) ResolveHookInput(
	_ context.Context,
	_ etcd.Versioned[etcd.AttachRecord],
	_ etcd.TaskRecord,
	hookContext backinghook.Context,
	consume BackingHookInputConsumer,
) error {
	return consume(backinghook.Input{
		Context: hookContext,
		Values:  []backinghook.Value{{Key: "HOST", Value: []byte("svc.internal")}},
	})
}

func (backingHookPlanInputResolver) ResolveTaskIdentity(
	context.Context,
	etcd.Versioned[etcd.AttachRecord],
	string,
	AttachPlanIdentityConsumer,
) error {
	return nil
}

func (backingHookPlanInputResolver) ResolveHookInput(
	context.Context,
	etcd.Versioned[etcd.AttachRecord],
	etcd.TaskRecord,
	backinghook.Context,
	BackingHookInputConsumer,
) error {
	return nil
}

func (backingHookPlanInputResolver) ResolveLifecycleHookInput(
	_ context.Context,
	_ etcd.TaskRecord,
	serviceID string,
	event backinghook.Event,
	consume BackingHookInputConsumer,
) error {
	return consume(backinghook.Input{
		Context: backinghook.Context{Event: event, BackingServiceID: serviceID},
		Values:  []backinghook.Value{{Key: "HOST", Value: []byte("svc.internal")}},
	})
}

func (resolver backingHookPlanInputResolver) ResolveDraftLifecycleHookInput(
	ctx context.Context,
	task etcd.TaskRecord,
	_ *etcd.BackingHookEncryptedInputs,
	serviceID string,
	event backinghook.Event,
	consume BackingHookInputConsumer,
) error {
	return resolver.ResolveLifecycleHookInput(ctx, task, serviceID, event, consume)
}

// BACK-14: initial Custom creation starts the container before after-start and
// freezes the hook as the final environment-create step across plan rebuilds.
func TestBackingServiceCreationRunsAfterStartAfterRuntimeReadiness(t *testing.T) {
	reader, task := blueprintPlanTestState(t)
	serviceID := reader.projection.DesiredServices[0].Desired.ID
	definition := &backinghook.Definition{Command: []string{"/hook", "after-start"}, TimeoutSeconds: 20}
	reader.project.Kind, reader.project.TenantID = etcd.ProjectKindBacking, ""
	reader.projection.DesiredServices[0].Desired.Adapter = "custom"
	reader.projection.DesiredServices[0].Desired.Hooks = &backinghook.Configuration{AfterStart: definition}

	original := append([]etcd.TaskStepRecord(nil), task.Steps...)
	environmentStep := etcd.TaskStepRecord{Kind: etcd.TaskStepOperation, ID: ids.New(ids.KindStep)}
	hookStep := etcd.TaskStepRecord{Kind: etcd.TaskStepOperation, ID: ids.New(ids.KindStep)}
	task.Steps = []etcd.TaskStepRecord{environmentStep, original[1], original[0], original[2], hookStep}
	task.Params[etcd.TaskBackingServiceCreationParam] = serviceID
	task.Params[etcd.TaskBackingServiceVolumeDirectoryParam] = reader.environment.VolumeDir
	task.Params[etcd.TaskBackingServiceAfterStartParam] = serviceID
	task.Owner.ProjectID = reader.project.ID

	resolver, err := NewTaskPlanResolverWithBlueprints("/var/lib/groundplane/vol", reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	resolver.attachIdentities = backingHookPlanInputResolver{}
	plan, err := resolver.ResolveExecutionPlan(t.Context(), task)
	if err != nil {
		t.Fatalf("ResolveExecutionPlan() error = %v", err)
	}
	if plan.Operation != agentpb.PlanOperation_PLAN_OPERATION_ENVIRONMENT_CREATE || len(plan.Steps) != 5 {
		t.Fatalf("creation plan operation/steps = %v/%d", plan.Operation, len(plan.Steps))
	}
	apply := plan.Steps[3].GetComposeApply()
	hook := plan.Steps[4].GetBackingHookProcedure()
	if apply == nil || hook == nil || hook.Event != agentpb.BackingHookEvent_BACKING_HOOK_EVENT_AFTER_START ||
		plan.Steps[4].PrerequisiteStepId != plan.Steps[3].StepId ||
		hook.BackingServiceId != serviceID || len(hook.Inputs) != 1 ||
		hook.Inputs[0].Key != "HOST" || string(hook.Inputs[0].Value) != "svc.internal" {
		t.Fatalf("creation after-start order/input = %#v", plan.Steps)
	}
}
