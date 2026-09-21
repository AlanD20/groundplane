package taskplanning

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testattachments "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testtaskconfiguration "github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type backingHookPlanInputResolver struct{}

func (state *attachPlanTestState) ResolveHookInput(
	_ context.Context,
	_ testkeyvalue.Versioned[testattachments.Record],
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
	context.Context, testkeyvalue.Versioned[testattachments.Record], string,

	AttachPlanIdentityConsumer,
) error {
	return nil
}

func (backingHookPlanInputResolver) ResolveHookInput(
	context.Context, testkeyvalue.Versioned[testattachments.Record],
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
	_ *testtaskconfiguration.BackingHookEncryptedInputs,
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
	reader.project.Kind, reader.project.TenantID = testhierarchy.ProjectKindBacking, ""
	reader.projection.DesiredServices[0].Desired.Adapter = "custom"
	reader.projection.DesiredServices[0].Desired.Hooks = &backinghook.Configuration{AfterStart: definition}

	original := append([]testtaskjournal.TaskStepRecord(nil), task.Steps...)
	environmentStep := testtaskjournal.TaskStepRecord{
		Kind: testtaskjournal.TaskStepOperation,
		ID:   ids.New(ids.KindStep),
	}
	hookStep := testtaskjournal.TaskStepRecord{Kind: testtaskjournal.TaskStepOperation, ID: ids.New(ids.KindStep)}
	task.Steps = []testtaskjournal.TaskStepRecord{environmentStep, original[1], original[0], original[2], hookStep}
	task.Params[testtaskjournal.TaskBackingServiceCreationParam] = serviceID
	task.Params[testtaskjournal.TaskBackingServiceVolumeDirectoryParam] = reader.environment.VolumeDir
	task.Params[testtaskconfiguration.TaskBackingServiceAfterStartParam] = serviceID
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
