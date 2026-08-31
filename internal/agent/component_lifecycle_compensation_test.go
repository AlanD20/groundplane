package agent

import (
	"context"
	"crypto/sha256"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/infra/docker/composehelper"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type compensationSelectionHelper struct {
	taskRunner runner.Runner
	request    *agentpb.ComposeHelperRequest
}

type compensationOrderingHelper struct{ events *[]string }

func (helper compensationOrderingHelper) Execute(
	_ context.Context,
	request *agentpb.ComposeHelperRequest,
) (*agentpb.ComposeHelperResponse, error) {
	step := request.GetPlan().GetSteps()[0]
	if step.GetComposeApply() != nil {
		*helper.events = append(*helper.events, "compose-apply")
	} else if step.GetComposeRemove() != nil {
		*helper.events = append(*helper.events, "compose-remove")
	}
	return &agentpb.ComposeHelperResponse{
		Schema:     composeHelperSchema,
		Outcome:    agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED,
		Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE,
	}, nil
}

func (helper *compensationSelectionHelper) Execute(
	ctx context.Context,
	request *agentpb.ComposeHelperRequest,
) (*agentpb.ComposeHelperResponse, error) {
	helper.request = proto.Clone(request).(*agentpb.ComposeHelperRequest)
	return composehelper.Execute(ctx, helper.taskRunner, request)
}

type compensationActionRuntime struct{ events *[]string }

func (runtime compensationActionRuntime) ExecuteComponentAction(
	context.Context,
	Assignment,
	*agentpb.ExecutionStep,
	ManagedConfigPayload,
) (*ComponentActionResult, error) {
	*runtime.events = append(*runtime.events, "observe")
	return &ComponentActionResult{DNSResolverObservation: &agentpb.DNSResolverObservationEvidence{}}, nil
}

func (runtime compensationActionRuntime) FinalizeManagedConfig(
	_ context.Context,
	_ Assignment,
	step *agentpb.ExecutionStep,
	commit bool,
) (ManagedConfigTransactionState, error) {
	if commit {
		*runtime.events = append(*runtime.events, "commit")
	} else {
		*runtime.events = append(*runtime.events, "rollback")
	}
	state := ManagedConfigTransactionState{}
	expected := step.GetComponentApply().GetExpectedPreviousArtifactDigest()
	if len(expected) == sha256.Size {
		state.Live.Present = true
		state.Previous.Present = true
		copy(state.Live.SHA256[:], expected)
		copy(state.Previous.SHA256[:], expected)
	}
	return state, nil
}

type compensationHostRuntime struct{ events *[]string }

func (runtime compensationHostRuntime) ExecuteHostResolution(
	_ context.Context,
	_ Assignment,
	step *agentpb.ExecutionStep,
) error {
	if step.GetHostResolutionApply() != nil {
		*runtime.events = append(*runtime.events, "host-apply")
	} else {
		*runtime.events = append(*runtime.events, "host-restore")
	}
	return nil
}

func TestEnableCompensationReversesResolverServiceAndCandidate(t *testing.T) {
	events := []string{}
	compose, err := NewComposeRuntime(
		compensationOrderingHelper{events: &events},
		&fakeComposeObserver{projects: []*agentpb.ObservedProject{{ProjectName: "groundplane-infra"}}},
	)
	if err != nil {
		t.Fatalf("NewComposeRuntime() error = %v", err)
	}
	pool := NewWorkerPool(1, "/tmp", nil, testLogger())
	pool.compose = compose
	pool.componentActions = compensationActionRuntime{events: &events}
	pool.hostResolution = compensationHostRuntime{events: &events}
	assignment, apply := sealedLifecycleCompensationAssignment(
		t,
		agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_ENABLE,
	)
	managed := assignment.Plan.GetSteps()[0]
	host := assignment.Plan.GetSteps()[3]
	if _, err := pool.compensateComponentLifecycle(
		assignment,
		managed,
		[]*agentpb.ExecutionStep{managed, apply, host},
	); err != nil {
		t.Fatal(err)
	}
	want := []string{"host-restore", "compose-remove", "rollback"}
	if len(events) != len(want) {
		t.Fatalf("events = %v", events)
	}
	for index := range want {
		if events[index] != want[index] {
			t.Fatalf("events = %v, want %v", events, want)
		}
	}
}

func TestDisableCompensationReappliesServiceBeforeResolver(t *testing.T) {
	events := []string{}
	compose, err := NewComposeRuntime(
		compensationOrderingHelper{events: &events},
		&fakeComposeObserver{projects: []*agentpb.ObservedProject{{ProjectName: "groundplane-infra"}}},
	)
	if err != nil {
		t.Fatalf("NewComposeRuntime() error = %v", err)
	}
	pool := NewWorkerPool(1, "/tmp", nil, testLogger())
	pool.compose = compose
	pool.componentActions = compensationActionRuntime{events: &events}
	pool.hostResolution = compensationHostRuntime{events: &events}
	assignment, remove := sealedLifecycleCompensationAssignment(
		t,
		agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_DISABLE,
	)
	restore := assignment.Plan.GetSteps()[0]
	if _, err := pool.compensateComponentLifecycle(
		assignment,
		nil,
		[]*agentpb.ExecutionStep{restore, remove},
	); err != nil {
		t.Fatal(err)
	}
	want := []string{"compose-apply", "observe", "host-apply"}
	for index := range want {
		if events[index] != want[index] {
			t.Fatalf("events = %v, want %v", events, want)
		}
	}
}

func TestEnableCompensationRequiresComposeRuntime(t *testing.T) {
	assignment, composeStep := sealedLifecycleCompensationAssignment(
		t,
		agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_ENABLE,
	)
	events := []string{}
	pool := NewWorkerPool(1, "/tmp", nil, testLogger())
	pool.componentActions = compensationActionRuntime{events: &events}
	bypassCalled := false
	pool.executeStep = func(context.Context, *agentpb.ExecutionStep) error {
		bypassCalled = true
		return nil
	}
	managedStep := assignment.Plan.GetSteps()[0]

	_, err := pool.compensateComponentLifecycle(
		assignment,
		managedStep,
		[]*agentpb.ExecutionStep{managedStep, composeStep},
	)
	if err == nil || !strings.Contains(err.Error(), "Compose compensation runtime is not configured") {
		t.Fatalf("compensateComponentLifecycle() error = %v, want missing Compose runtime", err)
	}
	if bypassCalled {
		t.Fatal("Compose compensation used the unvalidated executeStep bypass")
	}
}

func TestEnableCompensationSealsComposeRemovePlan(t *testing.T) {
	assignment, composeStep := sealedLifecycleCompensationAssignment(
		t,
		agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_ENABLE,
	)
	taskRunner := runner.NewFake()
	helper := &compensationSelectionHelper{taskRunner: taskRunner}
	compose, err := NewComposeRuntime(
		helper,
		&fakeComposeObserver{projects: []*agentpb.ObservedProject{{ProjectName: "groundplane-infra"}}},
	)
	if err != nil {
		t.Fatalf("NewComposeRuntime() error = %v", err)
	}
	events := []string{}
	pool := NewWorkerPool(1, "/tmp", nil, testLogger())
	pool.compose = compose
	pool.componentActions = compensationActionRuntime{events: &events}
	managedStep := assignment.Plan.GetSteps()[0]

	if _, err := pool.compensateComponentLifecycle(
		assignment,
		managedStep,
		[]*agentpb.ExecutionStep{managedStep, composeStep},
	); err != nil {
		t.Fatalf("compensateComponentLifecycle() error = %v", err)
	}
	plan := requireSealedComposeCompensationPlan(t, helper.request, assignment.Plan.GetArtifacts()[0], composeStep)
	remove := plan.GetSteps()[0].GetComposeRemove()
	if remove == nil || remove.GetArtifactId() != composeStep.GetComposeApply().GetArtifactId() ||
		len(remove.GetServiceIds()) != 1 || remove.GetServiceIds()[0] != composeStep.GetComposeApply().GetServiceIds()[0] {
		t.Fatalf("derived compensation step = %#v", plan.GetSteps()[0])
	}
	if len(taskRunner.Calls) != 1 ||
		!strings.Contains(strings.Join(taskRunner.Calls[0].Args, " "), " rm --stop --force resolver") {
		t.Fatalf("Compose helper calls = %#v, want one Compose remove", taskRunner.Calls)
	}
}

func TestDisableCompensationSealsComposeApplyPlan(t *testing.T) {
	assignment, composeStep := sealedLifecycleCompensationAssignment(
		t,
		agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_DISABLE,
	)
	taskRunner := runner.NewFake()
	helper := &compensationSelectionHelper{taskRunner: taskRunner}
	compose, err := NewComposeRuntime(
		helper,
		&fakeComposeObserver{projects: []*agentpb.ObservedProject{{ProjectName: "groundplane-infra"}}},
	)
	if err != nil {
		t.Fatalf("NewComposeRuntime() error = %v", err)
	}
	events := []string{}
	pool := NewWorkerPool(1, "/tmp", nil, testLogger())
	pool.compose = compose
	pool.componentActions = compensationActionRuntime{events: &events}

	if _, err := pool.compensateComponentLifecycle(
		assignment,
		nil,
		[]*agentpb.ExecutionStep{composeStep},
	); err != nil {
		t.Fatalf("compensateComponentLifecycle() error = %v", err)
	}
	plan := requireSealedComposeCompensationPlan(t, helper.request, assignment.Plan.GetArtifacts()[0], composeStep)
	apply := plan.GetSteps()[0].GetComposeApply()
	if apply == nil || apply.GetArtifactId() != composeStep.GetComposeRemove().GetArtifactId() ||
		len(apply.GetServiceIds()) != 1 || apply.GetServiceIds()[0] != composeStep.GetComposeRemove().GetServiceIds()[0] ||
		!apply.GetForceRecreate() || !apply.GetNoDependencies() {
		t.Fatalf("derived compensation step = %#v", plan.GetSteps()[0])
	}
	if len(taskRunner.Calls) != 2 ||
		!strings.Contains(strings.Join(taskRunner.Calls[0].Args, " "), " config --quiet --no-interpolate") ||
		!strings.Contains(strings.Join(taskRunner.Calls[1].Args, " "), " up --detach --force-recreate --no-deps resolver") {
		t.Fatalf("Compose helper calls = %#v, want validation and Compose apply", taskRunner.Calls)
	}
}

func requireSealedComposeCompensationPlan(
	t *testing.T,
	request *agentpb.ComposeHelperRequest,
	sourceArtifact *agentpb.ComposeArtifact,
	forwardStep *agentpb.ExecutionStep,
) *agentpb.ExecutionPlan {
	t.Helper()
	if request == nil {
		t.Fatal("Compose helper request is missing")
	}
	plan, err := executionplan.Validate(request.GetPlan())
	if err != nil {
		t.Fatalf("executionplan.Validate() error = %v", err)
	}
	if plan.GetOperation() != agentpb.PlanOperation_PLAN_OPERATION_RECONCILE ||
		plan.GetComponentLifecycleMode() != agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_UNSPECIFIED ||
		plan.GetComponentRollbackObservation() != nil || len(plan.GetArtifacts()) != 1 || len(plan.GetSteps()) != 1 {
		t.Fatalf("derived compensation plan shape = %#v", plan)
	}
	if plan.GetPlanId() != sourceArtifact.GetServices()[0].GetExpectedLabels()[2].GetValue() ||
		plan.GetRenderGeneration() != 7 || !proto.Equal(plan.GetArtifacts()[0], sourceArtifact) {
		t.Fatalf("derived compensation authority or artifact changed: %#v", plan)
	}
	if request.GetStepId() != plan.GetSteps()[0].GetStepId() ||
		plan.GetSteps()[0].GetStepId() == forwardStep.GetStepId() {
		t.Fatalf("derived compensation step selection = %q, forward = %q", request.GetStepId(), forwardStep.GetStepId())
	}
	return plan
}

func sealedLifecycleCompensationAssignment(
	t *testing.T,
	mode agentpb.ComponentLifecycleMode,
) (Assignment, *agentpb.ExecutionStep) {
	t.Helper()
	now := time.Date(2026, time.August, 31, 12, 0, 0, 0, time.UTC)
	componentID := ids.NewAt(ids.KindComponent, now, 1)
	planID := ids.NewAt(ids.KindPlan, now, 2)
	artifactID := ids.NewAt(ids.KindConfig, now, 3)
	serviceID := ids.NewAt(ids.KindService, now, 4)
	yaml := []byte("services:\n  resolver:\n    image: registry.example/resolver@sha256:" +
		"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\n")
	yamlDigest := sha256.Sum256(yaml)
	actionDigest := sha256.Sum256([]byte("component lifecycle compensation"))
	artifact := &agentpb.ComposeArtifact{
		ArtifactId: artifactID, OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_PLATFORM,
		ProjectName: "groundplane-infra", CanonicalYaml: yaml, YamlSha256: yamlDigest[:],
		Services: []*agentpb.ComposeService{{
			ServiceId: serviceID, ComposeName: "resolver", ExpectedReplicas: 1,
			ExpectedLabels: []*agentpb.LabelPair{
				{Key: "com.groundplane.kind", Value: "service"},
				{Key: "com.groundplane.managed", Value: "true"},
				{Key: "com.groundplane.plan-id", Value: planID},
				{Key: "com.groundplane.render-generation", Value: "7"},
				{Key: "com.groundplane.service-id", Value: serviceID},
			},
		}},
	}
	plan := &agentpb.ExecutionPlan{
		Schema: executionplan.SchemaVersion, PlanId: planID, RenderGeneration: 7,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_COMPONENT_APPLY, TargetId: componentID,
		ComponentLifecycleMode: mode, Artifacts: []*agentpb.ComposeArtifact{artifact},
	}
	var composeStep *agentpb.ExecutionStep
	if mode == agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_ENABLE {
		managed := &agentpb.ExecutionStep{
			StepId: ids.NewAt(ids.KindStep, now, 5), TimeoutSeconds: 30,
			Payload: &agentpb.ExecutionStep_ComponentApply{ComponentApply: &agentpb.ComponentApply{
				ComponentId: componentID, DefinitionDigest: actionDigest[:], CatalogDigest: actionDigest[:],
				ActionId: "publish-config", ArtifactId: artifactID, ArtifactDigest: actionDigest[:],
				Generation: 7, ManagedConfigContent: true,
			}},
		}
		composeStep = &agentpb.ExecutionStep{
			StepId: ids.NewAt(ids.KindStep, now, 6), TimeoutSeconds: 30, PrerequisiteStepId: managed.GetStepId(),
			Payload: &agentpb.ExecutionStep_ComposeApply{ComposeApply: &agentpb.ComposeApply{
				ArtifactId: artifactID, ServiceIds: []string{serviceID}, ForceRecreate: true, NoDependencies: true,
			}},
		}
		observation := &agentpb.ExecutionStep{
			StepId: ids.NewAt(ids.KindStep, now, 7), TimeoutSeconds: 30, PrerequisiteStepId: composeStep.GetStepId(),
			Payload: &agentpb.ExecutionStep_ComponentApply{ComponentApply: &agentpb.ComponentApply{
				ComponentId: componentID, DefinitionDigest: actionDigest[:], CatalogDigest: actionDigest[:],
				ActionId: "observe-serving", ArtifactId: artifactID, ArtifactDigest: actionDigest[:], Generation: 7,
			}},
		}
		host := &agentpb.ExecutionStep{
			StepId: ids.NewAt(ids.KindStep, now, 8), TimeoutSeconds: 30, PrerequisiteStepId: observation.GetStepId(),
			Payload: &agentpb.ExecutionStep_HostResolutionApply{HostResolutionApply: &agentpb.HostResolutionApply{
				ComponentId: componentID, Generation: 7,
			}},
		}
		plan.Steps = []*agentpb.ExecutionStep{managed, composeStep, observation, host}
	} else {
		restore := &agentpb.ExecutionStep{
			StepId: ids.NewAt(ids.KindStep, now, 5), TimeoutSeconds: 30,
			Payload: &agentpb.ExecutionStep_HostResolutionRestore{HostResolutionRestore: &agentpb.HostResolutionRestore{
				ComponentId: componentID, Generation: 7,
			}},
		}
		composeStep = &agentpb.ExecutionStep{
			StepId: ids.NewAt(ids.KindStep, now, 6), TimeoutSeconds: 30, PrerequisiteStepId: restore.GetStepId(),
			Payload: &agentpb.ExecutionStep_ComposeRemove{ComposeRemove: &agentpb.ComposeRemove{
				ArtifactId: artifactID, ServiceIds: []string{serviceID},
			}},
		}
		plan.ComponentRollbackObservation = &agentpb.ComponentApply{
			ComponentId: componentID, DefinitionDigest: actionDigest[:], CatalogDigest: actionDigest[:],
			ActionId: "observe-serving", ArtifactId: artifactID, ArtifactDigest: actionDigest[:], Generation: 7,
		}
		plan.Steps = []*agentpb.ExecutionStep{restore, composeStep}
	}
	sealed, err := executionplan.Seal(plan)
	if err != nil {
		t.Fatalf("executionplan.Seal() error = %v", err)
	}
	return Assignment{
		AssignmentID: ids.NewAt(ids.KindAssignment, now, 9),
		TaskID:       ids.NewAt(ids.KindTask, now, 10),
		OperationID:  ids.NewAt(ids.KindOperation, now, 11),
		Plan:         sealed,
	}, sealed.GetSteps()[1]
}
