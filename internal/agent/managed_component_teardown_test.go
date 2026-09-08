package agent

import (
	"context"
	"errors"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Rationale: a stale managed container may differ only in volatile plan
// labels, but a foreign Component or Service at the same Compose name must
// stop teardown before the helper receives mutation authority.
func TestManagedComponentTeardownPreflightRejectsForeignSameName(t *testing.T) {
	t.Parallel()
	source := &agentpb.ManagedComponentService{
		ComponentId: "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV", ServiceId: "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		ComposeServiceName: "caddy", SourceArtifactId: "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		RemoveStepId: "step_01ARZ3NDEKTSV4RRFFQ69G5FAV",
	}
	plan := &agentpb.ExecutionPlan{ManagedComponentProcedure: &agentpb.ManagedComponentProcedure{
		Services: []*agentpb.ManagedComponentService{source},
	}}
	observer := &fakeComposeObserver{projects: []*agentpb.ObservedProject{{
		Collisions: []*agentpb.ObservedCollision{{
			Kind:               agentpb.ObservedCollisionKind_OBSERVED_COLLISION_KIND_CONTAINER,
			ComposeServiceName: "caddy", ComponentId: "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAW",
			ServiceId: source.ServiceId,
		}},
	}}}
	runtime := &ComposeRuntime{observer: observer}

	stepID, err := runtime.preflightManagedComponentTeardown(context.Background(), plan)
	if stepID != source.RemoveStepId || !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("preflight = %q, %v", stepID, err)
	}
}

// Rationale: a prior plan's labels are expected during cleanup; exact stable
// Component and Service ownership must remain sufficient removal authority.
func TestManagedComponentTeardownPreflightAcceptsOwnedStaleContainer(t *testing.T) {
	t.Parallel()
	source := &agentpb.ManagedComponentService{
		ComponentId: "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV", ServiceId: "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		ComposeServiceName: "caddy", SourceArtifactId: "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		RemoveStepId: "step_01ARZ3NDEKTSV4RRFFQ69G5FAV",
	}
	plan := &agentpb.ExecutionPlan{ManagedComponentProcedure: &agentpb.ManagedComponentProcedure{
		Services: []*agentpb.ManagedComponentService{source},
	}}
	observer := &fakeComposeObserver{projects: []*agentpb.ObservedProject{{
		Collisions: []*agentpb.ObservedCollision{{
			Kind:               agentpb.ObservedCollisionKind_OBSERVED_COLLISION_KIND_CONTAINER,
			ComposeServiceName: "caddy", ComponentId: source.ComponentId, ServiceId: source.ServiceId,
		}},
	}}}
	runtime := &ComposeRuntime{observer: observer}

	stepID, err := runtime.preflightManagedComponentTeardown(context.Background(), plan)
	if err != nil || stepID != "" {
		t.Fatalf("preflight = %q, %v", stepID, err)
	}
}

func TestManagedComponentTeardownPostconditionRejectsRemainingContainer(t *testing.T) {
	t.Parallel()
	assignment, _ := composeRuntimeAssignment()
	step := &agentpb.ExecutionStep{
		StepId: "step_remove", TimeoutSeconds: 30,
		Payload: &agentpb.ExecutionStep_ComposeRemove{ComposeRemove: &agentpb.ComposeRemove{
			ArtifactId: "artifact_platform", ServiceIds: []string{"svc_api"},
		}},
	}
	source := &agentpb.ManagedComponentService{
		ComponentKind: "caddy", ComponentId: "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		ServiceId: "svc_api", ComposeServiceName: "api", SourceArtifactId: "artifact_platform",
		RemoveStepId: step.StepId,
	}
	assignment.Plan.ManagedComponentProcedure = &agentpb.ManagedComponentProcedure{
		Services: []*agentpb.ManagedComponentService{source},
	}
	helper := completedComposeHelper()
	observer := &fakeComposeObserver{projects: []*agentpb.ObservedProject{{
		ProjectName: "gp-platform", Containers: []*agentpb.ObservedContainer{{ServiceId: "svc_api"}},
	}}}
	runtime, err := NewComposeRuntime(helper, observer)
	if err != nil {
		t.Fatal(err)
	}
	result, err := runtime.executeStep(context.Background(), assignment, step)
	if !errors.Is(err, errs.New(errs.KindRequestFailed, "")) || !result.ReconciliationRequired ||
		helper.request == nil || observer.calls != 1 {
		t.Fatalf("teardown result = %#v, error = %v, observations = %d", result, err, observer.calls)
	}
}
