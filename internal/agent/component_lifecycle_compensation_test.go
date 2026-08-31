package agent

import (
	"context"
	"crypto/sha256"
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

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
	pool := NewWorkerPool(1, "/tmp", nil, testLogger())
	pool.componentActions = compensationActionRuntime{events: &events}
	pool.hostResolution = compensationHostRuntime{events: &events}
	pool.executeStep = func(_ context.Context, step *agentpb.ExecutionStep) error {
		if step.GetComposeRemove() != nil {
			events = append(events, "compose-remove")
		}
		return nil
	}
	managed := managedLifecycleStep()
	managed.GetComponentApply().ExpectedPreviousArtifactDigest = make([]byte, sha256.Size)
	apply := &agentpb.ExecutionStep{
		StepId: "apply",
		Payload: &agentpb.ExecutionStep_ComposeApply{
			ComposeApply: &agentpb.ComposeApply{ArtifactId: "cfg_candidate", ServiceIds: []string{"svc_coredns"}},
		},
	}
	host := &agentpb.ExecutionStep{
		StepId: "host",
		Payload: &agentpb.ExecutionStep_HostResolutionApply{
			HostResolutionApply: &agentpb.HostResolutionApply{ComponentId: "cmp_coredns", Generation: 7},
		},
	}
	assignment := lifecycleAssignment(
		agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_ENABLE,
		managed,
		apply,
		host,
	)
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
	pool := NewWorkerPool(1, "/tmp", nil, testLogger())
	pool.componentActions = compensationActionRuntime{events: &events}
	pool.hostResolution = compensationHostRuntime{events: &events}
	pool.executeStep = func(_ context.Context, step *agentpb.ExecutionStep) error {
		if step.GetComposeApply() != nil {
			events = append(events, "compose-apply")
		}
		return nil
	}
	restore := &agentpb.ExecutionStep{
		StepId: "restore",
		Payload: &agentpb.ExecutionStep_HostResolutionRestore{
			HostResolutionRestore: &agentpb.HostResolutionRestore{ComponentId: "cmp_coredns", Generation: 7},
		},
	}
	remove := &agentpb.ExecutionStep{
		StepId: "remove",
		Payload: &agentpb.ExecutionStep_ComposeRemove{
			ComposeRemove: &agentpb.ComposeRemove{ArtifactId: "cfg_candidate", ServiceIds: []string{"svc_coredns"}},
		},
	}
	assignment := lifecycleAssignment(agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_DISABLE, restore, remove)
	assignment.Plan.ComponentRollbackObservation = &agentpb.ComponentApply{
		ComponentId: "cmp_coredns", ArtifactId: "cfg_candidate", ArtifactDigest: make([]byte, sha256.Size),
		Generation: 7,
	}
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

func managedLifecycleStep() *agentpb.ExecutionStep {
	return &agentpb.ExecutionStep{
		StepId: "publish",
		Payload: &agentpb.ExecutionStep_ComponentApply{
			ComponentApply: &agentpb.ComponentApply{ManagedConfigContent: true},
		},
	}
}

func lifecycleAssignment(mode agentpb.ComponentLifecycleMode, steps ...*agentpb.ExecutionStep) Assignment {
	return Assignment{
		Plan: &agentpb.ExecutionPlan{
			Operation:              agentpb.PlanOperation_PLAN_OPERATION_COMPONENT_APPLY,
			ComponentLifecycleMode: mode,
			Steps:                  steps,
		},
	}
}
