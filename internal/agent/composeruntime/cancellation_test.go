package composeruntime

import (
	"context"
	"errors"
	"testing"
	"time"

	testtaskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type cancellationHelper struct{}

func (cancellationHelper) Execute(
	context.Context,
	*agentpb.ComposeHelperRequest,
) (*agentpb.ComposeHelperResponse, error) {
	return nil, context.Canceled
}

type cancellationObserver struct {
	calls       int
	liveContext bool
}

func (observer *cancellationObserver) Observe(
	ctx context.Context,
	_ *agentpb.ExecutionPlan,
	_ string,
) (*agentpb.ObservedProject, error) {
	observer.calls++
	observer.liveContext = ctx.Err() == nil
	return &agentpb.ObservedProject{ProjectName: "gp-platform"}, nil
}

func (observer *cancellationObserver) ObserveRestoration(
	ctx context.Context,
	_ *executionplan.RestorationObservation,
) (*agentpb.ObservedProject, error) {
	return observer.Observe(ctx, nil, "")
}

func (observer *cancellationObserver) ObserveReleaseRestoration(
	ctx context.Context,
	_ *agentpb.ExecutionPlan,
	_ string,
	_ string,
) (*agentpb.ObservedProject, error) {
	return observer.Observe(ctx, nil, "")
}

func TestComposeRuntimeObservesAfterCancelledMutation(t *testing.T) {
	observer := &cancellationObserver{}
	runtime, err := New(cancellationHelper{}, observer)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	step := &agentpb.ExecutionStep{
		StepId: "step_apply", TimeoutSeconds: 30,
		Payload: &agentpb.ExecutionStep_ComposeApply{ComposeApply: &agentpb.ComposeApply{
			ArtifactId: "artifact_platform", ServiceIds: []string{"svc_api"},
		}},
	}
	assignment := testtaskassignment.Assignment{
		TaskID: "tsk_01J00000000000000000000000", OperationID: "op_01J00000000000000000000000",
		Plan: &agentpb.ExecutionPlan{Artifacts: []*agentpb.ComposeArtifact{{
			ArtifactId: "artifact_platform", ProjectName: "gp-platform",
		}}},
		Deadline: time.Now().Add(time.Minute),
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result, err := runtime.mutate(ctx, assignment, step, "artifact_platform", nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("mutate() error = %v, want cancellation", err)
	}
	if observer.calls != 1 || !observer.liveContext {
		t.Fatalf("cancel reconciliation observations = %d, live = %t", observer.calls, observer.liveContext)
	}
	if !result.ReconciliationRequired || !result.MutationAttempted {
		t.Fatalf("mutate() result = %#v, want reconciliation evidence", result)
	}
}
