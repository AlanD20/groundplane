package agent

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const (
	workerTestAssignmentID = "asgn_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	workerTestTaskID       = "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	workerOtherTaskID      = "task_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	workerTestStepID       = "step_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	workerTestServiceID    = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	workerTestArtifactID   = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV"
)

// Rationale: replaying one live assignment must neither execute twice nor
// replace the cancellation authority reserved by the first delivery.
func TestWorkerPoolDeduplicatesMatchingLiveAssignmentAndRejectsHashMismatch(t *testing.T) {
	t.Parallel()
	pool := NewWorkerPool(2, "/var/lib/groundplane/vol", nil, testLogger())
	started := make(chan struct{}, 2)
	pool.executeStep = func(ctx context.Context, _ *agentpb.ExecutionStep) error {
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		pool.Run(ctx)
		close(done)
	}()

	assignment := workerAssignment(workerTestTaskID, "plan-a")
	if err := pool.Submit(ctx, assignment); err != nil {
		t.Fatalf("Submit(first) error = %v", err)
	}
	if err := pool.Submit(ctx, assignment); err != nil {
		t.Fatalf("Submit(replay) error = %v", err)
	}
	mismatch := workerAssignment(workerTestTaskID, "plan-b")
	if err := pool.Submit(ctx, mismatch); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("Submit(mismatch) error = %v, want state conflict", err)
	}
	stale := assignment
	stale.AssignmentID = "asgn_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	if err := pool.Submit(ctx, stale); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("Submit(stale assignment) error = %v, want state conflict", err)
	}
	if capacity := pool.Capacity(); capacity != 1 {
		t.Fatalf("Capacity() = %d, want 1 reservation remaining", capacity)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first assignment did not start")
	}
	select {
	case <-started:
		t.Fatal("matching replay executed concurrently")
	default:
	}
	if err := pool.Abort(
		ctx, workerTestTaskID, "asgn_01ARZ3NDEKTSV4RRFFQ69G5FAW",
	); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("Abort(stale assignment) error = %v, want state conflict", err)
	}
	if err := pool.Abort(ctx, workerTestTaskID, assignment.AssignmentID); err != nil {
		t.Fatalf("Abort() error = %v", err)
	}
	result := nextWorkerResult(t, pool)
	if result.TaskID != workerTestTaskID || result.PlanHash != hashForPlan(assignment.Plan) ||
		result.Terminal != TaskTerminalAborted {
		t.Fatalf("result = %#v", result)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run() did not join workers")
	}
}

// Rationale: TaskAbort can overtake worker dequeue, so cancellation ownership
// must exist while queued and must suppress every step side effect.
func TestWorkerPoolRetainsQueuedAbortAndReservationCapacity(t *testing.T) {
	t.Parallel()
	pool := NewWorkerPool(1, "/var/lib/groundplane/vol", nil, testLogger())
	executed := make(chan struct{}, 1)
	pool.executeStep = func(context.Context, *agentpb.ExecutionStep) error {
		executed <- struct{}{}
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	assignment := workerAssignment(workerTestTaskID, "plan-a")
	if err := pool.Submit(ctx, assignment); err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if capacity := pool.Capacity(); capacity != 0 {
		t.Fatalf("Capacity() = %d, want queued reservation to consume the slot", capacity)
	}
	if err := pool.Abort(ctx, workerTestTaskID, assignment.AssignmentID); err != nil {
		t.Fatalf("Abort() error = %v", err)
	}
	done := make(chan struct{})
	go func() {
		pool.Run(ctx)
		close(done)
	}()
	result := nextWorkerResult(t, pool)
	if result.Terminal != TaskTerminalAborted {
		t.Fatalf("terminal = %v, want aborted", result.Terminal)
	}
	select {
	case <-executed:
		t.Fatal("queued aborted assignment executed a step")
	default:
	}
	cancel()
	<-done
}

// Rationale: the gRPC receive goroutine is the only task/control reader; a
// full pool must reject promptly instead of blocking TaskAbort or Shutdown.
func TestWorkerPoolSubmitReturnsConflictWithoutBlockingWhenFull(t *testing.T) {
	t.Parallel()
	pool := NewWorkerPool(1, "/var/lib/groundplane/vol", nil, testLogger())
	ctx := context.Background()
	if err := pool.Submit(ctx, workerAssignment(workerTestTaskID, "plan-a")); err != nil {
		t.Fatalf("Submit(first) error = %v", err)
	}
	returned := make(chan error, 1)
	go func() { returned <- pool.Submit(ctx, workerAssignment(workerOtherTaskID, "plan-b")) }()
	select {
	case err := <-returned:
		if !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
			t.Fatalf("Submit(full) error = %v, want state conflict", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Submit(full) blocked the control loop")
	}
}

func TestWorkerPoolRejectsMissingAssignmentIdentity(t *testing.T) {
	t.Parallel()
	pool := NewWorkerPool(1, "/var/lib/groundplane/vol", nil, testLogger())
	assignment := workerAssignment(workerTestTaskID, "plan-a")
	assignment.AssignmentID = ""
	if err := pool.Submit(context.Background(), assignment); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("Submit(missing assignment) error = %v, want internal", err)
	}
}

// Rationale: Run returning is the shutdown join boundary; cancellation must
// reach active task contexts and wait for their executor cleanup.
func TestWorkerPoolCancellationJoinsActiveWorkers(t *testing.T) {
	t.Parallel()
	pool := NewWorkerPool(1, "/var/lib/groundplane/vol", nil, testLogger())
	started := make(chan struct{})
	exited := make(chan struct{})
	pool.executeStep = func(ctx context.Context, _ *agentpb.ExecutionStep) error {
		close(started)
		<-ctx.Done()
		close(exited)
		return ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := pool.Submit(ctx, workerAssignment(workerTestTaskID, "plan-a")); err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	done := make(chan struct{})
	go func() {
		pool.Run(ctx)
		close(done)
	}()
	<-started
	cancel()
	select {
	case <-done:
		select {
		case <-exited:
		default:
			t.Fatal("Run() returned before executor cleanup")
		}
	case <-time.After(time.Second):
		t.Fatal("Run() did not join canceled worker")
	}
}

func workerAssignment(taskID, plan string) Assignment {
	planID := "plan_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	if plan == "plan-b" {
		planID = "plan_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	}
	yaml := []byte("services:\n  api:\n    image: registry.example/api@sha256:" +
		"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\n")
	yamlHash := sha256.Sum256(yaml)
	unsealed := &agentpb.ExecutionPlan{
		Schema: executionplan.SchemaVersion, PlanId: planID, RenderGeneration: 1,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_DEPLOY,
		TargetId:  workerTestServiceID,
		Artifacts: []*agentpb.ComposeArtifact{{
			ArtifactId:  workerTestArtifactID,
			OwnerKind:   agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_PLATFORM,
			ProjectName: "groundplane-infra", CanonicalYaml: yaml, YamlSha256: yamlHash[:],
			Services: []*agentpb.ComposeService{{
				ServiceId: workerTestServiceID, ComposeName: "api", ExpectedReplicas: 1, HasHealthcheck: true,
				ExpectedLabels: []*agentpb.LabelPair{
					{Key: "com.groundplane.kind", Value: "service"},
					{Key: "com.groundplane.managed", Value: "true"},
					{Key: "com.groundplane.plan-id", Value: planID},
					{Key: "com.groundplane.render-generation", Value: "1"},
					{Key: "com.groundplane.service-id", Value: workerTestServiceID},
				},
			}},
		}},
		Steps: []*agentpb.ExecutionStep{{
			StepId: workerTestStepID, TimeoutSeconds: 30,
			Payload: &agentpb.ExecutionStep_ComposeApply{ComposeApply: &agentpb.ComposeApply{
				ArtifactId: workerTestArtifactID, ServiceIds: []string{workerTestServiceID},
			}},
		}},
	}
	sealed, err := executionplan.Seal(unsealed)
	if err != nil {
		panic(err)
	}
	return Assignment{
		AssignmentID: workerTestAssignmentID,
		TaskID:       taskID, OperationID: "op_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Plan: sealed, Deadline: time.Now().Add(time.Minute),
	}
}

func nextWorkerResult(t *testing.T, pool *WorkerPool) TaskResult {
	t.Helper()
	for {
		select {
		case output := <-pool.Outputs():
			if output.Result != nil {
				return *output.Result
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for worker result")
		}
	}
}
