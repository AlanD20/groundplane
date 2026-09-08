package agent

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type releaseExecutionHelper struct {
	mu        sync.Mutex
	responses map[string]*agentpb.ComposeHelperResponse
	calls     []releaseExecutionCall
}

type releaseExecutionCall struct {
	taskID string
	stepID string
}

type releaseExecutionObserver struct{}

func (releaseExecutionObserver) Observe(
	_ context.Context,
	_ *agentpb.ExecutionPlan,
	_ string,
) (*agentpb.ObservedProject, error) {
	return proto.Clone(releaseObservedProject()).(*agentpb.ObservedProject), nil
}

func (helper *releaseExecutionHelper) Execute(
	_ context.Context,
	request *agentpb.ComposeHelperRequest,
) (*agentpb.ComposeHelperResponse, error) {
	helper.mu.Lock()
	defer helper.mu.Unlock()
	helper.calls = append(helper.calls, releaseExecutionCall{taskID: request.TaskId, stepID: request.StepId})
	return helper.responses[request.StepId], nil
}

// Rationale: switch_back is physical compensation, not a status label. A
// later member failure must restore each prior successful proxy in reverse.
func TestExecuteReleaseCompensatesPriorSuccessfulSwitch(t *testing.T) {
	t.Parallel()
	helper := &releaseExecutionHelper{responses: map[string]*agentpb.ComposeHelperResponse{
		"switch-api": releaseExecutionSuccess("api", false),
		"switch-worker": {
			Outcome: agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_FAILED, ExitCode: 17,
			Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPONENT_ACTIVATION_FAILED,
		},
		"compensate-api": releaseExecutionSuccess("api", true),
	}}
	pool := releaseExecutionPool(t, helper)
	plan := &agentpb.ExecutionPlan{
		Operation: agentpb.PlanOperation_PLAN_OPERATION_DEPLOY,
		Steps: []*agentpb.ExecutionStep{
			releaseForwardSwitch("switch-api"), releaseForwardSwitch("switch-worker"),
			releaseCompensate("compensate-api", "switch-api"),
		},
	}
	result := runReleaseExecution(t, pool, "task-initial", "", plan)
	if result.Terminal != TaskTerminalFailed || result.Compose.GetReconciliationRequired() ||
		len(result.Compose.GetProxyEvidence()) != 1 || !result.Compose.GetProxyEvidence()[0].GetCompensated() {
		t.Fatalf("release result = %#v", result)
	}
	assertReleaseExecutionCalls(t, helper, []releaseExecutionCall{
		{taskID: "task-initial", stepID: "switch-api"},
		{taskID: "task-initial", stepID: "switch-worker"},
		{taskID: "task-initial", stepID: "compensate-api"},
	})
}

// Rationale: a failed candidate mutation without any applicable sealed
// compensation can never claim that reconciliation is complete.
func TestExecuteReleaseZeroApplicableCompensationRemainsReconciliationRequired(t *testing.T) {
	t.Parallel()
	helper := &releaseExecutionHelper{responses: map[string]*agentpb.ComposeHelperResponse{
		"switch-api": {
			Schema: 1, Outcome: agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_FAILED, ExitCode: 17,
			Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPOSE_FAILED,
		},
	}}
	plan := &agentpb.ExecutionPlan{
		Operation: agentpb.PlanOperation_PLAN_OPERATION_DEPLOY,
		Steps: []*agentpb.ExecutionStep{
			releaseForwardSwitch("switch-api"),
		},
	}
	result := runReleaseExecution(t, releaseExecutionPool(t, helper), "task-zero-compensation", "", plan)
	if result.Terminal != TaskTerminalFailed || !result.Compose.GetReconciliationRequired() {
		t.Fatalf("release result = %#v, want failed reconciliation-required", result)
	}
}

// Rationale: helper completion is not restoration evidence; an applicable
// compensation without its exact typed proof keeps the failed operation gated.
func TestExecuteReleaseCompletedCompensationWithoutExactProofRequiresReconciliation(t *testing.T) {
	t.Parallel()
	helper := &releaseExecutionHelper{responses: map[string]*agentpb.ComposeHelperResponse{
		"switch-api": releaseExecutionSuccess("api", false),
		"fail-next": {
			Outcome: agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_FAILED, ExitCode: 17,
			Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPONENT_ACTIVATION_FAILED,
		},
		"compensate-api": {
			Outcome:    agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED,
			Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE,
		},
	}}
	plan := &agentpb.ExecutionPlan{
		Operation: agentpb.PlanOperation_PLAN_OPERATION_DEPLOY,
		Steps: []*agentpb.ExecutionStep{
			releaseForwardSwitch("switch-api"), releaseForwardSwitch("fail-next"),
			releaseCompensate("compensate-api", "switch-api"),
		},
	}
	result := runReleaseExecution(t, releaseExecutionPool(t, helper), "task-proofless-compensation", "", plan)
	if result.Terminal != TaskTerminalFailed || !result.Compose.GetReconciliationRequired() {
		t.Fatalf("proofless compensation result = %#v", result)
	}
}

// Rationale: a mutation-capable candidate step cannot reach the host until
// Controller acceptance of its exact running event is returned to the Agent.
func TestExecuteReleaseWaitsForDurableRunningEventBeforeCandidateMutation(t *testing.T) {
	helper := &releaseExecutionHelper{responses: map[string]*agentpb.ComposeHelperResponse{
		"switch-api": releaseExecutionSuccess("api", false),
	}}
	pool := releaseExecutionPool(t, helper)
	plan := &agentpb.ExecutionPlan{
		PlanHash: make([]byte, 32), Operation: agentpb.PlanOperation_PLAN_OPERATION_DEPLOY,
		Steps: []*agentpb.ExecutionStep{releaseForwardSwitch("switch-api")},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	reservation := &taskReservation{assignment: Assignment{
		AssignmentID: "assignment-event-fence", TaskID: "task-event-fence", OperationID: "operation-event-fence",
		Plan: plan, ExecutionEpoch: 2, ExecutionMode: agentpb.TaskExecutionMode_TASK_EXECUTION_MODE_FORWARD,
		Deadline: time.Now().Add(5 * time.Second),
	}, ctx: ctx, cancel: cancel, eventsDurable: true}
	go pool.executeRelease(context.Background(), reservation)

	output := <-pool.Outputs()
	if output.Progress == nil || output.Progress.State != TaskProgressRunning {
		t.Fatalf("first output = %#v, want running event", output)
	}
	helper.mu.Lock()
	if len(helper.calls) != 0 {
		helper.mu.Unlock()
		t.Fatalf("candidate mutation ran before event acceptance: %#v", helper.calls)
	}
	helper.mu.Unlock()
	ack := &agentpb.TaskEventAck{
		TaskId: output.Progress.TaskID, AssignmentId: output.Progress.AssignmentID,
		PlanHash: output.Progress.PlanHash[:], StepId: output.Progress.StepID,
		ExecutionEpoch: 1, Ordinal: output.Progress.Ordinal, State: agentpb.TaskState_TASK_STATE_RUNNING,
	}
	if err := pool.AcceptTaskEventAck(context.Background(), ack); err == nil {
		t.Fatal("accepted stale execution epoch")
	}
	ack.ExecutionEpoch = output.Progress.ExecutionEpoch
	if err := pool.AcceptTaskEventAck(context.Background(), ack); err != nil {
		t.Fatalf("AcceptTaskEventAck() error = %v", err)
	}

	for {
		output = <-pool.Outputs()
		if output.Result != nil {
			break
		}
	}
	helper.mu.Lock()
	defer helper.mu.Unlock()
	if len(helper.calls) != 1 || helper.calls[0].stepID != "switch-api" {
		t.Fatalf("candidate mutation calls = %#v", helper.calls)
	}
}

// Rationale: exact recovery proof is part of the step outcome. A helper's
// completed outcome with mismatched evidence must publish failed progress so
// the Controller cannot advance the recovery cursor.
func TestExecuteReleaseRecoveryProofFailurePublishesFailedProgress(t *testing.T) {
	probe := releaseProbe("probe-api")
	helper := &releaseExecutionHelper{responses: map[string]*agentpb.ComposeHelperResponse{
		"probe-api": releaseExecutionSuccess("api", false),
	}}
	helper.responses["probe-api"].ProxyEvidence.ProxyGeneration = 99
	pool := releaseExecutionPool(t, helper)
	plan := &agentpb.ExecutionPlan{PlanHash: make([]byte, 32), Operation: agentpb.PlanOperation_PLAN_OPERATION_DEPLOY,
		Steps: []*agentpb.ExecutionStep{probe}, Artifacts: []*agentpb.ComposeArtifact{
			releaseTestArtifact("candidate-artifact"), releaseTestArtifact("prior-artifact"),
		}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	reservation := &taskReservation{assignment: Assignment{
		AssignmentID: "assignment-proof-progress", TaskID: "task-proof-progress", OperationID: "operation-proof-progress",
		Plan: plan, ExecutionEpoch: 1, ExecutionMode: agentpb.TaskExecutionMode_TASK_EXECUTION_MODE_RECOVERY_ONLY,
		ReleaseRecoveryDirective: &agentpb.ReleaseRecoveryDirective{
			Phase:   agentpb.ReleaseRecoveryPhase_RELEASE_RECOVERY_PHASE_PROBE,
			StepIds: []string{"probe-api"},
		},
	}, ctx: ctx, cancel: cancel, eventsDurable: true}
	go pool.executeRelease(context.Background(), reservation)
	running := <-pool.Outputs()
	if running.Progress == nil || running.Progress.State != TaskProgressRunning {
		t.Fatalf("first recovery progress = %#v", running)
	}
	failed := <-pool.Outputs()
	if failed.Progress == nil || failed.Progress.State != TaskProgressFailed {
		t.Fatalf("proof failure progress = %#v, want failed", failed)
	}
	ack := &agentpb.TaskEventAck{TaskId: failed.Progress.TaskID, AssignmentId: failed.Progress.AssignmentID,
		PlanHash: failed.Progress.PlanHash[:], StepId: failed.Progress.StepID, ExecutionEpoch: failed.Progress.ExecutionEpoch,
		Ordinal: failed.Progress.Ordinal, State: agentpb.TaskState_TASK_STATE_FAILED}
	if err := pool.AcceptTaskEventAck(context.Background(), ack); err != nil {
		t.Fatalf("AcceptTaskEventAck() error = %v", err)
	}
	terminal := <-pool.Outputs()
	if terminal.Result == nil || terminal.Result.Terminal != TaskTerminalFailed ||
		!terminal.Result.Compose.GetReconciliationRequired() {
		t.Fatalf("proof failure terminal = %#v", terminal)
	}
}

// Rationale: rejection of the durable running event occurs before host
// authority exists, so it terminalizes without invoking the helper or opening
// a reconciliation gate.
func TestExecuteReleaseRejectedRunningEventRemainsTerminalWithoutRecovery(t *testing.T) {
	helper := &releaseExecutionHelper{responses: map[string]*agentpb.ComposeHelperResponse{
		"switch-api": releaseExecutionSuccess("api", false),
	}}
	pool := releaseExecutionPool(t, helper)
	plan := &agentpb.ExecutionPlan{
		PlanHash: make([]byte, 32), Operation: agentpb.PlanOperation_PLAN_OPERATION_DEPLOY,
		Steps: []*agentpb.ExecutionStep{releaseForwardSwitch("switch-api")},
	}
	taskCtx, cancel := context.WithCancel(context.Background())
	reservation := &taskReservation{assignment: Assignment{
		AssignmentID: "assignment-rejected-event", TaskID: "task-rejected-event",
		OperationID: "operation-rejected-event", Plan: plan, ExecutionEpoch: 2,
		ExecutionMode: agentpb.TaskExecutionMode_TASK_EXECUTION_MODE_FORWARD, Deadline: time.Now().Add(5 * time.Second),
	}, ctx: taskCtx, cancel: cancel, eventsDurable: true}
	go pool.executeRelease(context.Background(), reservation)

	output := <-pool.Outputs()
	if output.Progress == nil || output.Progress.State != TaskProgressRunning {
		t.Fatalf("first output = %#v, want running event", output)
	}
	stale := &agentpb.TaskEventAck{
		TaskId: output.Progress.TaskID, AssignmentId: output.Progress.AssignmentID,
		PlanHash: output.Progress.PlanHash[:], StepId: output.Progress.StepID,
		ExecutionEpoch: 1, Ordinal: output.Progress.Ordinal, State: agentpb.TaskState_TASK_STATE_RUNNING,
	}
	if err := pool.AcceptTaskEventAck(context.Background(), stale); err == nil {
		t.Fatal("accepted rejected running-event authority")
	}
	cancel()
	for {
		output = <-pool.Outputs()
		if output.Result == nil {
			continue
		}
		if output.Result.Terminal != TaskTerminalAborted || output.Result.Compose.GetReconciliationRequired() {
			t.Fatalf("rejected running-event result = %#v", output.Result)
		}
		break
	}
	assertReleaseExecutionCalls(t, helper, nil)
}

func TestExecuteReleaseRecreateCompensatesSingletonReplacement(t *testing.T) {
	t.Parallel()
	helper := &releaseExecutionHelper{responses: map[string]*agentpb.ComposeHelperResponse{
		"apply-worker": {
			Schema:     1,
			Outcome:    agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED,
			Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE,
		},
		"fail-next": {
			Outcome: agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_FAILED, ExitCode: 17,
			Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPONENT_ACTIVATION_FAILED,
		},
		"restore-worker": {
			Schema: 1, Outcome: agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED,
			Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE,
			RecreateEvidence: &agentpb.ServiceRecreateEvidence{
				ServiceId:   "worker",
				ReleaseId:   "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV",
				ArtifactId:  "prior-artifact",
				Compensated: true,
				Target:      "singleton",
			},
		},
	}}
	pool := releaseExecutionPool(t, helper)
	plan := &agentpb.ExecutionPlan{
		Operation: agentpb.PlanOperation_PLAN_OPERATION_DEPLOY,
		Steps: []*agentpb.ExecutionStep{
			releaseForwardApply(
				"apply-worker",
			), releaseForwardSwitch("fail-next"), releaseRecreateCompensate("restore-worker", "apply-worker"),
		},
	}
	result := runReleaseExecution(t, pool, "task-recreate", "", plan)
	if result.Terminal != TaskTerminalFailed || result.Compose.GetReconciliationRequired() ||
		len(result.Compose.GetRecreateEvidence()) != 1 || !result.Compose.GetRecreateEvidence()[0].GetCompensated() {
		t.Fatalf("recreate result = %#v", result)
	}
	assertReleaseExecutionCalls(t, helper, []releaseExecutionCall{
		{taskID: "task-recreate", stepID: "apply-worker"},
		{taskID: "task-recreate", stepID: "fail-next"},
		{taskID: "task-recreate", stepID: "restore-worker"},
	})
}

// Rationale: first-deploy switch_back closes only after the exact authority-
// bound candidate absence proof is returned by its compensation step.
func TestExecuteReleaseCandidateAbsenceCompensationClosesWithExactProof(t *testing.T) {
	t.Parallel()
	planHash := make([]byte, 32)
	authorityDigest := make([]byte, 32)
	for index := range authorityDigest {
		authorityDigest[index] = 0x71
	}
	candidates := []*agentpb.CandidateReleaseService{{ServiceId: "api", ReleaseId: "release-api"}}
	compensate := &agentpb.ExecutionStep{
		StepId: "restore-absence", PrerequisiteStepId: "apply-api", TimeoutSeconds: 5,
		Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE,
		Payload: &agentpb.ExecutionStep_CandidateRestorationCompensate{
			CandidateRestorationCompensate: &agentpb.CandidateRestorationCompensate{
				CandidateArtifactId: "candidate-artifact", ServiceId: "api", CandidateReleaseId: "release-api",
			},
		},
	}
	plan := &agentpb.ExecutionPlan{
		PlanHash: planHash, Operation: agentpb.PlanOperation_PLAN_OPERATION_DEPLOY,
		Steps: []*agentpb.ExecutionStep{
			releaseForwardApply("apply-api"),
			releaseForwardSwitch("fail-next"),
			compensate,
		},
		CandidateReleaseProcedure: &agentpb.CandidateReleaseProcedure{Members: []*agentpb.CandidateReleaseMember{{
			ServiceId: "api", CandidateReleaseId: "release-api", CandidateArtifactId: "candidate-artifact",
			CandidateAbsence: &agentpb.CandidateAbsenceRestoration{
				ComposeProjectName: "gp-release", Services: candidates,
			},
		}}},
	}
	assignment := Assignment{
		AssignmentID: "assignment-absence", TaskID: "task-absence", OperationID: "operation-absence",
		Plan: plan, Deadline: time.Now().Add(5 * time.Second), ExecutionEpoch: 1,
		ExecutionMode: agentpb.TaskExecutionMode_TASK_EXECUTION_MODE_FORWARD,
		RestorationAuthority: &agentpb.ReleaseRestorationAuthority{
			PlanHash: planHash, CandidateArtifactId: "candidate-artifact", AuthoritySha256: authorityDigest,
			Candidates: []*agentpb.ReleaseRestorationCandidate{
				{
					ServiceId: "api",
					ReleaseId: "release-api",
					Target:    agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_CANDIDATE_ABSENCE,
				},
			},
		},
	}
	helper := &releaseExecutionHelper{responses: map[string]*agentpb.ComposeHelperResponse{
		"apply-api": {Schema: 1, Outcome: agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED,
			Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE},
		"fail-next": {
			Outcome: agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_FAILED, ExitCode: 17,
			Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPONENT_ACTIVATION_FAILED,
		},
		"restore-absence": {
			Schema: 1, Outcome: agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED,
			Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE,
			CandidateAbsenceEvidence: &agentpb.CandidateAbsenceEvidence{
				AssignmentId: assignment.AssignmentID, PlanHash: planHash, AuthoritySha256: authorityDigest,
				ComposeProjectName: "gp-release", CandidateArtifactId: "candidate-artifact",
				Candidates: candidates, AbsenceProven: true,
			},
		},
	}}
	pool := releaseExecutionPool(t, helper)
	taskCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool.executeRelease(context.Background(), &taskReservation{assignment: assignment, ctx: taskCtx, cancel: cancel})
	var result TaskResult
	for len(pool.outputs) > 0 {
		output := <-pool.outputs
		if output.Result != nil {
			result = *output.Result
		}
	}
	if result.Terminal != TaskTerminalFailed || result.Compose.GetReconciliationRequired() ||
		!result.Compose.GetCandidateAbsenceEvidence().GetAbsenceProven() {
		t.Fatalf("candidate absence result = %#v", result)
	}
	assertReleaseExecutionCalls(t, helper, []releaseExecutionCall{
		{taskID: assignment.TaskID, stepID: "apply-api"},
		{taskID: assignment.TaskID, stepID: "fail-next"},
		{taskID: assignment.TaskID, stepID: "restore-absence"},
	})
}

// Rationale: retry reconstruction cannot replay forward mutations. A fresh
// Agent process executes only recover probes, then enabled compensation.
func TestExecuteReleaseRetryAfterRestartRunsRecoveryOnly(t *testing.T) {
	t.Parallel()
	post := releaseHookStep("post-deploy-api", agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_POST_HOOK)
	post.PrerequisiteStepId = "switch-api"
	plan := &agentpb.ExecutionPlan{
		Operation: agentpb.PlanOperation_PLAN_OPERATION_ROLLBACK,
		Steps: []*agentpb.ExecutionStep{
			releaseForwardSwitch(
				"switch-api",
			), post, releaseProbe("probe-api"), releaseCompensate("compensate-api", "switch-api"),
		},
	}
	for _, taskID := range []string{"task-retry-one", "task-retry-two"} {
		helper := &releaseExecutionHelper{responses: map[string]*agentpb.ComposeHelperResponse{
			"probe-api": releaseExecutionSuccess("api", false), "compensate-api": releaseExecutionSuccess("api", true),
		}}
		result := runReleaseExecution(t, releaseExecutionPool(t, helper), taskID, "task-original", plan)
		if result.Terminal != TaskTerminalCompleted || result.Compose.GetReconciliationRequired() ||
			len(result.Compose.GetProxyEvidence()) != 1 || !result.Compose.GetProxyEvidence()[0].GetCompensated() {
			t.Fatalf("restart retry result = %#v", result)
		}
		assertReleaseExecutionCalls(t, helper, []releaseExecutionCall{
			{taskID: taskID, stepID: "probe-api"}, {taskID: taskID, stepID: "compensate-api"},
		})
	}
}

func TestExecuteReleaseRetryCompensatesOnlyTouchedMember(t *testing.T) {
	t.Parallel()
	apiProbe := releaseProbe("probe-api")
	workerProbe := releaseProbe("probe-worker")
	workerProbe.GetServiceProxyProbe().ServiceId = "worker"
	workerProbe.GetServiceProxyProbe().ExpectedTarget = "blue"
	workerProbe.GetServiceProxyProbe().ReleaseId = "release-worker"
	workerProbe.GetServiceProxyProbe().AlternateTarget = "green"
	workerProbe.GetServiceProxyProbe().AlternateReleaseId = "candidate-worker"
	apiCompensate := releaseCompensate("compensate-api", "switch-api")
	workerCompensate := releaseCompensate("compensate-worker", "switch-worker")
	workerCompensate.GetServiceProxyCompensate().ServiceId = "worker"
	helper := &releaseExecutionHelper{responses: map[string]*agentpb.ComposeHelperResponse{
		"probe-api":         releaseExecutionSuccess("api", false),
		"probe-worker":      releaseExecutionSuccess("worker", false),
		"compensate-api":    releaseExecutionSuccess("api", true),
		"compensate-worker": releaseExecutionSuccess("worker", true),
	}}
	plan := &agentpb.ExecutionPlan{
		Operation: agentpb.PlanOperation_PLAN_OPERATION_DEPLOY,
		Steps: []*agentpb.ExecutionStep{
			apiProbe, workerProbe, apiCompensate, workerCompensate,
		},
	}
	result := runReleaseExecution(t, releaseExecutionPool(t, helper), "task-selective-recovery", "task-original", plan)
	if result.Terminal != TaskTerminalCompleted || result.Compose.GetReconciliationRequired() {
		t.Fatalf("selective recovery result = %#v", result)
	}
	assertReleaseExecutionCalls(t, helper, []releaseExecutionCall{
		{taskID: "task-selective-recovery", stepID: "probe-api"},
		{taskID: "task-selective-recovery", stepID: "probe-worker"},
		{taskID: "task-selective-recovery", stepID: "compensate-api"},
	})
}

func TestExecuteReleaseRestartCompensatesAfterAmbiguousTransitionProbe(t *testing.T) {
	t.Parallel()
	helper := &releaseExecutionHelper{responses: map[string]*agentpb.ComposeHelperResponse{
		"probe-transition": {
			Outcome: agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_FAILED, ExitCode: 19,
			Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPOSE_FAILED,
		},
		"restore-prior": releaseExecutionSuccess("api", true),
	}}
	plan := &agentpb.ExecutionPlan{
		Operation: agentpb.PlanOperation_PLAN_OPERATION_DEPLOY,
		Steps: []*agentpb.ExecutionStep{
			releaseProbe("probe-transition"), releaseCompensate("restore-prior", ""),
		},
	}
	result := runReleaseExecution(t, releaseExecutionPool(t, helper), "task-transition-retry", "task-original", plan)
	if result.Terminal != TaskTerminalFailed || !result.Compose.GetReconciliationRequired() ||
		len(result.Compose.GetProxyEvidence()) != 0 {
		t.Fatalf("transition recovery result = %#v", result)
	}
	assertReleaseExecutionCalls(t, helper, []releaseExecutionCall{
		{taskID: "task-transition-retry", stepID: "probe-transition"},
	})
}

// Rationale: parallel retry Tasks share a WorkerPool but not execution state;
// proxy evidence and step selection stay scoped to the owning Task.
func TestExecuteReleaseConcurrentRetriesKeepEvidenceIsolated(t *testing.T) {
	t.Parallel()
	helper := &releaseExecutionHelper{responses: map[string]*agentpb.ComposeHelperResponse{
		"probe-api": releaseExecutionSuccess("api", false),
	}}
	pool := releaseExecutionPool(t, helper)
	plan := &agentpb.ExecutionPlan{
		Operation: agentpb.PlanOperation_PLAN_OPERATION_DEPLOY,
		Steps: []*agentpb.ExecutionStep{
			releaseForwardSwitch("switch-api"), releaseProbe("probe-api"),
		},
	}
	var wait sync.WaitGroup
	for _, taskID := range []string{"task-a", "task-b"} {
		wait.Add(1)
		go func() {
			defer wait.Done()
			runReleaseExecution(t, pool, taskID, "task-original", plan)
		}()
	}
	wait.Wait()
	helper.mu.Lock()
	defer helper.mu.Unlock()
	seen := map[string]int{}
	for _, call := range helper.calls {
		if call.stepID != "probe-api" {
			t.Fatalf("concurrent retry executed non-recovery step %#v", call)
		}
		seen[call.taskID]++
	}
	if seen["task-a"] != 1 || seen["task-b"] != 1 {
		t.Fatalf("concurrent retry calls = %#v", helper.calls)
	}
}

func releaseExecutionPool(t *testing.T, helper *releaseExecutionHelper) *WorkerPool {
	t.Helper()
	runtime, err := NewComposeRuntime(helper, releaseExecutionObserver{})
	if err != nil {
		t.Fatalf("NewComposeRuntime() error = %v", err)
	}
	pool := NewWorkerPool(64, "/var/lib/groundplane/volumes", nil, nil)
	pool.compose = runtime
	return pool
}

func runReleaseExecution(
	t *testing.T,
	pool *WorkerPool,
	taskID string,
	retryOf string,
	plan *agentpb.ExecutionPlan,
) TaskResult {
	t.Helper()
	plan = proto.Clone(plan).(*agentpb.ExecutionPlan)
	if composeArtifact(plan, "candidate-artifact") == nil {
		plan.Artifacts = append(plan.Artifacts, releaseTestArtifact("candidate-artifact"))
	}
	if composeArtifact(plan, "prior-artifact") == nil {
		plan.Artifacts = append(plan.Artifacts, releaseTestArtifact("prior-artifact"))
	}
	taskCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	reservation := &taskReservation{assignment: Assignment{
		AssignmentID: "assignment-" + taskID, TaskID: taskID, OperationID: "operation-" + taskID,
		RetryOf: retryOf, Plan: plan, Deadline: time.Now().Add(5 * time.Second), ExecutionEpoch: 1,
		ExecutionMode: agentpb.TaskExecutionMode_TASK_EXECUTION_MODE_FORWARD,
	}, ctx: taskCtx, cancel: cancel}
	if retryOf != "" {
		reservation.assignment.ExecutionMode = agentpb.TaskExecutionMode_TASK_EXECUTION_MODE_RECOVERY_ONLY
		stepIDs := make([]string, 0)
		compensateIDs := make([]string, 0)
		for _, step := range plan.GetSteps() {
			if step.GetPolicy() == agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE {
				stepIDs = append(stepIDs, step.GetStepId())
			} else if step.GetPolicy() == agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE {
				compensateIDs = append(compensateIDs, step.GetStepId())
			}
		}
		for index := len(compensateIDs) - 1; index >= 0; index-- {
			stepIDs = append(stepIDs, compensateIDs[index])
		}
		reservation.assignment.ReleaseRecoveryDirective = &agentpb.ReleaseRecoveryDirective{
			StepIds: stepIDs, Phase: agentpb.ReleaseRecoveryPhase_RELEASE_RECOVERY_PHASE_PROBE,
			ApplicableCompensationStepIds: append([]string(nil), stepIDs[len(stepIDs)-len(compensateIDs):]...),
		}
	}
	pool.executeRelease(context.Background(), reservation)
	for len(pool.outputs) > 0 {
		output := <-pool.outputs
		if output.Result != nil && output.Result.TaskID == taskID {
			return *output.Result
		}
	}
	t.Fatalf("release execution for %s emitted no result", taskID)
	return TaskResult{}
}

func releaseForwardSwitch(stepID string) *agentpb.ExecutionStep {
	return &agentpb.ExecutionStep{
		StepId: stepID, TimeoutSeconds: 5,
		Policy:  agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
		Payload: &agentpb.ExecutionStep_ServiceProxySwitch{ServiceProxySwitch: &agentpb.ServiceProxySwitch{}},
	}
}

func releaseForwardApply(stepID string) *agentpb.ExecutionStep {
	return &agentpb.ExecutionStep{
		StepId: stepID, TimeoutSeconds: 5, Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
		Payload: &agentpb.ExecutionStep_ComposeApply{
			ComposeApply: &agentpb.ComposeApply{
				ArtifactId:     "candidate-artifact",
				ServiceIds:     []string{"worker"},
				ForceRecreate:  true,
				NoDependencies: true,
			},
		},
	}
}

func TestComposeRemoveIsCandidateMutation(t *testing.T) {
	step := &agentpb.ExecutionStep{
		Payload: &agentpb.ExecutionStep_ComposeRemove{ComposeRemove: &agentpb.ComposeRemove{}},
	}
	if !candidateMutationStep(step) {
		t.Fatal("ComposeRemove did not require the durable running-event barrier")
	}
}

func TestRecoveryLeaveActiveAllowsProbeProvenNoop(t *testing.T) {
	probe := releaseProbe("probe-api")
	probe.GetServiceProxyProbe().ExpectedTarget = "blue"
	probe.GetServiceProxyProbe().ReleaseId = "release-api"
	probe.GetServiceProxyProbe().AlternateTarget = "green"
	compensate := releaseCompensate("compensate-api", "switch-api")
	compensate.GetServiceProxyCompensate().Enabled = false
	helper := &releaseExecutionHelper{responses: map[string]*agentpb.ComposeHelperResponse{
		"probe-api": releaseExecutionSuccess("api", false),
	}}
	result := runReleaseExecution(
		t,
		releaseExecutionPool(t, helper),
		"task-leave-active-noop",
		"task-original",
		&agentpb.ExecutionPlan{
			Operation: agentpb.PlanOperation_PLAN_OPERATION_DEPLOY, Steps: []*agentpb.ExecutionStep{probe, compensate},
		},
	)
	if result.Terminal != TaskTerminalCompleted || result.Compose.GetReconciliationRequired() {
		t.Fatalf("probe-proven leave_active recovery = %#v", result)
	}
	assertReleaseExecutionCalls(
		t,
		helper,
		[]releaseExecutionCall{{taskID: "task-leave-active-noop", stepID: "probe-api"}},
	)
}

func releaseProbe(stepID string) *agentpb.ExecutionStep {
	return &agentpb.ExecutionStep{
		StepId: stepID, TimeoutSeconds: 5,
		Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE,
		Payload: &agentpb.ExecutionStep_ServiceProxyProbe{ServiceProxyProbe: &agentpb.ServiceProxyProbe{
			CandidateArtifactId: "candidate-artifact", PriorArtifactId: "prior-artifact", ServiceId: "api",
			ExpectedTarget: "green", ProxyGeneration: 1, ConfigSha256: make([]byte, 32), ReleaseId: "prior-api",
			AlternateTarget: "blue", AlternateProxyGeneration: 1,
			AlternateConfigSha256: make([]byte, 32), AlternateReleaseId: "release-api",
		}},
	}
}

func releaseCompensate(stepID string, prerequisite string) *agentpb.ExecutionStep {
	return &agentpb.ExecutionStep{
		StepId: stepID, PrerequisiteStepId: prerequisite, TimeoutSeconds: 5,
		Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE,
		Payload: &agentpb.ExecutionStep_ServiceProxyCompensate{ServiceProxyCompensate: &agentpb.ServiceProxyCompensate{
			CandidateArtifactId: "candidate-artifact", PriorArtifactId: "prior-artifact",
			ServiceId: "api", PriorTarget: "blue", ProxyGeneration: 1, ConfigSha256: make([]byte, 32),
			PriorReleaseId: "release-api", Enabled: true,
		}},
	}
}

func releaseRecreateCompensate(stepID string, prerequisite string) *agentpb.ExecutionStep {
	return &agentpb.ExecutionStep{
		StepId: stepID, PrerequisiteStepId: prerequisite, TimeoutSeconds: 5,
		Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE,
		Payload: &agentpb.ExecutionStep_ServiceRecreateCompensate{
			ServiceRecreateCompensate: &agentpb.ServiceRecreateCompensate{
				Enabled: true, ArtifactId: "prior-artifact", ServiceId: "worker",
				PriorReleaseId: "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV", PriorTarget: "singleton",
			},
		},
	}
}

func releaseExecutionSuccess(serviceID string, compensated bool) *agentpb.ComposeHelperResponse {
	return &agentpb.ComposeHelperResponse{
		Outcome:    agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED,
		Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE,
		ProxyEvidence: &agentpb.ServiceProxyEvidence{
			ServiceId: serviceID, Target: "blue", ProxyGeneration: 1, ConfigSha256: make([]byte, 32),
			ReleaseId: "release-" + serviceID, Compensated: compensated,
		},
	}
}

func releaseTestArtifact(artifactID string) *agentpb.ComposeArtifact {
	service := func(serviceID, name, role, slot, releaseID string, kind agentpb.ComposeServiceRole) *agentpb.ComposeService {
		labels := []*agentpb.LabelPair{
			{Key: "com.groundplane.runtime-role", Value: role},
			{Key: "com.groundplane.release-id", Value: releaseID},
		}
		if slot != "" {
			labels = append(labels, &agentpb.LabelPair{Key: "com.groundplane.slot", Value: slot})
		}
		imageID := fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(releaseID)))
		return &agentpb.ComposeService{ServiceId: serviceID, ComposeName: name, Role: kind, Slot: slot,
			ExpectedReplicas: 1, HasHealthcheck: true, ImageReference: imageID,
			ExpectedLabels: labels}
	}
	return &agentpb.ComposeArtifact{
		ArtifactId:  artifactID,
		ProjectName: "gp-release",
		Services: []*agentpb.ComposeService{
			service(
				"api",
				"api-blue",
				"slot",
				"blue",
				"release-api",
				agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT,
			),
			service(
				"api",
				"api-green",
				"slot",
				"green",
				"prior-api",
				agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT,
			),
			service(
				"worker",
				"worker-blue",
				"slot",
				"blue",
				"release-worker",
				agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT,
			),
			service(
				"worker",
				"worker",
				"singleton",
				"",
				"dep_01ARZ3NDEKTSV4RRFFQ69G5FAV",
				agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON,
			),
		},
	}
}

func releaseObservedContainer(name, serviceID, role, slot, releaseID string) *agentpb.ObservedContainer {
	labels := []*agentpb.LabelPair{
		{Key: "com.groundplane.runtime-role", Value: role},
		{Key: "com.groundplane.release-id", Value: releaseID},
	}
	if slot != "" {
		labels = append(labels, &agentpb.LabelPair{Key: "com.groundplane.slot", Value: slot})
	}
	containerID := fmt.Sprintf("%x", sha256.Sum256([]byte(name)))
	imageID := fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(releaseID)))
	return &agentpb.ObservedContainer{ContainerId: containerID, Name: name, ServiceId: serviceID, Labels: labels,
		ImageReference: imageID,
		ImageId:        imageID,
		State:          agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_RUNNING,
		Health:         agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_HEALTHY}
}

func releaseObservedProject() *agentpb.ObservedProject {
	return &agentpb.ObservedProject{ProjectName: "gp-release", Containers: []*agentpb.ObservedContainer{
		releaseObservedContainer("api-blue", "api", "slot", "blue", "release-api"),
		releaseObservedContainer("api-green", "api", "slot", "green", "prior-api"),
		releaseObservedContainer("worker-blue", "worker", "slot", "blue", "release-worker"),
		releaseObservedContainer("worker", "worker", "singleton", "", "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV"),
	}}
}

func assertReleaseExecutionCalls(t *testing.T, helper *releaseExecutionHelper, want []releaseExecutionCall) {
	t.Helper()
	helper.mu.Lock()
	defer helper.mu.Unlock()
	if len(helper.calls) != len(want) {
		t.Fatalf("release calls = %#v, want %#v", helper.calls, want)
	}
	for index := range want {
		if helper.calls[index] != want[index] {
			t.Fatalf("release calls = %#v, want %#v", helper.calls, want)
		}
	}
}
