package agent

import (
	"context"
	"testing"
	"time"

	testcomposeruntime "github.com/AlanD20/groundplane/internal/agent/composeruntime"
	testtaskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/infra/docker/composehelper"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type negativeProbeObserver struct{ t *testing.T }

func (observer negativeProbeObserver) Observe(
	context.Context,
	*agentpb.ExecutionPlan,
	string,
) (*agentpb.ObservedProject, error) {
	observer.t.Fatal("an unrestored probe attempted to prove a healthy predecessor")
	return nil, nil
}

// Rationale: a negative probe advances to an existing obligation, not an
// invented one. Failed compensation must still keep recovery nonterminal.
func TestServingNegativeProbeExecutesOnlyDeclaredCompensation(t *testing.T) {
	for _, applicable := range []bool{false, true} {
		assignment, probe := servingProbeAssignment(t)
		member := assignment.Plan.CandidateReleaseProcedure.Members[0]
		compensate := &agentpb.ExecutionStep{StepId: member.ServingPredecessor.CompensateStepId, TimeoutSeconds: 30,
			Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE,
			Payload: &agentpb.ExecutionStep_CandidateRestorationCompensate{
				CandidateRestorationCompensate: &agentpb.CandidateRestorationCompensate{
					ServiceId: member.ServiceId, CandidateReleaseId: member.CandidateReleaseId, CandidateArtifactId: member.CandidateArtifactId,
				},
			}}
		assignment.Plan.Steps = []*agentpb.ExecutionStep{probe, compensate}
		assignment.ExecutionMode, assignment.ExecutionEpoch = agentpb.TaskExecutionMode_TASK_EXECUTION_MODE_RECOVERY_ONLY, 2
		assignment.ReleaseRecoveryDirective = &agentpb.ReleaseRecoveryDirective{
			Phase: agentpb.ReleaseRecoveryPhase_RELEASE_RECOVERY_PHASE_PROBE, StepIds: []string{probe.StepId, compensate.StepId},
		}
		if applicable {
			assignment.ReleaseRecoveryDirective.ApplicableCompensationStepIds = []string{compensate.StepId}
		}
		helper := &releaseExecutionHelper{responses: map[string]*agentpb.ComposeHelperResponse{
			probe.StepId: {
				Schema:     composehelper.SchemaVersion,
				Outcome:    agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_RESTORATION_REQUIRED,
				Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE,
			},
			compensate.StepId: {
				Schema:     composehelper.SchemaVersion,
				Outcome:    agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_FAILED,
				ExitCode:   1,
				Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPOSE_FAILED,
			},
		}}
		pool := releaseExecutionPool(t, helper)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		pool.executeRelease(ctx, &taskReservation{assignment: assignment, ctx: ctx, cancel: cancel})
		cancel()
		var result *TaskResult
		for len(pool.outputs) > 0 {
			output := <-pool.outputs
			if output.Result != nil {
				result = output.Result
			}
		}
		if result == nil || result.Terminal != TaskTerminalFailed || !result.Compose.GetReconciliationRequired() {
			t.Fatal("unproven compensation falsely closed recovery")
		}
		calls := []releaseExecutionCall{{taskID: assignment.TaskID, stepID: probe.StepId}}
		if applicable {
			calls = append(calls, releaseExecutionCall{taskID: assignment.TaskID, stepID: compensate.StepId})
		}
		assertReleaseExecutionCalls(t, helper, calls)
	}
}

// Rationale: an owned but unrestored serving probe must reach the sealed
// compensation decision without fabricating successful predecessor evidence.
func TestServingProbeRequiresDeclaredCompensationWithoutSuccessEvidence(t *testing.T) {
	assignment, step := servingProbeAssignment(t)
	response := &agentpb.ComposeHelperResponse{Schema: composehelper.SchemaVersion,
		Outcome:    agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_RESTORATION_REQUIRED,
		Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE}
	runtime, err := testcomposeruntime.New(
		&releaseExecutionHelper{responses: map[string]*agentpb.ComposeHelperResponse{step.StepId: response}},
		negativeProbeObserver{t},
	)
	if err != nil {
		t.Fatal(err)
	}
	result, err := runtime.ExecuteStep(context.Background(), assignment, step)
	if err != nil {
		t.Fatalf("owned negative probe failed before compensation: %v", err)
	}
	required, err := testcomposeruntime.ReleaseProbeEvidenceStatus(assignment, step, result)
	if err != nil || !required || result.MutationAttempted || result.ReconciliationRequired {
		t.Fatalf("negative probe did not require compensation: required=%t err=%v", required, err)
	}
	compensate := proto.CloneOf(step)
	compensate.Policy = agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE
	compensate.Payload = &agentpb.ExecutionStep_CandidateRestorationCompensate{
		CandidateRestorationCompensate: &agentpb.CandidateRestorationCompensate{
			ServiceId: step.GetCandidateRestorationProbe().ServiceId, CandidateReleaseId: step.GetCandidateRestorationProbe().CandidateReleaseId,
			CandidateArtifactId: step.GetCandidateRestorationProbe().CandidateArtifactId,
		},
	}
	if testcomposeruntime.ReleaseRestorationEvidenceProven(assignment, compensate, result) {
		t.Fatal("negative observation became restoration proof")
	}
}

// Rationale: the new result is legal only for the exact selected serving probe,
// never for compensation, another member, an absence target, or mixed evidence.
func TestServingNegativeProbeRejectsWrongScopeAndContradictions(t *testing.T) {
	for _, mutate := range []func(*testtaskassignment.Assignment, *agentpb.ExecutionStep, *agentpb.ComposeHelperResponse){
		func(a *testtaskassignment.Assignment, _ *agentpb.ExecutionStep, _ *agentpb.ComposeHelperResponse) {
			a.RestorationAuthority.Candidates[0].Target = agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_CANDIDATE_ABSENCE
		},
		func(_ *testtaskassignment.Assignment, s *agentpb.ExecutionStep, _ *agentpb.ComposeHelperResponse) {
			s.GetCandidateRestorationProbe().CandidateReleaseId = "dep_01ARZ3NDEKTSV4RRFFQ69G5FAW"
		},
		func(_ *testtaskassignment.Assignment, s *agentpb.ExecutionStep, _ *agentpb.ComposeHelperResponse) {
			s.GetCandidateRestorationProbe().CandidateArtifactId = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAW"
		},
		func(_ *testtaskassignment.Assignment, s *agentpb.ExecutionStep, _ *agentpb.ComposeHelperResponse) {
			s.GetCandidateRestorationProbe().ServiceId = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAW"
		},
		func(_ *testtaskassignment.Assignment, s *agentpb.ExecutionStep, _ *agentpb.ComposeHelperResponse) {
			s.Policy = agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE
		},
		func(_ *testtaskassignment.Assignment, _ *agentpb.ExecutionStep, r *agentpb.ComposeHelperResponse) {
			r.ExitCode = 1
		},
		func(_ *testtaskassignment.Assignment, _ *agentpb.ExecutionStep, r *agentpb.ComposeHelperResponse) {
			r.ProxyEvidence = &agentpb.ServiceProxyEvidence{}
		},
		func(_ *testtaskassignment.Assignment, _ *agentpb.ExecutionStep, r *agentpb.ComposeHelperResponse) {
			r.RecreateEvidence = &agentpb.ServiceRecreateEvidence{}
		},
		func(_ *testtaskassignment.Assignment, _ *agentpb.ExecutionStep, r *agentpb.ComposeHelperResponse) {
			r.CandidateAbsenceEvidence = &agentpb.CandidateAbsenceEvidence{}
		},
	} {
		assignment, step := servingProbeAssignment(t)
		response := &agentpb.ComposeHelperResponse{Schema: composehelper.SchemaVersion,
			Outcome:    agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_RESTORATION_REQUIRED,
			Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE}
		mutate(&assignment, step, response)
		runtime, err := testcomposeruntime.New(
			&releaseExecutionHelper{
				responses: map[string]*agentpb.ComposeHelperResponse{step.StepId: response},
			},
			negativeProbeObserver{t},
		)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := runtime.ExecuteStep(context.Background(), assignment, step); err == nil {
			t.Fatal("wrong negative probe scope accepted")
		}
	}
}

func servingProbeAssignment(t *testing.T) (testtaskassignment.Assignment, *agentpb.ExecutionStep) {
	t.Helper()
	assignment, _, _ := nativeServingAssignment(t)
	if err := testtaskassignment.ValidateCandidateReleaseAuthority(assignment, assignment.Plan); err != nil {
		t.Fatal(err)
	}
	member := assignment.Plan.CandidateReleaseProcedure.Members[0]
	return assignment, &agentpb.ExecutionStep{StepId: member.ServingPredecessor.ProbeStepId, TimeoutSeconds: 30,
		Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE,
		Payload: &agentpb.ExecutionStep_CandidateRestorationProbe{
			CandidateRestorationProbe: &agentpb.CandidateRestorationProbe{
				ServiceId: member.ServiceId, CandidateReleaseId: member.CandidateReleaseId, CandidateArtifactId: member.CandidateArtifactId,
			},
		}}
}
