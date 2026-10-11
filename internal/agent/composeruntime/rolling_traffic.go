package composeruntime

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/rollingrelease"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (runtime *Runtime) rollingTrafficSwitch(ctx context.Context, assignment taskassignment.Assignment,
	step *agentpb.ExecutionStep,
) (StepResult, error) {
	result := StepResult{}
	switching := step.GetRollingTrafficSwitch()
	member, err := executionplan.RollingMember(assignment.Plan, switching.GetServiceId())
	if err != nil {
		return result, err
	}
	plan, err := executionplan.OpenRollingPlan(member)
	if err != nil {
		return result, err
	}
	changes, err := plan.Changes()
	ordinal := switching.GetChangeOrdinal()
	if err != nil || ordinal == 0 || int(ordinal) > len(changes) {
		return result, errs.New(
			errs.KindValidationFailed,
			"agent: rolling traffic change is absent from the captured plan",
		)
	}
	change := changes[ordinal-1]
	result.Observed, err = runtime.rollingTargetsReady(ctx, assignment.Plan, plan.ServiceID, change.After)
	if err != nil {
		result.ReconciliationRequired = true
		return result, err
	}
	if !member.GetRolling().GetHasProxy() {
		return result, nil // A portless worker has no traffic activation side effect.
	}
	result.MutationAttempted = true
	response, err := runtime.helper.Execute(ctx, &agentpb.ComposeHelperRequest{
		Schema: composeHelperSchema, AssignmentId: assignment.AssignmentID, TaskId: assignment.TaskID,
		OperationId: assignment.OperationID, Plan: assignment.Plan, StepId: step.GetStepId(),
		TimeoutSeconds: taskassignment.RemainingSeconds(ctx, step.GetTimeoutSeconds()),
	})
	if response != nil {
		result.ExitCode, result.Diagnostic = response.GetExitCode(), response.GetDiagnostic()
	}
	if err != nil {
		result.ReconciliationRequired = true
		return result, err
	}
	expected := member.GetRolling().GetChanges()[ordinal-1]
	evidence := response.GetRollingProxyEvidence()
	if response.GetSchema() != composeHelperSchema ||
		response.GetOutcome() != agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED ||
		response.GetExitCode() != 0 ||
		response.GetDiagnostic() != agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE ||
		evidence.GetServiceId() != plan.ServiceID ||
		evidence.GetChangeOrdinal() != ordinal ||
		evidence.GetProxyGeneration() != expected.GetProxyGeneration() ||
		!bytes.Equal(evidence.GetConfigSha256(), expected.GetProxyConfigSha256()) {
		result.ReconciliationRequired = true
		return result, errs.New(errs.KindStateConflict, "agent: rolling proxy did not confirm the selected replica set")
	}
	result.Observed, err = runtime.rollingTargetsReady(ctx, assignment.Plan, plan.ServiceID, change.After)
	result.ReconciliationRequired = err != nil
	return result, err
}

// Check every traffic recipient, including retained old replicas. Neither a
// previous health step nor an accepted proxy reload proves they remain ready.
func (runtime *Runtime) rollingTargetsReady(ctx context.Context, plan *agentpb.ExecutionPlan,
	serviceID string, replicas []rollingrelease.Replica,
) (*agentpb.ObservedProject, error) {
	observations := make(map[string]*agentpb.ObservedProject, 2)
	var latest *agentpb.ObservedProject
	for _, replica := range replicas {
		artifact := taskassignment.ComposeArtifact(plan, replica.ArtifactID)
		workload, err := executionplan.RollingReplicaService(artifact, serviceID, replica)
		if err != nil {
			return nil, err
		}
		observed := observations[replica.ArtifactID]
		if observed == nil {
			observed, err = runtime.observer.Observe(ctx, plan, replica.ArtifactID)
			if err != nil {
				return nil, err
			}
			observations[replica.ArtifactID] = observed
		}
		convergence, err := evaluateRollingReplicaConvergence(artifact, observed, workload, replica)
		if err != nil {
			return observed, err
		}
		if !convergence.Ready {
			return observed, errs.Newf(
				errs.KindStateConflict,
				"rolling replica %s is not ready to receive traffic",
				replica.ComposeName,
			)
		}
		latest = observed
	}
	return latest, nil
}
