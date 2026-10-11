package composeruntime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"slices"

	"github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/rollingrelease"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (runtime *Runtime) acknowledgeRollingRelease(ctx context.Context, assignment taskassignment.Assignment,
	step *agentpb.ExecutionStep,
) (StepResult, error) {
	result := StepResult{ReconciliationRequired: true}
	serviceID := step.GetRollingReleaseAcknowledge().GetServiceId()
	member, err := executionplan.RollingMember(assignment.Plan, serviceID)
	if err != nil {
		return result, err
	}
	plan, err := executionplan.OpenRollingPlan(member)
	if err != nil {
		return result, err
	}
	prepared, err := executionplan.PrepareCandidateRuntimes(assignment.Plan)
	if err != nil {
		return result, err
	}
	var candidate *executionplan.CandidateRuntime
	for index := range prepared {
		if prepared[index].ServiceID == serviceID {
			candidate = &prepared[index]
		}
	}
	if candidate == nil || candidate.Target != "rolling" || candidate.ReleaseID != plan.ReleaseID {
		return result, errs.New(errs.KindStateConflict, "agent: final rolling runtime was not captured by the plan")
	}
	if member.GetRolling().GetHasProxy() {
		response, err := runtime.helper.Execute(ctx, &agentpb.ComposeHelperRequest{
			Schema: composeHelperSchema, AssignmentId: assignment.AssignmentID, TaskId: assignment.TaskID,
			OperationId: assignment.OperationID, Plan: assignment.Plan, StepId: step.GetStepId(),
			TimeoutSeconds: taskassignment.RemainingSeconds(ctx, step.GetTimeoutSeconds()),
		})
		if response != nil {
			result.ExitCode, result.Diagnostic = response.GetExitCode(), response.GetDiagnostic()
		}
		if err != nil {
			return result, err
		}
		evidence := response.GetRollingProxyEvidence()
		if response.GetSchema() != composeHelperSchema ||
			response.GetOutcome() != agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED ||
			response.GetExitCode() != 0 ||
			response.GetDiagnostic() != agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE ||
			evidence.GetServiceId() != serviceID ||
			int(evidence.GetChangeOrdinal()) != len(member.GetRolling().GetChanges()) ||
			evidence.GetProxyGeneration() != candidate.ProxyGeneration ||
			!bytes.Equal(evidence.GetConfigSha256(), candidate.ProxyConfigSHA256) {
			return result, errs.New(
				errs.KindStateConflict,
				"agent: final rolling proxy configuration was not confirmed",
			)
		}
	}
	if err := runtime.rollingPredecessorsAbsent(ctx, assignment.Plan, plan); err != nil {
		return result, err
	}
	result.Observed, err = runtime.rollingTargetsReady(ctx, assignment.Plan, serviceID, plan.Candidate)
	if err != nil {
		return result, err
	}
	for _, collision := range result.Observed.GetCollisions() {
		// The unchanged proxy can retain its predecessor's ownership labels.
		// The read-only helper just proved that exact proxy's image, ownership
		// and active/restart configuration independently of candidate labels.
		if member.GetRolling().GetHasProxy() &&
			collision.GetKind() == agentpb.ObservedCollisionKind_OBSERVED_COLLISION_KIND_CONTAINER &&
			collision.GetServiceId() == serviceID &&
			rollingProxyComposeName(assignment.Plan, member) == collision.GetComposeServiceName() {
			continue
		}
		if collision.GetKind() != agentpb.ObservedCollisionKind_OBSERVED_COLLISION_KIND_CONTAINER ||
			collision.GetServiceId() == "" || collision.GetServiceId() == serviceID {
			return result, errs.New(
				errs.KindStateConflict,
				"agent: unexpected runtime remains in the completed rolling Service",
			)
		}
	}
	digest := sha256.Sum256(candidate.CurrentArtifact)
	result.RollingEvidence = &agentpb.RollingReleaseEvidence{ServiceId: serviceID, ReleaseId: plan.ReleaseID,
		ArtifactId: member.GetCandidateArtifactId(), RuntimeSha256: slices.Clone(digest[:]), Replicas: uint32(len(plan.Candidate)),
		ProxyGeneration: candidate.ProxyGeneration, ProxyConfigSha256: slices.Clone(candidate.ProxyConfigSHA256)}
	result.ReconciliationRequired = false
	return result, nil
}

func rollingProxyComposeName(plan *agentpb.ExecutionPlan, member *agentpb.CandidateReleaseMember) string {
	artifact := taskassignment.ComposeArtifact(plan, member.GetCandidateArtifactId())
	for _, service := range artifact.GetServices() {
		if service.GetServiceId() == member.GetServiceId() &&
			service.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
			return service.GetComposeName()
		}
	}
	return ""
}

func (runtime *Runtime) rollingPredecessorsAbsent(
	ctx context.Context,
	sealed *agentpb.ExecutionPlan,
	plan rollingrelease.Plan,
) error {
	observations := make(map[string]*agentpb.ObservedProject, 1)
	for _, replica := range append(slices.Clone(plan.Prior), plan.Retained...) {
		artifact := taskassignment.ComposeArtifact(sealed, replica.ArtifactID)
		workload, err := executionplan.RollingReplicaService(artifact, plan.ServiceID, replica)
		if err != nil {
			return err
		}
		observed := observations[replica.ArtifactID]
		if observed == nil {
			observed, err = runtime.observer.Observe(ctx, sealed, replica.ArtifactID)
			if err != nil {
				return err
			}
			observations[replica.ArtifactID] = observed
		}
		if err := selectedReplicaCollisions(observed, workload); err != nil {
			return err
		}
		if len(containersForService(observed.GetContainers(), workload)) != 0 {
			return errs.New(errs.KindStateConflict, "agent: predecessor replica remains after rolling deployment")
		}
	}
	return nil
}
