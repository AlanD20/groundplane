package agent

import (
	"bytes"
	"context"
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (runtime *ComposeRuntime) candidateRestoration(
	ctx context.Context,
	assignment Assignment,
	step *agentpb.ExecutionStep,
) (composeStepResult, error) {
	result := composeStepResult{MutationAttempted: step.GetCandidateRestorationCompensate() != nil}
	response, err := runtime.helper.Execute(ctx, &agentpb.ComposeHelperRequest{
		Schema: composeHelperSchema, AssignmentId: assignment.AssignmentID, TaskId: assignment.TaskID,
		OperationId: assignment.OperationID, Plan: assignment.Plan, StepId: step.GetStepId(),
		TimeoutSeconds: remainingSeconds(ctx, step.GetTimeoutSeconds()), RestorationAuthority: assignment.RestorationAuthority,
	})
	if err != nil || response == nil || response.GetSchema() != composeHelperSchema ||
		response.GetOutcome() != agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED {
		result.ReconciliationRequired = true
		if err != nil {
			return result, err
		}
		return result, errs.New(errs.KindRequestFailed, "agent: candidate restoration proof failed")
	}
	result.ExitCode, result.Diagnostic = response.GetExitCode(), response.GetDiagnostic()
	if response.GetProxyEvidence() != nil {
		result.ProxyEvidence = proto.Clone(response.GetProxyEvidence()).(*agentpb.ServiceProxyEvidence)
	}
	if response.GetRecreateEvidence() != nil {
		result.RecreateEvidence = proto.Clone(response.GetRecreateEvidence()).(*agentpb.ServiceRecreateEvidence)
	}
	if response.GetCandidateAbsenceEvidence() != nil {
		result.CandidateAbsenceEvidence = proto.Clone(response.GetCandidateAbsenceEvidence()).(*agentpb.CandidateAbsenceEvidence)
	}
	return runtime.verifyReleaseRestorationPostcondition(ctx, assignment, step, result, nil)
}

func (runtime *ComposeRuntime) proxyProcedure(
	ctx context.Context,
	assignment Assignment,
	step *agentpb.ExecutionStep,
) (composeStepResult, error) {
	result := composeStepResult{MutationAttempted: step.GetServiceProxySwitch() != nil || step.GetServiceProxyCompensate() != nil}
	response, err := runtime.helper.Execute(ctx, &agentpb.ComposeHelperRequest{
		Schema: composeHelperSchema, AssignmentId: assignment.AssignmentID, TaskId: assignment.TaskID,
		OperationId: assignment.OperationID, Plan: assignment.Plan, StepId: step.GetStepId(),
		TimeoutSeconds: remainingSeconds(ctx, step.GetTimeoutSeconds()),
	})
	if err != nil || response == nil {
		result.ReconciliationRequired = true
		if err != nil {
			return result, err
		}
		return result, errs.New(errs.KindInternal, "agent: release proxy helper returned an invalid response")
	}
	result.ExitCode, result.Diagnostic = response.GetExitCode(), response.GetDiagnostic()
	if response.GetOutcome() != agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED {
		result.ReconciliationRequired = true
		return result, errs.Newf(errs.KindRequestFailed, "agent: release proxy procedure failed with diagnostic %s", response.GetDiagnostic().String())
	}
	if response.GetProxyEvidence() != nil {
		result.ProxyEvidence = proto.Clone(response.GetProxyEvidence()).(*agentpb.ServiceProxyEvidence)
	}
	if step.GetServiceProxyProbe() == nil && step.GetServiceProxyCompensate() == nil {
		return result, nil
	}
	return runtime.verifyReleaseRestorationPostcondition(ctx, assignment, step, result, nil)
}

func (runtime *ComposeRuntime) verifyReleaseRestorationPostcondition(
	ctx context.Context,
	assignment Assignment,
	step *agentpb.ExecutionStep,
	result composeStepResult,
	stepErr error,
) (composeStepResult, error) {
	if stepErr != nil {
		return result, stepErr
	}
	artifact, serviceID, target, releaseID, observationPlan, errorValue := restorationObservationTarget(assignment, step, result)
	if errorValue != nil {
		result.ReconciliationRequired = true
		return result, errorValue
	}
	if artifact == nil { // Candidate absence is proven without a predecessor workload.
		return result, nil
	}
	observed, err := runtime.observer.Observe(ctx, observationPlan, artifact.GetArtifactId())
	result.Observed = observed
	if err == nil {
		err = releaseRestorationWorkloadTargetProven(artifact, observed, serviceID, target, releaseID)
	}
	if err != nil {
		result.ReconciliationRequired = true
		return result, err
	}
	return result, nil
}

func restorationObservationTarget(
	assignment Assignment,
	step *agentpb.ExecutionStep,
	result composeStepResult,
) (*agentpb.ComposeArtifact, string, string, string, *agentpb.ExecutionPlan, error) {
	if step.GetPolicy() == agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE {
		if !releaseRestorationEvidenceProven(assignment, step, result) {
			return nil, "", "", "", nil, errs.New(errs.KindInternal, "agent: release compensation returned no exact restoration proof")
		}
	} else if _, err := releaseProbeEvidenceStatus(assignment, step, result); err != nil {
		return nil, "", "", "", nil, err
	}
	if compensate := step.GetServiceProxyCompensate(); compensate != nil {
		return planRestorationTarget(assignment.Plan, compensate.GetPriorArtifactId(), compensate.GetServiceId(),
			compensate.GetPriorTarget(), compensate.GetPriorReleaseId())
	}
	if probe := step.GetServiceProxyProbe(); probe != nil {
		artifactID, target, releaseID := probe.GetPriorArtifactId(), probe.GetExpectedTarget(), probe.GetReleaseId()
		if result.ProxyEvidence.GetTarget() == probe.GetAlternateTarget() {
			artifactID, target, releaseID = probe.GetCandidateArtifactId(), probe.GetAlternateTarget(), probe.GetAlternateReleaseId()
		}
		return planRestorationTarget(assignment.Plan, artifactID, probe.GetServiceId(), target, releaseID)
	}
	if compensate := step.GetServiceRecreateCompensate(); compensate != nil {
		return planRestorationTarget(assignment.Plan, compensate.GetArtifactId(), compensate.GetServiceId(),
			compensate.GetPriorTarget(), compensate.GetPriorReleaseId())
	}
	if probe := step.GetServiceRecreateProbe(); probe != nil {
		artifactID, releaseID := probe.GetPriorArtifactId(), probe.GetPriorReleaseId()
		if result.RecreateEvidence.GetArtifactId() == probe.GetCandidateArtifactId() {
			artifactID, releaseID = probe.GetCandidateArtifactId(), probe.GetCandidateReleaseId()
		}
		return planRestorationTarget(
			assignment.Plan, artifactID, probe.GetServiceId(), result.RecreateEvidence.GetTarget(), releaseID,
		)
	}
	if assignment.RestorationAuthority.GetTarget() == agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_CANDIDATE_ABSENCE {
		return nil, "", "", "", assignment.Plan, nil
	}
	serviceID := step.GetCandidateRestorationProbe().GetServiceId()
	if serviceID == "" {
		serviceID = step.GetCandidateRestorationCompensate().GetServiceId()
	}
	artifact, target, releaseID, err := openServingPredecessorAuthority(assignment, serviceID)
	if err != nil {
		return nil, "", "", "", nil, err
	}
	plan, err := observationPlanWithArtifact(assignment.Plan, artifact)
	return artifact, serviceID, target, releaseID, plan, err
}

func planRestorationTarget(
	plan *agentpb.ExecutionPlan,
	artifactID, serviceID, target, releaseID string,
) (*agentpb.ComposeArtifact, string, string, string, *agentpb.ExecutionPlan, error) {
	artifact := composeArtifact(plan, artifactID)
	if artifact == nil {
		return nil, "", "", "", nil, errs.New(errs.KindInternal, "agent: release restoration artifact is absent")
	}
	return artifact, serviceID, target, releaseID, plan, nil
}

func openServingPredecessorAuthority(
	assignment Assignment,
	serviceID string,
) (*agentpb.ComposeArtifact, string, string, error) {
	sealed := assignment.RestorationAuthority.GetServingPredecessor()
	if sealed == nil || len(sealed.GetComposeArtifact()) == 0 || len(sealed.GetComposeArtifactSha256()) != sha256.Size {
		return nil, "", "", errs.New(errs.KindInternal, "agent: serving predecessor authority is incomplete")
	}
	digest := sha256.Sum256(sealed.GetComposeArtifact())
	artifact := new(agentpb.ComposeArtifact)
	if !bytes.Equal(digest[:], sealed.GetComposeArtifactSha256()) ||
		(proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(sealed.GetComposeArtifact(), artifact) != nil ||
		executionplan.RejectUnknown(artifact) != nil {
		return nil, "", "", errs.New(errs.KindInternal, "agent: serving predecessor artifact is invalid")
	}
	canonical, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil || !bytes.Equal(canonical, sealed.GetComposeArtifact()) {
		return nil, "", "", errs.New(errs.KindInternal, "agent: serving predecessor artifact is not canonical")
	}
	var target, releaseID string
	for _, service := range artifact.GetServices() {
		if service.GetServiceId() != serviceID || service.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY ||
			restorationReleaseLabel(service) == "" {
			continue
		}
		if releaseID != "" {
			return nil, "", "", errs.New(errs.KindStateConflict, "agent: serving predecessor workload is ambiguous")
		}
		target, releaseID = service.GetSlot(), restorationReleaseLabel(service)
		if target == "" {
			target = "singleton"
		}
	}
	if target == "" || releaseID == "" {
		return nil, "", "", errs.New(errs.KindInternal, "agent: serving predecessor workload is absent")
	}
	return artifact, target, releaseID, nil
}

func observationPlanWithArtifact(plan *agentpb.ExecutionPlan, artifact *agentpb.ComposeArtifact) (*agentpb.ExecutionPlan, error) {
	owned := proto.Clone(plan).(*agentpb.ExecutionPlan)
	owned.PlanHash = nil
	found := false
	for index, current := range owned.GetArtifacts() {
		if current.GetArtifactId() == artifact.GetArtifactId() {
			owned.Artifacts[index], found = proto.Clone(artifact).(*agentpb.ComposeArtifact), true
		}
	}
	if !found {
		owned.Artifacts = append(owned.Artifacts, proto.Clone(artifact).(*agentpb.ComposeArtifact))
	}
	return executionplan.Seal(owned)
}

func candidateServingPredecessorEvidenceMatches(assignment Assignment, serviceID string, result composeStepResult) bool {
	artifact, target, releaseID, err := openServingPredecessorAuthority(assignment, serviceID)
	if err != nil {
		return false
	}
	var proxy *agentpb.ComposeService
	for _, service := range artifact.GetServices() {
		if service.GetServiceId() == serviceID && service.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
			if proxy != nil {
				return false
			}
			proxy = service
		}
	}
	if proxy == nil {
		return exclusiveRecreateEvidence(result) && exactRecreateEvidence(result.RecreateEvidence,
			serviceID, artifact.GetArtifactId(), releaseID, target, true)
	}
	generation, err := executionplan.ProxyConfigGeneration(proxy.GetProxyConfigJson(), releaseID)
	return err == nil && exclusiveProxyEvidence(result) && exactProxyEvidence(result.ProxyEvidence,
		serviceID, target, generation, proxy.GetProxyConfigSha256(), releaseID, true)
}

func releaseRestorationWorkloadSetProven(
	artifact *agentpb.ComposeArtifact,
	observed *agentpb.ObservedProject,
	serviceID string,
) error {
	return releaseRestorationWorkloadTargetProven(artifact, observed, serviceID, "", "")
}

func releaseRestorationWorkloadTargetProven(
	artifact *agentpb.ComposeArtifact,
	observed *agentpb.ObservedProject,
	serviceID, target, releaseID string,
) error {
	if artifact == nil || observed == nil || artifact.GetProjectName() == "" || observed.GetProjectName() != artifact.GetProjectName() || len(observed.GetCollisions()) != 0 {
		return errs.New(errs.KindStateConflict, "agent: release restoration observation identity diverges")
	}
	expected := make([]*agentpb.ComposeService, 0, 2)
	known := make([]*agentpb.ComposeService, 0, 3)
	for _, service := range artifact.GetServices() {
		if service.GetServiceId() != serviceID || service.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
			continue
		}
		known = append(known, service)
		serviceTarget := service.GetSlot()
		if serviceTarget == "" {
			serviceTarget = "singleton"
		}
		if target != "" && serviceTarget != target || releaseID != "" && restorationReleaseLabel(service) != releaseID {
			continue
		}
		expected = append(expected, service)
	}
	if len(expected) == 0 {
		return errs.New(errs.KindInternal, "agent: sealed predecessor workload is absent")
	}
	for _, service := range expected {
		if service.GetExpectedReplicas() < 1 ||
			(target != "" || releaseID != "") && (service.GetImageReference() == "" || !service.GetHasHealthcheck()) {
			return errs.New(errs.KindInternal, "agent: sealed predecessor workload health authority is incomplete")
		}
	}
	counts := make([]uint32, len(expected))
	for _, container := range observed.GetContainers() {
		if container.GetServiceId() != serviceID || observedLabel(container, "com.groundplane.runtime-role") == "proxy" {
			continue
		}
		matched := -1
		for index, service := range expected {
			if containerHasExpectedLabels(container, service.GetExpectedLabels()) {
				if matched != -1 {
					return errs.New(errs.KindStateConflict, "agent: predecessor workload lineage is ambiguous")
				}
				matched = index
			}
		}
		if matched == -1 {
			sealedOther := false
			for _, service := range known {
				sealedOther = sealedOther || containerHasExpectedLabels(container, service.GetExpectedLabels())
			}
			if sealedOther {
				continue
			}
		}
		if matched == -1 || container.GetImageReference() != expected[matched].GetImageReference() ||
			container.GetState() != agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_RUNNING ||
			container.GetHealth() != agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_HEALTHY {
			return errs.New(errs.KindStateConflict, "agent: predecessor workload is foreign or unhealthy")
		}
		counts[matched]++
	}
	for index, service := range expected {
		if counts[index] != service.GetExpectedReplicas() {
			return errs.New(errs.KindStateConflict, "agent: predecessor workload replica count diverges")
		}
	}
	return nil
}

func observedLabel(container *agentpb.ObservedContainer, key string) string {
	for _, label := range container.GetLabels() {
		if label.GetKey() == key {
			return label.GetValue()
		}
	}
	return ""
}

func restorationReleaseLabel(service *agentpb.ComposeService) string {
	for _, label := range service.GetExpectedLabels() {
		if label.GetKey() == "com.groundplane.release-id" {
			return label.GetValue()
		}
	}
	return ""
}

func containerHasExpectedLabels(container *agentpb.ObservedContainer, expected []*agentpb.LabelPair) bool {
	for _, label := range expected {
		if observedLabel(container, label.GetKey()) != label.GetValue() {
			return false
		}
	}
	return true
}
