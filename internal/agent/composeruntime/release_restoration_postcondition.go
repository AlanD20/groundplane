package composeruntime

import (
	"bytes"
	"context"
	taskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/common/ids"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workloadimage"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (runtime *Runtime) candidateRestoration(
	ctx context.Context,
	assignment taskassignment.Assignment,
	step *agentpb.ExecutionStep,
) (StepResult, error) {
	result := StepResult{MutationAttempted: step.GetCandidateRestorationCompensate() != nil}
	response, err := runtime.helper.Execute(ctx, &agentpb.ComposeHelperRequest{
		Schema: composeHelperSchema, AssignmentId: assignment.AssignmentID, TaskId: assignment.TaskID,
		OperationId: assignment.OperationID, Plan: assignment.Plan, StepId: step.GetStepId(),
		TimeoutSeconds: taskassignment.RemainingSeconds(
			ctx,
			step.GetTimeoutSeconds(),
		), RestorationAuthority: assignment.RestorationAuthority,
	})
	if err == nil && response.GetOutcome() == agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_RESTORATION_REQUIRED {
		if !validUnrestoredProbeResponse(assignment, step, response) {
			result.ReconciliationRequired = true
			return result, invalidReleaseProbeEvidence()
		}
		result.Diagnostic, result.RestorationRequired = response.GetDiagnostic(), true
		return result, nil
	}
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

func (runtime *Runtime) proxyProcedure(
	ctx context.Context,
	assignment taskassignment.Assignment,
	step *agentpb.ExecutionStep,
) (StepResult, error) {
	result := StepResult{
		MutationAttempted: step.GetServiceProxySwitch() != nil || step.GetServiceProxyCompensate() != nil,
	}
	response, err := runtime.helper.Execute(ctx, &agentpb.ComposeHelperRequest{
		Schema: composeHelperSchema, AssignmentId: assignment.AssignmentID, TaskId: assignment.TaskID,
		OperationId: assignment.OperationID, Plan: assignment.Plan, StepId: step.GetStepId(),
		TimeoutSeconds: taskassignment.RemainingSeconds(ctx, step.GetTimeoutSeconds()),
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
		return result, errs.Newf(
			errs.KindRequestFailed,
			"agent: release proxy procedure failed with diagnostic %s",
			response.GetDiagnostic().String(),
		)
	}
	if response.GetProxyEvidence() != nil {
		result.ProxyEvidence = proto.Clone(response.GetProxyEvidence()).(*agentpb.ServiceProxyEvidence)
	}
	if step.GetServiceProxyProbe() == nil && step.GetServiceProxyCompensate() == nil {
		return result, nil
	}
	return runtime.verifyReleaseRestorationPostcondition(ctx, assignment, step, result, nil)
}

func (runtime *Runtime) verifyReleaseRestorationPostcondition(
	ctx context.Context,
	assignment taskassignment.Assignment,
	step *agentpb.ExecutionStep,
	result StepResult,
	stepErr error,
) (StepResult, error) {
	if stepErr != nil {
		return result, stepErr
	}
	artifact, serviceID, target, releaseID, observationPlan, errorValue := restorationObservationTarget(
		assignment,
		step,
		result,
	)
	if errorValue != nil {
		result.ReconciliationRequired = true
		return result, errorValue
	}
	if artifact == nil { // Candidate absence is proven without a predecessor workload.
		return result, nil
	}
	var observed *agentpb.ObservedProject
	var err error
	if step.GetCandidateRestorationProbe() != nil || step.GetCandidateRestorationCompensate() != nil {
		var observation *executionplan.RestorationObservation
		observation, err = executionplan.NewRestorationObservation(
			assignment.Plan,
			assignment.RestorationAuthority,
			step.GetStepId(),
		)
		if err == nil {
			observed, err = runtime.observer.ObserveRestoration(ctx, observation)
		}
	} else {
		observed, err = runtime.observer.ObserveReleaseRestoration(ctx, observationPlan, step.GetStepId(), artifact.GetArtifactId())
	}
	result.Observed = observed
	if err == nil {
		err = releaseRestorationWorkloadTargetProven(artifact, observed, serviceID, target, releaseID,
			executionplan.RestorationCandidateWorkloads(assignment.Plan, serviceID))
	}
	if err != nil {
		result.ReconciliationRequired = true
		return result, err
	}
	return result, nil
}

func restorationObservationTarget(
	assignment taskassignment.Assignment,
	step *agentpb.ExecutionStep,
	result StepResult,
) (*agentpb.ComposeArtifact, string, string, string, *agentpb.ExecutionPlan, error) {
	if step.GetPolicy() == agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE {
		if !ReleaseRestorationEvidenceProven(assignment, step, result) {
			return nil, "", "", "", nil, errs.New(
				errs.KindInternal,
				"agent: release compensation returned no exact restoration proof",
			)
		}
	} else if _, err := ReleaseProbeEvidenceStatus(assignment, step, result); err != nil {
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
	serviceID := step.GetCandidateRestorationProbe().GetServiceId()
	if serviceID == "" {
		serviceID = step.GetCandidateRestorationCompensate().GetServiceId()
	}
	if executionplan.RestorationTargetForService(
		assignment.RestorationAuthority,
		serviceID,
	) == agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_CANDIDATE_ABSENCE {
		return nil, "", "", "", assignment.Plan, nil
	}
	artifact, target, releaseID, err := openServingPredecessorAuthority(assignment, serviceID)
	if err != nil {
		return nil, "", "", "", nil, err
	}
	return artifact, serviceID, target, releaseID, assignment.Plan, nil
}

func planRestorationTarget(
	plan *agentpb.ExecutionPlan,
	artifactID, serviceID, target, releaseID string,
) (*agentpb.ComposeArtifact, string, string, string, *agentpb.ExecutionPlan, error) {
	artifact := taskassignment.ComposeArtifact(plan, artifactID)
	if artifact == nil {
		return nil, "", "", "", nil, errs.New(errs.KindInternal, "agent: release restoration artifact is absent")
	}
	return artifact, serviceID, target, releaseID, plan, nil
}

func openServingPredecessorAuthority(
	assignment taskassignment.Assignment,
	serviceID string,
) (*agentpb.ComposeArtifact, string, string, error) {
	var encoded []byte
	for _, witness := range assignment.RestorationAuthority.GetNativePredecessors() {
		if witness.GetServiceId() != serviceID {
			continue
		}
		if len(encoded) != 0 || len(witness.GetCurrentArtifact()) == 0 {
			return nil, "", "", errs.New(
				errs.KindInternal,
				"agent: native serving predecessor authority is incomplete",
			)
		}
		encoded = witness.GetCurrentArtifact()
	}
	if len(encoded) == 0 {
		return nil, "", "", errs.New(errs.KindInternal, "agent: serving predecessor authority is incomplete")
	}
	if err := executionplan.ValidateNativePredecessorWitness(
		assignment.RestorationAuthority.GetEnvironmentId(), serviceID, encoded,
		nativeServingPredecessorRetainedArtifact(assignment.RestorationAuthority, serviceID),
	); err != nil {
		return nil, "", "", errs.New(errs.KindInternal, "agent: native serving predecessor authority is invalid")
	}
	artifact := &agentpb.ComposeArtifact{}
	if (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(encoded, artifact) != nil ||
		executionplan.RejectUnknown(artifact) != nil {
		return nil, "", "", errs.New(errs.KindInternal, "agent: serving predecessor artifact is invalid")
	}
	canonical, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil || !bytes.Equal(canonical, encoded) {
		return nil, "", "", errs.New(errs.KindInternal, "agent: serving predecessor artifact is not canonical")
	}
	var target, releaseID string
	for _, service := range artifact.GetServices() {
		if service.GetServiceId() != serviceID ||
			service.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY ||
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

func nativeServingPredecessorRetainedArtifact(authority *agentpb.ReleaseRestorationAuthority, serviceID string) []byte {
	for _, witness := range authority.GetNativePredecessors() {
		if witness.GetServiceId() == serviceID {
			return witness.GetRetainedPriorArtifact()
		}
	}
	return nil
}

func candidateServingPredecessorEvidenceMatches(
	assignment taskassignment.Assignment,
	serviceID string,
	result StepResult,
) bool {
	artifact, target, releaseID, err := openServingPredecessorAuthority(assignment, serviceID)
	if err != nil {
		return false
	}
	var proxy *agentpb.ComposeService
	for _, service := range artifact.GetServices() {
		if service.GetServiceId() == serviceID &&
			service.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
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
	return releaseRestorationWorkloadTargetProven(artifact, observed, serviceID, "", "", nil)
}

func releaseRestorationWorkloadTargetProven(
	artifact *agentpb.ComposeArtifact,
	observed *agentpb.ObservedProject,
	serviceID, target, releaseID string,
	candidateWorkloads []*agentpb.ComposeService,
) error {
	if artifact == nil || observed == nil || artifact.GetProjectName() == "" ||
		observed.GetProjectName() != artifact.GetProjectName() {
		return errs.New(errs.KindStateConflict, "agent: release restoration observation identity diverges")
	}
	expected := make([]*agentpb.ComposeService, 0, 2)
	known := make([]*agentpb.ComposeService, 0, 3)
	for _, service := range artifact.GetServices() {
		if service.GetServiceId() != serviceID ||
			service.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
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
	known = append(known, candidateWorkloads...)
	if scoped := scopeLifecycleComposeObservation(artifact, observed, known); len(scoped.GetCollisions()) != 0 {
		return errs.New(errs.KindStateConflict, "agent: release restoration observation identity diverges")
	}
	for _, service := range expected {
		if service.GetExpectedReplicas() < 1 || !workloadimage.LocalIDValid(service.GetImageReference()) ||
			!service.GetHasHealthcheck() {
			return errs.New(errs.KindInternal, "agent: sealed predecessor workload health authority is incomplete")
		}
	}
	counts := make([]uint32, len(expected))
	containerIDs := make(map[string]struct{})
	for _, container := range observed.GetContainers() {
		if container.GetServiceId() != serviceID ||
			observedLabel(container, "com.groundplane.runtime-role") == "proxy" {
			continue
		}
		containerID := container.GetContainerId()
		if len(containerID) != 64 || !ids.ValidContainerID(containerID) {
			return errs.New(errs.KindStateConflict, "agent: predecessor workload container identity is invalid")
		}
		if _, duplicate := containerIDs[containerID]; duplicate {
			return errs.New(errs.KindStateConflict, "agent: predecessor workload container identity is duplicated")
		}
		containerIDs[containerID] = struct{}{}
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
			if matchesKnownRestorationWorkload(container, known) {
				continue
			}
		}
		if matched == -1 || container.GetImageReference() != expected[matched].GetImageReference() ||
			container.GetImageId() != expected[matched].GetImageReference() ||
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

func matchesKnownRestorationWorkload(container *agentpb.ObservedContainer, known []*agentpb.ComposeService) bool {
	for _, service := range known {
		if containerHasExpectedLabels(container, service.GetExpectedLabels()) &&
			workloadimage.LocalIDValid(service.GetImageReference()) &&
			container.GetImageReference() == service.GetImageReference() && container.GetImageId() == service.GetImageReference() {
			return true
		}
	}
	return false
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
