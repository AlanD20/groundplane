package agent

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

const recreateRuntimeRole = "singleton"

type recreateObservationTarget struct {
	artifactID  string
	releaseID   string
	target      string
	compensated bool
}

func (runtime *ComposeRuntime) observeRecreateSet(
	ctx context.Context,
	plan *agentpb.ExecutionPlan,
	candidateArtifactID, priorArtifactID, serviceID, candidateReleaseID, priorReleaseID string,
) (composeStepResult, error) {
	result := composeStepResult{}
	targets := []recreateObservationTarget{
		{artifactID: candidateArtifactID, releaseID: candidateReleaseID, target: recreateRuntimeRole},
		{artifactID: priorArtifactID, releaseID: priorReleaseID, compensated: true},
	}
	ticker := time.NewTicker(composeHealthPollInterval)
	defer ticker.Stop()
	for {
		for _, target := range targets {
			if target.artifactID == "" {
				continue
			}
			observed, err := runtime.observer.Observe(ctx, plan, target.artifactID)
			result.Observed = observed
			if err != nil {
				continue
			}
			result.RecreateEvidence = observedRecreateSetEvidence(
				composeArtifact(plan, target.artifactID), observed, serviceID, target.releaseID,
				target.target, target.compensated,
			)
			if result.RecreateEvidence != nil {
				return result, nil
			}
		}
		select {
		case <-ctx.Done():
			result.ReconciliationRequired = true
			return result, ctx.Err()
		case <-ticker.C:
		}
	}
}

func observedRecreateSetEvidence(
	artifact *agentpb.ComposeArtifact,
	observed *agentpb.ObservedProject,
	serviceID, releaseID, expectedTarget string,
	compensated bool,
) *agentpb.ServiceRecreateEvidence {
	if artifact == nil || observed == nil || artifact.GetArtifactId() == "" || serviceID == "" || releaseID == "" ||
		artifact.GetProjectName() == "" || observed.GetProjectName() != artifact.GetProjectName() ||
		len(observed.GetCollisions()) != 0 {
		return nil
	}
	var workload *agentpb.ComposeService
	target := ""
	for _, service := range artifact.GetServices() {
		serviceTarget, ok := sealedRecreateServiceTarget(service)
		if !ok || service.GetServiceId() != serviceID || restorationReleaseLabel(service) != releaseID ||
			expectedTarget != "" && serviceTarget != expectedTarget {
			continue
		}
		if workload != nil {
			return nil
		}
		workload = service
		target = serviceTarget
	}
	if workload == nil || workload.GetExpectedReplicas() < 1 || workload.GetImageReference() == "" ||
		!workload.GetHasHealthcheck() || !hasCanonicalRecreateRole(workload, target) {
		return nil
	}

	var replicas uint32
	for _, container := range observed.GetContainers() {
		if container.GetServiceId() != serviceID {
			continue
		}
		role := observedLabel(container, "com.groundplane.runtime-role")
		if role == "proxy" {
			continue
		}
		if role != observedRecreateRuntimeRole(target) ||
			!containerHasExpectedLabels(container, workload.GetExpectedLabels()) ||
			container.GetImageReference() != workload.GetImageReference() ||
			container.GetState() != agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_RUNNING ||
			container.GetHealth() != agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_HEALTHY {
			return nil
		}
		replicas++
	}
	if replicas != workload.GetExpectedReplicas() {
		return nil
	}
	return &agentpb.ServiceRecreateEvidence{
		ServiceId: serviceID, ReleaseId: releaseID, ArtifactId: artifact.GetArtifactId(),
		Compensated: compensated, Target: target,
	}
}

func sealedRecreateServiceTarget(service *agentpb.ComposeService) (string, bool) {
	switch service.GetRole() {
	case agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON:
		return recreateRuntimeRole, service.GetSlot() == ""
	case agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT:
		return service.GetSlot(), (service.GetSlot() == "blue" || service.GetSlot() == "green") &&
			service.GetExpectedReplicas() == 1
	default:
		return "", false
	}
}

func observedRecreateRuntimeRole(target string) string {
	if target == recreateRuntimeRole {
		return recreateRuntimeRole
	}
	return "slot"
}

func hasCanonicalRecreateRole(service *agentpb.ComposeService, target string) bool {
	roleMatches, slotMatches := 0, 0
	for _, label := range service.GetExpectedLabels() {
		if label.GetKey() == "com.groundplane.runtime-role" {
			roleMatches++
			if label.GetValue() != observedRecreateRuntimeRole(target) {
				return false
			}
		}
		if label.GetKey() == "com.groundplane.slot" {
			slotMatches++
			if label.GetValue() != target || target == recreateRuntimeRole {
				return false
			}
		}
	}
	return roleMatches == 1 && (target == recreateRuntimeRole && slotMatches == 0 ||
		target != recreateRuntimeRole && slotMatches == 1)
}
