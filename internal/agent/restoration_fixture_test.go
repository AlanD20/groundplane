package agent

import (
	"context"
	"crypto/sha256"
	"fmt"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Retained physical Compose restoration checks use this helper peer. It has no
// worker, assignment queue, progress/receipt owner or execution engine.
type releaseExecutionHelper struct {
	responses map[string]*agentpb.ComposeHelperResponse
}

func (helper *releaseExecutionHelper) Execute(_ context.Context,
	request *agentpb.ComposeHelperRequest,
) (*agentpb.ComposeHelperResponse, error) {
	return helper.responses[request.StepId], nil
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
