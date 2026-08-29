package agent

import (
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

func TestEvaluateReleaseWorkloadConvergenceIgnoresUnrelatedProjectCollisions(t *testing.T) {
	artifact := convergenceArtifact()
	observed := &agentpb.ObservedProject{
		ProjectName: artifact.ProjectName,
		Containers:  []*agentpb.ObservedContainer{healthyContainer("api-1", "svc_api")},
		Collisions: []*agentpb.ObservedCollision{{
			Kind: agentpb.ObservedCollisionKind_OBSERVED_COLLISION_KIND_NETWORK,
			Name: "unrelated-prior-plan-network",
		}},
	}
	artifact.Services[0].ExpectedReplicas = 1

	result, err := evaluateReleaseWorkloadConvergence(artifact, observed, []string{"api"})
	if err != nil {
		t.Fatalf("evaluateReleaseWorkloadConvergence() error = %v", err)
	}
	if !result.Ready {
		t.Fatalf("evaluateReleaseWorkloadConvergence() = %#v, want selected workload ready", result)
	}
}

func TestEvaluateReleaseWorkloadConvergenceAcceptsRunningServiceWithoutHealthcheck(t *testing.T) {
	artifact := convergenceArtifact()
	artifact.Services[0].ExpectedReplicas = 1
	artifact.Services[0].HasHealthcheck = false
	container := healthyContainer("api-1", "svc_api")
	container.Health = agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_NONE
	observed := &agentpb.ObservedProject{
		ProjectName: artifact.ProjectName,
		Containers:  []*agentpb.ObservedContainer{container},
	}

	result, err := evaluateReleaseWorkloadConvergence(artifact, observed, []string{"api"})
	if err != nil || !result.Ready {
		t.Fatalf("release convergence = %#v, %v", result, err)
	}
	if _, err := evaluateComposeConvergence(artifact, observed, []string{"api"}); err == nil {
		t.Fatal("WaitHealthy accepted a service without a healthcheck")
	}
}
