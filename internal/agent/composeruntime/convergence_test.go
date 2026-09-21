package composeruntime

import (
	"errors"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func TestEvaluateComposeConvergenceRequiresEveryExpectedHealthyReplica(t *testing.T) {
	artifact := convergenceArtifact()
	observed := &agentpb.ObservedProject{
		ProjectName: artifact.GetProjectName(),
		Containers: []*agentpb.ObservedContainer{
			healthyContainer("ctr_api_1", "svc_api"),
			healthyContainer("ctr_api_2", "svc_api"),
			healthyContainer("ctr_worker_1", "svc_worker"),
		},
	}

	result, err := evaluateComposeConvergence(artifact, observed, nil)
	if err != nil {
		t.Fatalf("evaluateComposeConvergence() error = %v", err)
	}
	if !result.Ready {
		t.Fatalf("evaluateComposeConvergence() = %#v, want ready", result)
	}

	observed.Containers = observed.Containers[1:]
	result, err = evaluateComposeConvergence(artifact, observed, nil)
	if err != nil {
		t.Fatalf("evaluateComposeConvergence() missing replica error = %v", err)
	}
	if result.Ready || !strings.Contains(result.Summary, "1 of 2 expected replicas") {
		t.Fatalf("evaluateComposeConvergence() missing replica = %#v", result)
	}
}

func TestEvaluateComposeConvergenceReportsUnhealthyContainer(t *testing.T) {
	artifact := convergenceArtifact()
	container := healthyContainer("ctr_api_1", "svc_api")
	container.Health = agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_UNHEALTHY
	observed := &agentpb.ObservedProject{
		ProjectName: artifact.GetProjectName(),
		Containers: []*agentpb.ObservedContainer{
			container,
			healthyContainer("ctr_api_2", "svc_api"),
		},
	}

	result, err := evaluateComposeConvergence(artifact, observed, []string{"api"})
	if err != nil {
		t.Fatalf("evaluateComposeConvergence() error = %v", err)
	}
	if result.Ready || !strings.Contains(result.Summary, "is not healthy") {
		t.Fatalf("evaluateComposeConvergence() = %#v, want unhealthy", result)
	}
}

func TestEvaluateComposeConvergenceScopesSelectedServices(t *testing.T) {
	artifact := convergenceArtifact()
	observed := &agentpb.ObservedProject{
		ProjectName: artifact.GetProjectName(),
		Containers: []*agentpb.ObservedContainer{
			healthyContainer("ctr_api_1", "svc_api"),
			healthyContainer("ctr_api_2", "svc_api"),
		},
	}

	result, err := evaluateComposeConvergence(artifact, observed, []string{"api"})
	if err != nil {
		t.Fatalf("evaluateComposeConvergence() error = %v", err)
	}
	if !result.Ready {
		t.Fatalf("evaluateComposeConvergence() = %#v, want selected service ready", result)
	}
}

func TestEvaluateComposeConvergenceDistinguishesReleaseRuntimes(t *testing.T) {
	workloadLabels := []*agentpb.LabelPair{{Key: "com.groundplane.runtime-role", Value: "slot"}}
	proxyLabels := []*agentpb.LabelPair{{Key: "com.groundplane.runtime-role", Value: "proxy"}}
	artifact := &agentpb.ComposeArtifact{
		ProjectName: "groundplane-release",
		Services: []*agentpb.ComposeService{
			{ServiceId: "svc_api", ComposeName: "api", ExpectedReplicas: 1, ExpectedLabels: proxyLabels},
			{
				ServiceId:        "svc_api",
				ComposeName:      "api--blue",
				ExpectedReplicas: 1,
				HasHealthcheck:   true,
				ExpectedLabels:   workloadLabels,
			},
		},
	}
	workload := healthyContainer("ctr_api_blue_1", "svc_api")
	workload.Labels = workloadLabels
	proxy := healthyContainer("ctr_api_proxy_1", "svc_api")
	proxy.Labels = proxyLabels
	observed := &agentpb.ObservedProject{
		ProjectName: artifact.ProjectName,
		Containers:  []*agentpb.ObservedContainer{proxy, workload},
	}

	result, err := evaluateComposeConvergence(artifact, observed, []string{"api--blue"})
	if err != nil {
		t.Fatalf("evaluateComposeConvergence() error = %v", err)
	}
	if !result.Ready {
		t.Fatalf("evaluateComposeConvergence() = %#v, want selected workload ready", result)
	}
}

func TestEvaluateComposeConvergenceRejectsUnsafeWaitInputs(t *testing.T) {
	artifact := convergenceArtifact()
	observed := &agentpb.ObservedProject{ProjectName: artifact.GetProjectName()}

	artifact.Services[0].HasHealthcheck = false
	_, err := evaluateComposeConvergence(artifact, observed, []string{"api"})
	if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("missing healthcheck error = %v, want validation failure", err)
	}

	artifact.Services[0].HasHealthcheck = true
	observed.Collisions = []*agentpb.ObservedCollision{{
		Kind: agentpb.ObservedCollisionKind_OBSERVED_COLLISION_KIND_CONTAINER,
		Name: "api-foreign",
	}}
	_, err = evaluateComposeConvergence(artifact, observed, []string{"api"})
	if !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("collision error = %v, want conflict", err)
	}
}

func convergenceArtifact() *agentpb.ComposeArtifact {
	return &agentpb.ComposeArtifact{
		ArtifactId:  "artifact_platform",
		ProjectName: "groundplane-platform",
		Services: []*agentpb.ComposeService{
			{ServiceId: "svc_api", ComposeName: "api", ExpectedReplicas: 2, HasHealthcheck: true},
			{ServiceId: "svc_worker", ComposeName: "worker", ExpectedReplicas: 1, HasHealthcheck: true},
		},
	}
}

func healthyContainer(name, serviceID string) *agentpb.ObservedContainer {
	return &agentpb.ObservedContainer{
		ContainerId: "sha256:" + name,
		Name:        name,
		ServiceId:   serviceID,
		State:       agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_RUNNING,
		Health:      agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_HEALTHY,
	}
}
