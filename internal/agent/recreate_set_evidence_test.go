package agent

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type recreateArtifactObserver struct {
	projects map[string]*agentpb.ObservedProject
	calls    []string
}

// Rationale: one recreate acknowledgement represents the complete sealed logical set;
// partial, extra, mixed-lineage, wrong-image, wrong-role, or unhealthy replicas prove nothing.
func TestObservedRecreateSetRequiresExactSealedReplicas(t *testing.T) {
	artifact := recreateTestArtifact("candidate-artifact", "candidate-release", 2)
	valid := recreateTestProject(artifact, "candidate-release", 2)
	tests := []struct {
		name   string
		mutate func(*agentpb.ComposeArtifact, *agentpb.ObservedProject)
		want   bool
	}{
		{name: "exact two replica set", want: true},
		{
			name: "wrong actual image despite matching config",
			mutate: func(_ *agentpb.ComposeArtifact, project *agentpb.ObservedProject) {
				project.Containers[1].ImageId = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
			},
		},
		{name: "missing actual image", mutate: func(_ *agentpb.ComposeArtifact, project *agentpb.ObservedProject) {
			project.Containers[1].ImageId = ""
		}},
		{
			name: "duplicate container identity",
			mutate: func(_ *agentpb.ComposeArtifact, project *agentpb.ObservedProject) {
				project.Containers[1].ContainerId = project.Containers[0].ContainerId
			},
		},
		{
			name: "missing container identity",
			mutate: func(_ *agentpb.ComposeArtifact, project *agentpb.ObservedProject) {
				project.Containers[1].ContainerId = ""
			},
		},
		{
			name: "invalid container identity",
			mutate: func(_ *agentpb.ComposeArtifact, project *agentpb.ObservedProject) {
				project.Containers[1].ContainerId = "not-a-docker-id"
			},
		},
		{
			name: "short alias cannot count as a distinct container",
			mutate: func(_ *agentpb.ComposeArtifact, project *agentpb.ObservedProject) {
				project.Containers[1].ContainerId = project.Containers[0].ContainerId[:12]
			},
		},
		{
			name: "mutable image is not a workload seal",
			mutate: func(artifact *agentpb.ComposeArtifact, project *agentpb.ObservedProject) {
				artifact.Services[0].ImageReference = "api:latest"
				for _, container := range project.Containers {
					container.ImageReference = "api:latest"
					container.ImageId = "api:latest"
				}
			},
		},
		{name: "missing replica", mutate: func(_ *agentpb.ComposeArtifact, project *agentpb.ObservedProject) {
			project.Containers = project.Containers[:1]
		}},
		{name: "extra replica", mutate: func(artifact *agentpb.ComposeArtifact, project *agentpb.ObservedProject) {
			project.Containers = recreateTestProject(artifact, "candidate-release", 3).GetContainers()
		}},
		{name: "mixed release lineage", mutate: func(_ *agentpb.ComposeArtifact, project *agentpb.ObservedProject) {
			project.Containers[1].Labels[0].Value = "prior-release"
		}},
		{name: "mixed image lineage", mutate: func(_ *agentpb.ComposeArtifact, project *agentpb.ObservedProject) {
			project.Containers[1].ImageReference = "registry.example/api@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		}},
		{name: "wrong runtime role", mutate: func(_ *agentpb.ComposeArtifact, project *agentpb.ObservedProject) {
			project.Containers[1].Labels[1].Value = "slot"
		}},
		{name: "unhealthy replica", mutate: func(_ *agentpb.ComposeArtifact, project *agentpb.ObservedProject) {
			project.Containers[1].Health = agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_UNHEALTHY
		}},
		{name: "starting replica", mutate: func(_ *agentpb.ComposeArtifact, project *agentpb.ObservedProject) {
			project.Containers[1].Health = agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_STARTING
		}},
		{name: "missing health status", mutate: func(_ *agentpb.ComposeArtifact, project *agentpb.ObservedProject) {
			project.Containers[1].Health = agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_NONE
		}},
		{name: "created replica", mutate: func(_ *agentpb.ComposeArtifact, project *agentpb.ObservedProject) {
			project.Containers[1].State = agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_CREATED
		}},
		{name: "exited replica", mutate: func(_ *agentpb.ComposeArtifact, project *agentpb.ObservedProject) {
			project.Containers[1].State = agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_EXITED
		}},
		{name: "dead replica", mutate: func(_ *agentpb.ComposeArtifact, project *agentpb.ObservedProject) {
			project.Containers[1].State = agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_DEAD
		}},
		{
			name: "missing sealed healthcheck",
			mutate: func(artifact *agentpb.ComposeArtifact, _ *agentpb.ObservedProject) {
				artifact.Services[0].HasHealthcheck = false
			},
		},
		{
			name: "missing sealed image",
			mutate: func(artifact *agentpb.ComposeArtifact, project *agentpb.ObservedProject) {
				artifact.Services[0].ImageReference = ""
				project.Containers[0].ImageReference = ""
				project.Containers[1].ImageReference = ""
			},
		},
		{name: "zero sealed replicas", mutate: func(artifact *agentpb.ComposeArtifact, _ *agentpb.ObservedProject) {
			artifact.Services[0].ExpectedReplicas = 0
		}},
		{
			name: "stable proxy excluded",
			want: true,
			mutate: func(_ *agentpb.ComposeArtifact, project *agentpb.ObservedProject) {
				project.Containers = append(project.Containers, &agentpb.ObservedContainer{
					ContainerId: "stable-proxy", ServiceId: "svc_api", ImageReference: "caddy:sealed",
					State:  agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_RUNNING,
					Health: agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_HEALTHY,
					Labels: []*agentpb.LabelPair{{Key: "com.groundplane.runtime-role", Value: "proxy"}},
				})
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ownedArtifact := proto.Clone(artifact).(*agentpb.ComposeArtifact)
			observed := proto.Clone(valid).(*agentpb.ObservedProject)
			if test.mutate != nil {
				test.mutate(ownedArtifact, observed)
			}
			evidence := observedRecreateSetEvidence(
				ownedArtifact, observed, "svc_api", "candidate-release", "singleton", false,
			)
			if (evidence != nil) != test.want {
				t.Fatalf("observedRecreateSetEvidence() = %#v, want evidence %t", evidence, test.want)
			}
			if test.want && (evidence.GetServiceId() != "svc_api" || evidence.GetReleaseId() != "candidate-release" ||
				evidence.GetArtifactId() != "candidate-artifact" || evidence.GetTarget() != "singleton" || evidence.GetCompensated()) {
				t.Fatalf("evidence does not identify the sealed candidate: %v", evidence)
			}
		})
	}
}

// Rationale: recreate acknowledgement observes one selected native service in
// a shared Compose project. Unrelated named containers and networks are not
// ownership evidence for that service, but selected, unnamed, and non-container
// collisions must remain fail-closed.
func TestObservedRecreateSetScopesLifecycleCollisions(t *testing.T) {
	const serviceID = "svc_api"
	artifact := recreateTestArtifact("candidate-artifact", "candidate-release", 3)
	artifact.Services = append(artifact.Services, &agentpb.ComposeService{
		ServiceId: serviceID, ComposeName: "api-proxy",
		Role: agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY,
	})
	artifact.Networks = []*agentpb.ComposeNetwork{{
		ComposeName: "frontend", DockerName: "gp_net_frontend",
	}}
	valid := recreateTestProject(artifact, "candidate-release", 3)
	tests := []struct {
		name       string
		collisions []*agentpb.ObservedCollision
		want       bool
	}{
		{
			name: "unrelated named containers",
			collisions: []*agentpb.ObservedCollision{
				{Kind: agentpb.ObservedCollisionKind_OBSERVED_COLLISION_KIND_CONTAINER,
					Name: "caddy-1", ComposeServiceName: "caddy", ServiceId: "cmp_caddy"},
				{Kind: agentpb.ObservedCollisionKind_OBSERVED_COLLISION_KIND_CONTAINER,
					Name: "tunnel-1", ComposeServiceName: "tunnel", ServiceId: "cmp_tunnel"},
			},
			want: true,
		},
		{
			name: "unrelated named network",
			collisions: []*agentpb.ObservedCollision{{
				Kind: agentpb.ObservedCollisionKind_OBSERVED_COLLISION_KIND_NETWORK,
				Name: "gp_net_sibling",
			}},
			want: true,
		},
		{
			name: "selected service id",
			collisions: []*agentpb.ObservedCollision{{
				Kind: agentpb.ObservedCollisionKind_OBSERVED_COLLISION_KIND_CONTAINER,
				Name: "foreign", ComposeServiceName: "other", ServiceId: serviceID,
			}},
		},
		{
			name: "selected service name",
			collisions: []*agentpb.ObservedCollision{{
				Kind: agentpb.ObservedCollisionKind_OBSERVED_COLLISION_KIND_CONTAINER,
				Name: "api-foreign", ComposeServiceName: "api", ServiceId: "cmp_foreign",
			}},
		},
		{
			name: "unnamed container",
			collisions: []*agentpb.ObservedCollision{{
				Kind: agentpb.ObservedCollisionKind_OBSERVED_COLLISION_KIND_CONTAINER,
				Name: "foreign", ServiceId: "cmp_foreign",
			}},
		},
		{
			name: "selected network",
			collisions: []*agentpb.ObservedCollision{{
				Kind: agentpb.ObservedCollisionKind_OBSERVED_COLLISION_KIND_NETWORK,
				Name: "gp_net_frontend",
			}},
		},
		{
			name: "volume",
			collisions: []*agentpb.ObservedCollision{{
				Kind: agentpb.ObservedCollisionKind_OBSERVED_COLLISION_KIND_VOLUME,
				Name: "shared-data",
			}},
		},
		{
			name:       "unknown collision kind",
			collisions: []*agentpb.ObservedCollision{{Name: "unknown"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			observed := proto.Clone(valid).(*agentpb.ObservedProject)
			observed.Collisions = test.collisions
			evidence := observedRecreateSetEvidence(
				artifact, observed, serviceID, "candidate-release", "singleton", false,
			)
			if (evidence != nil) != test.want {
				t.Fatalf("observedRecreateSetEvidence() = %#v, want evidence %t", evidence, test.want)
			}
		})
	}
}

// Rationale: blue-green to recreate recovery restores the exact sealed prior slot;
// it must not reinterpret a blue or green predecessor as a recreate singleton.
func TestObservedRecreateProbeAcceptsSealedBlueGreenPrior(t *testing.T) {
	candidate := recreateTestArtifact("candidate-artifact", "candidate-release", 3)
	prior := recreateTestArtifact("prior-artifact", "prior-release", 1)
	prior.Services[0].Role = agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT
	prior.Services[0].Slot = "blue"
	prior.Services[0].ExpectedLabels[1].Value = "slot"
	prior.Services[0].ExpectedLabels = append(prior.Services[0].ExpectedLabels,
		&agentpb.LabelPair{Key: "com.groundplane.slot", Value: "blue"})
	priorProject := recreateTestProject(prior, "prior-release", 1)
	priorProject.Containers[0].Labels[1].Value = "slot"
	priorProject.Containers[0].Labels = append(priorProject.Containers[0].Labels,
		&agentpb.LabelPair{Key: "com.groundplane.slot", Value: "blue"})
	observer := &recreateArtifactObserver{projects: map[string]*agentpb.ObservedProject{
		"candidate-artifact": recreateTestProject(candidate, "candidate-release", 2),
		"prior-artifact":     priorProject,
	}}
	runtime, err := NewComposeRuntime(completedComposeHelper(), observer)
	if err != nil {
		t.Fatal(err)
	}
	step := &agentpb.ExecutionStep{Payload: &agentpb.ExecutionStep_ServiceRecreateProbe{
		ServiceRecreateProbe: &agentpb.ServiceRecreateProbe{
			CandidateArtifactId: "candidate-artifact", PriorArtifactId: "prior-artifact",
			ServiceId: "svc_api", CandidateReleaseId: "candidate-release", PriorReleaseId: "prior-release",
		},
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	result, err := runtime.executeStep(ctx, Assignment{Plan: &agentpb.ExecutionPlan{
		Artifacts: []*agentpb.ComposeArtifact{candidate, prior},
	}}, step)
	if err != nil || result.RecreateEvidence.GetTarget() != "blue" ||
		result.RecreateEvidence.GetArtifactId() != "prior-artifact" {
		t.Fatalf("blue predecessor evidence = %#v, error = %v", result.RecreateEvidence, err)
	}
}

// Rationale: typed helper evidence cannot close compensation or candidate restoration
// when an independently observed container runs an image other than the sealed reference.
func TestReleaseRestorationRejectsWrongObservedImage(t *testing.T) {
	artifact := recreateTestArtifact("prior-artifact", "prior-release", 1)
	observed := recreateTestProject(artifact, "prior-release", 1)
	observed.Containers[0].ImageReference = "registry.example/api@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if err := releaseRestorationWorkloadTargetProven(
		artifact, observed, "svc_api", "singleton", "prior-release",
	); err == nil {
		t.Fatal("release restoration accepted a container with the wrong image")
	}
}

func (observer *recreateArtifactObserver) Observe(
	_ context.Context,
	_ *agentpb.ExecutionPlan,
	artifactID string,
) (*agentpb.ObservedProject, error) {
	observer.calls = append(observer.calls, artifactID)
	return observer.projects[artifactID], nil
}

// Rationale: candidate and predecessor restoration are distinct sealed alternatives;
// a prior replica set can only be proved from an observation made against its own artifact.
func TestObservedRecreateProbeUsesEachSealedArtifact(t *testing.T) {
	candidate := recreateTestArtifact("candidate-artifact", "candidate-release", 2)
	prior := recreateTestArtifact("prior-artifact", "prior-release", 2)
	prior.Networks = []*agentpb.ComposeNetwork{{ComposeName: "frontend", DockerName: "gp_net_frontend"}}
	plan := &agentpb.ExecutionPlan{Artifacts: []*agentpb.ComposeArtifact{candidate, prior}}
	priorObserved := recreateTestProject(prior, "prior-release", 2)
	priorObserved.Collisions = []*agentpb.ObservedCollision{
		{Kind: agentpb.ObservedCollisionKind_OBSERVED_COLLISION_KIND_CONTAINER,
			Name: "caddy-1", ComposeServiceName: "caddy", ServiceId: "cmp_caddy"},
		{Kind: agentpb.ObservedCollisionKind_OBSERVED_COLLISION_KIND_CONTAINER,
			Name: "tunnel-1", ComposeServiceName: "tunnel", ServiceId: "cmp_tunnel"},
		{Kind: agentpb.ObservedCollisionKind_OBSERVED_COLLISION_KIND_NETWORK,
			Name: "gp_net_sibling"},
	}
	observer := &recreateArtifactObserver{projects: map[string]*agentpb.ObservedProject{
		"candidate-artifact": recreateTestProject(candidate, "candidate-release", 1),
		"prior-artifact":     priorObserved,
	}}
	runtime, err := NewComposeRuntime(completedComposeHelper(), observer)
	if err != nil {
		t.Fatal(err)
	}
	step := &agentpb.ExecutionStep{Payload: &agentpb.ExecutionStep_ServiceRecreateProbe{
		ServiceRecreateProbe: &agentpb.ServiceRecreateProbe{
			CandidateArtifactId: "candidate-artifact", PriorArtifactId: "prior-artifact",
			ServiceId: "svc_api", CandidateReleaseId: "candidate-release", PriorReleaseId: "prior-release",
		},
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	result, err := runtime.executeStep(ctx, Assignment{Plan: plan}, step)
	if err != nil {
		t.Fatalf("executeStep() error = %v", err)
	}
	if result.RecreateEvidence.GetArtifactId() != "prior-artifact" ||
		!result.RecreateEvidence.GetCompensated() {
		t.Fatalf("recreate evidence = %#v, want sealed prior set", result.RecreateEvidence)
	}
	if len(observer.calls) != 2 || observer.calls[0] != "candidate-artifact" ||
		observer.calls[1] != "prior-artifact" {
		t.Fatalf("observation artifacts = %v, want candidate then prior", observer.calls)
	}
}

func recreateTestArtifact(artifactID, releaseID string, replicas uint32) *agentpb.ComposeArtifact {
	return &agentpb.ComposeArtifact{
		ArtifactId: artifactID, ProjectName: "gp-project",
		Services: []*agentpb.ComposeService{{
			ServiceId: "svc_api", ComposeName: "api", ExpectedReplicas: replicas, HasHealthcheck: true,
			Role:           agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON,
			ImageReference: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			ExpectedLabels: []*agentpb.LabelPair{
				{Key: "com.groundplane.release-id", Value: releaseID},
				{Key: "com.groundplane.runtime-role", Value: "singleton"},
			},
		}},
	}
}

func recreateTestProject(
	artifact *agentpb.ComposeArtifact,
	releaseID string,
	replicas int,
) *agentpb.ObservedProject {
	project := &agentpb.ObservedProject{ProjectName: artifact.GetProjectName()}
	for index := 0; index < replicas; index++ {
		project.Containers = append(project.Containers, &agentpb.ObservedContainer{
			ContainerId:    fmt.Sprintf("%064x", index+1),
			ServiceId:      "svc_api",
			ImageReference: artifact.GetServices()[0].GetImageReference(),
			ImageId:        artifact.GetServices()[0].GetImageReference(),
			State:          agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_RUNNING,
			Health:         agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_HEALTHY,
			Labels: []*agentpb.LabelPair{
				{Key: "com.groundplane.release-id", Value: releaseID},
				{Key: "com.groundplane.runtime-role", Value: "singleton"},
			},
		})
	}
	return project
}
