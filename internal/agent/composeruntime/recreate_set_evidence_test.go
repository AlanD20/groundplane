package composeruntime

import (
	"context"
	"fmt"
	"testing"
	"time"

	testtaskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type recreateArtifactObserver struct {
	projects            map[string]*agentpb.ObservedProject
	calls               []string
	restorationProject  *agentpb.ObservedProject
	restorationArtifact *agentpb.ComposeArtifact
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
				ownedArtifact, observed, "svc_api", "candidate-release", "singleton", false, nil,
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
// a shared Compose project (SVC-15). Unrelated named containers, networks and volumes
// are not ownership evidence for that service, but selected, unnamed and unknown-kind
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
	artifact.Volumes = []*agentpb.ComposeVolume{{ComposeName: "data", DockerName: "shared-data"}}
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
				artifact, observed, serviceID, "candidate-release", "singleton", false, nil,
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
	assignment, step := sealedRecreateProbeAssignment(t, 1, "blue")
	prior := assignment.Plan.Artifacts[1]
	observer := &recreateArtifactObserver{
		restorationProject: recreateTestProject(prior, step.GetServiceRecreateProbe().PriorReleaseId, 1),
	}
	runtime, err := New(completedComposeHelper(), observer)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := runtime.ExecuteStep(ctx, assignment, step)
	if err != nil || result.RecreateEvidence.GetTarget() != "blue" ||
		result.RecreateEvidence.GetArtifactId() != prior.ArtifactId || !result.RecreateEvidence.GetCompensated() {
		t.Fatalf("blue predecessor evidence = %#v, error = %v", result.RecreateEvidence, err)
	}
	if !proto.Equal(observer.restorationArtifact, prior) || len(observer.calls) != 0 {
		t.Fatal(
			"historical observation did not preserve the exact prior artifact independently of current-plan observation",
		)
	}
}

// Rationale: typed helper evidence cannot close compensation or candidate restoration
// when an independently observed container runs an image other than the sealed reference.
func TestReleaseRestorationRejectsWrongObservedImage(t *testing.T) {
	artifact := recreateTestArtifact("prior-artifact", "prior-release", 1)
	observed := recreateTestProject(artifact, "prior-release", 1)
	observed.Containers[0].ImageReference = "registry.example/api@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if err := releaseRestorationWorkloadTargetProven(
		artifact, observed, "svc_api", "singleton", "prior-release", nil,
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
	assignment, step := sealedRecreateProbeAssignment(t, 2, "singleton")
	candidate, prior := assignment.Plan.Artifacts[0], assignment.Plan.Artifacts[1]
	probe := step.GetServiceRecreateProbe()
	priorObserved := recreateTestProject(prior, probe.PriorReleaseId, 2)
	priorObserved.Collisions = []*agentpb.ObservedCollision{
		{Kind: agentpb.ObservedCollisionKind_OBSERVED_COLLISION_KIND_CONTAINER,
			Name: "caddy-1", ComposeServiceName: "caddy", ServiceId: "cmp_caddy"},
		{Kind: agentpb.ObservedCollisionKind_OBSERVED_COLLISION_KIND_CONTAINER,
			Name: "tunnel-1", ComposeServiceName: "tunnel", ServiceId: "cmp_tunnel"},
		{Kind: agentpb.ObservedCollisionKind_OBSERVED_COLLISION_KIND_NETWORK,
			Name: "gp_net_sibling"},
	}
	observer := &recreateArtifactObserver{projects: map[string]*agentpb.ObservedProject{
		candidate.ArtifactId: recreateTestProject(candidate, probe.CandidateReleaseId, 1),
		prior.ArtifactId:     priorObserved,
	}}
	runtime, err := New(completedComposeHelper(), observer)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	result, err := runtime.ExecuteStep(ctx, assignment, step)
	if err != nil {
		t.Fatalf("executeStep() error = %v", err)
	}
	if result.RecreateEvidence.GetArtifactId() != prior.ArtifactId ||
		!result.RecreateEvidence.GetCompensated() {
		t.Fatalf("recreate evidence = %#v, want sealed prior set", result.RecreateEvidence)
	}
	if !proto.Equal(observer.restorationArtifact, prior) {
		t.Fatal("probe did not first observe the exact historical predecessor")
	}
	if len(observer.calls) != 2 || observer.calls[0] != candidate.ArtifactId ||
		observer.calls[1] != prior.ArtifactId {
		t.Fatalf("observation artifacts = %v, want candidate then prior", observer.calls)
	}
}

// Rationale: repairing success fixtures must not make a missing seal, missing
// selection, changed witness, or unrelated step sufficient observation authority.
func TestObservedRecreateProbeRejectsInvalidAuthorityBeforeObservation(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testtaskassignment.Assignment, *agentpb.ExecutionStep)
	}{
		{"missing plan seal", func(a *testtaskassignment.Assignment, _ *agentpb.ExecutionStep) { a.Plan.PlanHash = nil }},
		{"missing authority", func(a *testtaskassignment.Assignment, _ *agentpb.ExecutionStep) { a.RestorationAuthority = nil }},
		{"changed witness", func(a *testtaskassignment.Assignment, _ *agentpb.ExecutionStep) {
			a.RestorationAuthority.AppliedPredecessor.ComposeArtifact[0] ^= 1
		}},
		{"unselected step", func(a *testtaskassignment.Assignment, step *agentpb.ExecutionStep) {
			step.StepId = a.Plan.Steps[0].StepId
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			assignment, step := sealedRecreateProbeAssignment(t, 2, "singleton")
			step = proto.CloneOf(step)
			test.mutate(&assignment, step)
			observer := &recreateArtifactObserver{}
			runtime, err := New(completedComposeHelper(), observer)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			result, err := runtime.ExecuteStep(ctx, assignment, step)
			if err == nil || !result.ReconciliationRequired || result.RecreateEvidence != nil ||
				observer.restorationArtifact != nil || len(observer.calls) != 0 {
				t.Fatalf("invalid authority reached observation or produced evidence: result=%#v, err=%v", result, err)
			}
		})
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
		labels := make([]*agentpb.LabelPair, len(artifact.Services[0].ExpectedLabels))
		for index, label := range artifact.Services[0].ExpectedLabels {
			labels[index] = proto.CloneOf(label)
			if label.Key == "com.groundplane.release-id" {
				labels[index].Value = releaseID
			}
		}
		project.Containers = append(project.Containers, &agentpb.ObservedContainer{
			ContainerId:    fmt.Sprintf("%064x", index+1),
			ServiceId:      artifact.Services[0].ServiceId,
			ImageReference: artifact.GetServices()[0].GetImageReference(),
			ImageId:        artifact.GetServices()[0].GetImageReference(),
			State:          agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_RUNNING,
			Health:         agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_HEALTHY,
			Labels:         labels,
		})
	}
	return project
}
