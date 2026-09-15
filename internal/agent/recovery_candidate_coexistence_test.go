package agent

import (
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: SVC-09 a failed candidate can remain beside the serving predecessor.
// It must not block exact recovery or count toward predecessor health; foreign
// ownership, image drift, missing predecessors and duplicate identities still fail.
func TestRecoveryCandidateCoexistsWithoutProvingPredecessorHealth(t *testing.T) {
	prior := recreateTestArtifact("prior-artifact", "prior-release", 1)
	candidate := recreateTestArtifact("candidate-artifact", "candidate-release", 1)
	candidate.Services[0].ComposeName = "api--green"
	candidate.Services[0].ExpectedLabels = append(candidate.Services[0].ExpectedLabels,
		&agentpb.LabelPair{Key: "com.groundplane.plan-id", Value: "candidate-plan"})
	for _, test := range []struct {
		name   string
		mutate func(*agentpb.ObservedProject)
		want   bool
	}{
		{name: "created unhealthy candidate", want: true},
		{name: "wrong candidate image", mutate: func(p *agentpb.ObservedProject) {
			p.Containers[1].ImageId = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		}},
		{name: "wrong candidate release", mutate: func(p *agentpb.ObservedProject) { p.Containers[1].Labels[0].Value = "foreign" }},
		{name: "wrong candidate plan", mutate: func(p *agentpb.ObservedProject) {
			p.Containers[1].Labels[len(p.Containers[1].Labels)-1].Value = "foreign"
		}},
		{name: "missing predecessor", mutate: func(p *agentpb.ObservedProject) { p.Containers = p.Containers[1:] }},
		{name: "unhealthy predecessor", mutate: func(p *agentpb.ObservedProject) {
			p.Containers[0].Health = agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_UNHEALTHY
		}},
		{name: "duplicate identity", mutate: func(p *agentpb.ObservedProject) { p.Containers[1].ContainerId = p.Containers[0].ContainerId }},
		{name: "unidentified collision", mutate: func(p *agentpb.ObservedProject) {
			p.Collisions = []*agentpb.ObservedCollision{{Kind: agentpb.ObservedCollisionKind_OBSERVED_COLLISION_KIND_CONTAINER}}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			observed := recreateTestProject(prior, "prior-release", 1)
			created := recreateTestProject(candidate, "candidate-release", 1).Containers[0]
			created.ContainerId = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
			created.Labels = proto.CloneOf(
				&agentpb.ObservedContainer{Labels: candidate.Services[0].ExpectedLabels},
			).Labels
			created.State = agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_CREATED
			created.Health = agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_UNHEALTHY
			observed.Containers = append(observed.Containers, created)
			if test.mutate != nil {
				test.mutate(observed)
			}
			err := releaseRestorationWorkloadTargetProven(
				prior,
				observed,
				"svc_api",
				"singleton",
				"prior-release",
				candidate.Services,
			)
			if (err == nil) != test.want {
				t.Fatalf("recovery result = %v, want success %t", err, test.want)
			}
		})
	}
}
