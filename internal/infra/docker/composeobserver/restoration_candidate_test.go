package composeobserver

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"google.golang.org/protobuf/proto"
)

// Rationale: SVC-09 a candidate created before failure is not foreign to its
// recovery. Exact candidate evidence must survive observation; incomplete or
// wrong ownership must still collide, and ordinary observation stays strict.
func TestRestorationObservationRecognizesOnlySealedCandidate(t *testing.T) {
	plan := observerPlan(t)
	prior := plan.Artifacts[0]
	candidate := proto.CloneOf(prior.Services[0])
	candidate.ComposeName = "api--green"
	candidate.ExpectedLabels = append(candidate.ExpectedLabels,
		&agentpb.LabelPair{Key: "com.groundplane.release-id", Value: "candidate"},
		&agentpb.LabelPair{Key: "com.groundplane.runtime-role", Value: "slot"})
	for _, test := range []struct {
		name, key, value string
		collision        bool
	}{
		{name: "owned candidate"},
		{name: "wrong plan", key: "com.groundplane.plan-id", value: "foreign", collision: true},
		{name: "wrong release", key: "com.groundplane.release-id", value: "foreign", collision: true},
		{name: "missing owner", key: "com.groundplane.managed", collision: true},
		{name: "unknown workload", key: composeServiceLabel, value: "api--unknown", collision: true},
		{name: "proxy cannot masquerade", key: "com.groundplane.runtime-role", value: "proxy", collision: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			labels := map[string]string{
				composeProjectLabel: prior.ProjectName,
				composeServiceLabel: candidate.ComposeName,
			}
			for _, label := range candidate.ExpectedLabels {
				labels[label.Key] = label.Value
			}
			if test.key != "" {
				labels[test.key] = test.value
			}
			engine := &fakeEngine{containers: []container.Summary{{ID: "candidate", Labels: labels}},
				inspects: map[string]client.ContainerInspectResult{"candidate": {Container: container.InspectResponse{
					ID: "candidate", Name: "/api--green-1", Image: candidate.ImageReference,
					Config: &container.Config{Image: candidate.ImageReference, Labels: labels},
					State:  &container.State{Status: container.StateExited},
				}}}}
			observer, err := NewWithEngine(engine)
			if err != nil {
				t.Fatal(err)
			}
			ordinary, err := observer.Observe(context.Background(), plan, prior.ArtifactId)
			if err != nil || len(ordinary.GetCollisions()) != 1 {
				t.Fatalf("ordinary observation lost collision: %v %v", ordinary, err)
			}
			observed, err := observer.observeArtifact(context.Background(), prior, []*agentpb.ComposeService{candidate})
			if err != nil {
				t.Fatal(err)
			}
			if test.collision {
				if len(observed.Collisions) != 1 || len(observed.Containers) != 0 {
					t.Fatal("foreign container admitted")
				}
			} else if len(observed.Collisions) != 0 || len(observed.Containers) != 1 ||
				observed.Containers[0].State != agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_EXITED {
				t.Fatalf("candidate evidence lost or reclassified: %v", observed)
			}
		})
	}
}
