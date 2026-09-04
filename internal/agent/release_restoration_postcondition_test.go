package agent

import (
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: helper evidence cannot prove restored runtime. The independent
// Compose observation must contain the complete sealed predecessor replica set
// with exact lineage and health before recovery can close.
func TestReleaseRestorationWorkloadSetRequiresExactHealthyLineage(t *testing.T) {
	labels := []*agentpb.LabelPair{
		{Key: "com.groundplane.release-id", Value: "prior-api"},
		{Key: "com.groundplane.runtime-role", Value: "singleton"},
	}
	artifact := &agentpb.ComposeArtifact{
		ArtifactId: "prior-artifact", ProjectName: "gp-release",
		Services: []*agentpb.ComposeService{{
			ServiceId: "api", ComposeName: "api", ExpectedReplicas: 2, HasHealthcheck: true,
			Role: agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON, ExpectedLabels: labels,
		}},
	}
	container := func(name string) *agentpb.ObservedContainer {
		return &agentpb.ObservedContainer{
			ContainerId: name, Name: name, ServiceId: "api",
			State:  agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_RUNNING,
			Health: agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_HEALTHY,
			Labels: proto.Clone(&agentpb.ObservedContainer{Labels: labels}).(*agentpb.ObservedContainer).GetLabels(),
		}
	}
	valid := &agentpb.ObservedProject{
		ProjectName: "gp-release", Containers: []*agentpb.ObservedContainer{container("api-1"), container("api-2")},
	}
	tests := []struct {
		name   string
		mutate func(*agentpb.ObservedProject)
		wantOK bool
	}{
		{name: "complete healthy set", wantOK: true},
		{name: "unhealthy predecessor", mutate: func(value *agentpb.ObservedProject) {
			value.Containers[1].Health = agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_UNHEALTHY
		}},
		{name: "missing predecessor", mutate: func(value *agentpb.ObservedProject) {
			value.Containers = value.Containers[:1]
		}},
		{name: "extra predecessor", mutate: func(value *agentpb.ObservedProject) {
			value.Containers = append(value.Containers, container("api-3"))
		}},
		{name: "mixed lineage", mutate: func(value *agentpb.ObservedProject) {
			value.Containers[1].Labels[0].Value = "candidate-api"
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			observed := proto.Clone(valid).(*agentpb.ObservedProject)
			if test.mutate != nil {
				test.mutate(observed)
			}
			err := releaseRestorationWorkloadSetProven(artifact, observed, "api")
			if (err == nil) != test.wantOK {
				t.Fatalf("releaseRestorationWorkloadSetProven() error = %v, want success %t", err, test.wantOK)
			}
		})
	}
}
