package composeruntime

import (
	"fmt"
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: SVC-15 helper evidence cannot prove restored runtime. The independent
// Compose observation must contain the complete sealed predecessor replica set
// with exact lineage and health before recovery can close. Other Services' named
// Volumes in the shared project must not block proof; required or unknown Volumes do.
func TestReleaseRestorationWorkloadSetRequiresExactHealthyLineage(t *testing.T) {
	const sealedImage = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	labels := []*agentpb.LabelPair{
		{Key: "com.groundplane.release-id", Value: "prior-api"},
		{Key: "com.groundplane.runtime-role", Value: "singleton"},
	}
	artifact := &agentpb.ComposeArtifact{
		ArtifactId: "prior-artifact", ProjectName: "gp-release",
		Volumes: []*agentpb.ComposeVolume{{ComposeName: "data", DockerName: "gp_data"}},
		Services: []*agentpb.ComposeService{{
			ServiceId: "api", ComposeName: "api", ExpectedReplicas: 2, HasHealthcheck: true,
			ImageReference: sealedImage,
			Role:           agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON, ExpectedLabels: labels,
		}, {
			ServiceId: "api", ComposeName: "api-proxy", ExpectedReplicas: 1,
			Role: agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY, ImageReference: "caddy:managed",
		}},
	}
	container := func(index int) *agentpb.ObservedContainer {
		return &agentpb.ObservedContainer{
			ContainerId: fmt.Sprintf("%064x", index), ServiceId: "api",
			ImageReference: sealedImage, ImageId: sealedImage,
			State:  agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_RUNNING,
			Health: agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_HEALTHY,
			Labels: proto.Clone(&agentpb.ObservedContainer{Labels: labels}).(*agentpb.ObservedContainer).GetLabels(),
		}
	}
	valid := &agentpb.ObservedProject{
		ProjectName: "gp-release", Containers: []*agentpb.ObservedContainer{container(1), container(2)},
	}
	tests := []struct {
		name   string
		mutate func(*agentpb.ObservedProject)
		wantOK bool
	}{
		{name: "complete healthy set", wantOK: true},
		{name: "unrelated project volume", wantOK: true, mutate: func(value *agentpb.ObservedProject) {
			value.Collisions = []*agentpb.ObservedCollision{{
				Kind: agentpb.ObservedCollisionKind_OBSERVED_COLLISION_KIND_VOLUME, Name: "gp_other_data",
			}}
		}},
		{name: "required volume collision", mutate: func(value *agentpb.ObservedProject) {
			value.Collisions = []*agentpb.ObservedCollision{{
				Kind: agentpb.ObservedCollisionKind_OBSERVED_COLLISION_KIND_VOLUME, Name: "gp_data",
			}}
		}},
		{name: "unidentified volume collision", mutate: func(value *agentpb.ObservedProject) {
			value.Collisions = []*agentpb.ObservedCollision{{
				Kind: agentpb.ObservedCollisionKind_OBSERVED_COLLISION_KIND_VOLUME,
			}}
		}},
		{name: "wrong actual image despite matching config", mutate: func(value *agentpb.ObservedProject) {
			value.Containers[1].ImageId = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		}},
		{name: "missing actual image", mutate: func(value *agentpb.ObservedProject) {
			value.Containers[1].ImageId = ""
		}},
		{name: "duplicate replica identity", mutate: func(value *agentpb.ObservedProject) {
			value.Containers[1].ContainerId = value.Containers[0].ContainerId
		}},
		{name: "invalid replica identity", mutate: func(value *agentpb.ObservedProject) {
			value.Containers[1].ContainerId = ""
		}},
		{name: "stable proxy has separate image authority", wantOK: true, mutate: func(value *agentpb.ObservedProject) {
			proxy := container(3)
			proxy.ImageReference = "caddy:managed"
			proxy.ImageId = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
			proxy.Labels[1].Value = "proxy"
			value.Containers = append(value.Containers, proxy)
		}},
		{name: "unhealthy predecessor", mutate: func(value *agentpb.ObservedProject) {
			value.Containers[1].Health = agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_UNHEALTHY
		}},
		{name: "missing predecessor", mutate: func(value *agentpb.ObservedProject) {
			value.Containers = value.Containers[:1]
		}},
		{name: "extra predecessor", mutate: func(value *agentpb.ObservedProject) {
			value.Containers = append(value.Containers, container(3))
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
			err = releaseRestorationWorkloadTargetProven(artifact, observed, "api", "singleton", "prior-api", nil)
			if (err == nil) != test.wantOK {
				t.Fatalf("releaseRestorationWorkloadTargetProven() error = %v, want success %t", err, test.wantOK)
			}
		})
	}
}
