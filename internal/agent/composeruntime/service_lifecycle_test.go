package composeruntime

import (
	"errors"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func TestServiceLifecyclePostconditionsRequireExactFootprintAndRejectTargetCollision(t *testing.T) {
	serviceID := "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	artifact := &agentpb.ComposeArtifact{ProjectName: "gp-env", Services: []*agentpb.ComposeService{
		{
			ServiceId: serviceID, ComposeName: "api", ExpectedReplicas: 1,
			ExpectedLabels: serviceLifecycleLabels(serviceID, "proxy"),
		},
		{
			ServiceId: serviceID, ComposeName: "api--singleton", ExpectedReplicas: 2,
			ExpectedLabels: serviceLifecycleLabels(serviceID, "singleton"),
		},
	}}
	source := &agentpb.ServiceLifecycleSource{ServiceId: serviceID, ComposeNames: []string{"api", "api--singleton"}}
	observed := &agentpb.ObservedProject{ProjectName: "gp-env", Containers: []*agentpb.ObservedContainer{
		serviceLifecycleContainer(serviceID, "proxy", agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_EXITED),
		serviceLifecycleContainer(
			serviceID,
			"singleton",
			agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_EXITED,
		),
		serviceLifecycleContainer(
			serviceID,
			"singleton",
			agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_EXITED,
		),
	}}
	if err := serviceLifecycleStopped(artifact, source, observed); err != nil {
		t.Fatalf("exact stopped footprint rejected: %v", err)
	}
	partial := proto.Clone(observed).(*agentpb.ObservedProject)
	partial.Containers = partial.Containers[:2]
	if err := serviceLifecycleStopped(artifact, source, partial); !errors.Is(
		err,
		errs.New(errs.KindStateConflict, ""),
	) {
		t.Fatalf("partial footprint error = %v", err)
	}
	collision := proto.Clone(observed).(*agentpb.ObservedProject)
	collision.Collisions = []*agentpb.ObservedCollision{{
		Kind:               agentpb.ObservedCollisionKind_OBSERVED_COLLISION_KIND_CONTAINER,
		ComposeServiceName: "api--singleton", ServiceId: serviceID,
	}}
	if err := serviceLifecycleRemoved(artifact, source, collision); !errors.Is(
		err,
		errs.New(errs.KindStateConflict, ""),
	) {
		t.Fatalf("target collision error = %v", err)
	}
}

func serviceLifecycleLabels(serviceID, role string) []*agentpb.LabelPair {
	return []*agentpb.LabelPair{
		{Key: "com.groundplane.runtime-role", Value: role},
		{Key: "com.groundplane.service-id", Value: serviceID},
	}
}

func serviceLifecycleContainer(
	serviceID, role string,
	state agentpb.ObservedContainerState,
) *agentpb.ObservedContainer {
	return &agentpb.ObservedContainer{
		ServiceId: serviceID, State: state, Labels: serviceLifecycleLabels(serviceID, role),
	}
}
