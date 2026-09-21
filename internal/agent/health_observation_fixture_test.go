package agent

import agentpb "github.com/AlanD20/groundplane/proto/agentpb"

func healthyContainer(name, serviceID string) *agentpb.ObservedContainer {
	return &agentpb.ObservedContainer{
		ContainerId: "sha256:" + name,
		Name:        name,
		ServiceId:   serviceID,
		State:       agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_RUNNING,
		Health:      agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_HEALTHY,
	}
}
