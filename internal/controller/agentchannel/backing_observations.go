package agentchannel

import (
	"sort"

	"github.com/AlanD20/groundplane/internal/common/backingruntimefact"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func durableBackingObservations(projects []*agentpb.ObservedProject) []backingruntimefact.Observation {
	var observations []backingruntimefact.Observation
	for _, project := range projects {
		for _, container := range project.GetContainers() {
			labels := make(map[string]string, len(container.GetLabels()))
			for _, pair := range container.GetLabels() {
				labels[pair.GetKey()] = pair.GetValue()
			}
			if labels["com.groundplane.managed"] != "true" || labels["com.groundplane.kind"] != "service" ||
				labels["com.groundplane.tenant-id"] != "" || labels["com.groundplane.component-id"] != "" ||
				labels["com.groundplane.release-id"] != "" || labels["com.groundplane.runtime-role"] != "" ||
				labels["com.groundplane.slot"] != "" {
				continue
			}
			pairs := make([]*agentpb.LabelPair, len(container.Labels))
			for index, pair := range container.Labels {
				pairs[index] = proto.CloneOf(pair)
			}
			observations = append(observations, backingruntimefact.Observation{
				ProjectName: project.ProjectName, ObservedAt: project.GetObservedAt().AsTime().UTC(),
				ContainerID: container.GetContainerId(), ServiceID: container.GetServiceId(),
				ImageReference: container.GetImageReference(), LocalImageID: container.GetImageId(),
				State: container.GetState(), Health: container.GetHealth(), Labels: pairs,
			})
		}
	}
	sort.Slice(observations, func(i, j int) bool {
		return observations[i].ProjectName+"\x00"+observations[i].ContainerID < observations[j].ProjectName+"\x00"+observations[j].ContainerID
	})
	return observations
}
