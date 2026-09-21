package agent

import (
	fmt "fmt"

	agentpb "github.com/AlanD20/groundplane/proto/agentpb"
	proto "google.golang.org/protobuf/proto"
)

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
