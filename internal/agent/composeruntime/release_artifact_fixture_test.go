package composeruntime

import (
	sha256 "crypto/sha256"
	fmt "fmt"

	agentpb "github.com/AlanD20/groundplane/proto/agentpb"
)

func releaseTestArtifact(artifactID string) *agentpb.ComposeArtifact {
	service := func(serviceID, name, role, slot, releaseID string, kind agentpb.ComposeServiceRole) *agentpb.ComposeService {
		labels := []*agentpb.LabelPair{
			{Key: "com.groundplane.runtime-role", Value: role},
			{Key: "com.groundplane.release-id", Value: releaseID},
		}
		if slot != "" {
			labels = append(labels, &agentpb.LabelPair{Key: "com.groundplane.slot", Value: slot})
		}
		imageID := fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(releaseID)))
		return &agentpb.ComposeService{ServiceId: serviceID, ComposeName: name, Role: kind, Slot: slot,
			ExpectedReplicas: 1, HasHealthcheck: true, ImageReference: imageID,
			ExpectedLabels: labels}
	}
	return &agentpb.ComposeArtifact{
		ArtifactId:  artifactID,
		ProjectName: "gp-release",
		Services: []*agentpb.ComposeService{
			service(
				"api",
				"api-blue",
				"slot",
				"blue",
				"release-api",
				agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT,
			),
			service(
				"api",
				"api-green",
				"slot",
				"green",
				"prior-api",
				agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT,
			),
			service(
				"worker",
				"worker-blue",
				"slot",
				"blue",
				"release-worker",
				agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT,
			),
			service(
				"worker",
				"worker",
				"singleton",
				"",
				"dep_01ARZ3NDEKTSV4RRFFQ69G5FAV",
				agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON,
			),
		},
	}
}
