package etcd

import (
	"crypto/sha256"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func withTestEnvironmentComposeArtifact(projection EnvironmentComposeProjection) EnvironmentComposeProjection {
	canonicalYAML := []byte("services: {}\n")
	digest := sha256.Sum256(canonicalYAML)
	componentOwners := make(map[string]string)
	for _, component := range projection.Components {
		if !component.Desired.Enabled {
			continue
		}
		for _, serviceID := range component.Runtime.GeneratedServices {
			if componentOwners[serviceID] == "" {
				componentOwners[serviceID] = component.Desired.ID
			}
		}
	}
	services := make([]*agentpb.ComposeService, len(projection.DesiredServices))
	for index, service := range projection.DesiredServices {
		services[index] = &agentpb.ComposeService{
			ServiceId:        service.Desired.ID,
			ComposeName:      service.Desired.Name,
			OwnerComponentId: componentOwners[service.Desired.ID],
		}
	}
	volumes := make([]*agentpb.ComposeVolume, len(projection.Volumes))
	for index, volume := range projection.Volumes {
		volumes[index] = &agentpb.ComposeVolume{VolumeId: volume.ID, ComposeName: volume.Key}
	}
	value, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&agentpb.ComposeArtifact{
		ArtifactId: "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		OwnerKind:  agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId:    projection.EnvironmentID, ProjectName: "groundplane-test",
		CanonicalYaml: canonicalYAML, YamlSha256: digest[:],
		AuthorizedVolumeDir: "/var/lib/groundplane/vol/test",
		Services:            services, Volumes: volumes,
	})
	if err != nil {
		panic(err)
	}
	projection.ComposeArtifact = value
	projection.NormalizedCompose = append([]byte(nil), canonicalYAML...)
	return projection
}
