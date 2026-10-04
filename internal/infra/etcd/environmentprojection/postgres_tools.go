package environmentprojection

import (
	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Tool selection is derived release identity, never authored Compose input.
func PostgresToolsImages(projection EnvironmentComposeProjection) (map[string]string, error) {
	artifact := &agentpb.ComposeArtifact{}
	if proto.Unmarshal(projection.ComposeArtifact, artifact) != nil {
		return nil, CorruptEnvironmentComposeProjection()
	}
	images := make(map[string]string)
	for _, service := range artifact.Services {
		if service.PostgresToolsImage == "" {
			continue
		}
		if _, err := postgres16protocol.ToolsDirectory(service.PostgresToolsImage); err != nil {
			return nil, err
		}
		if existing := images[service.ServiceId]; existing != "" && existing != service.PostgresToolsImage {
			return nil, CorruptEnvironmentComposeProjection()
		}
		images[service.ServiceId] = service.PostgresToolsImage
	}
	return images, nil
}
