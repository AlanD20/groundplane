package composerender

import (
	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	"github.com/AlanD20/groundplane/internal/controller/composeidentity"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/compose-spec/compose-go/v2/types"
)

// Backup clients are release assets. They never enter normalized operator
// Compose, replace the server binary, or use libraries from the database image.
func projectPostgresBackupTools(project *types.Project, services []*agentpb.ComposeService,
	identities []composeidentity.Resource,
) error {
	selected := make(map[string]string)
	for _, identity := range identities {
		if identity.PostgresToolsImage != "" {
			selected[identity.ID] = identity.PostgresToolsImage
		}
	}
	for _, metadata := range services {
		image := selected[metadata.ServiceId]
		if image == "" || metadata.Role == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
			continue
		}
		directory, err := postgres16protocol.ToolsDirectory(image)
		if err != nil {
			return err
		}
		service := project.Services[metadata.ComposeName]
		mounted := false
		for _, volume := range service.Volumes {
			if volume.Target == postgres16protocol.HelperDirectoryPath {
				if mounted || volume.Type != "bind" || volume.Source != directory || !volume.ReadOnly ||
					volume.Bind == nil || bool(volume.Bind.CreateHostPath) {
					return errs.New(errs.KindValidationFailed, "operator mount collides with PostgreSQL backup tools")
				}
				mounted = true
			}
		}
		if !mounted {
			service.Volumes = append(service.Volumes, types.ServiceVolumeConfig{
				Type: "bind", Source: directory, Target: postgres16protocol.HelperDirectoryPath,
				ReadOnly: true, Bind: &types.ServiceVolumeBind{CreateHostPath: false},
			})
		}
		project.Services[metadata.ComposeName] = service
		metadata.PostgresToolsImage = image
	}
	return nil
}
