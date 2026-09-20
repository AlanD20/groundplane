package backingservices

import (
	environmentfile "github.com/AlanD20/groundplane/internal/controller/environmentfile"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"path/filepath"
	"strings"

	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/common/backingendpoint"

	"github.com/AlanD20/groundplane/internal/core"

	composetypes "github.com/compose-spec/compose-go/v2/types"
)

func backingComposeProject(
	spec adapters.CreationSpec,
	serviceID string,
	zone core.Zone,
	volume *core.Volume,
	environment hierarchyrecord.EnvironmentRecord,
) *composetypes.Project {
	service := composetypes.ServiceConfig{
		Name: spec.ServiceName, Image: spec.Image, Command: composetypes.ShellCommand(backingComposeShell(spec.Command)),
		Expose: composetypes.StringOrNumberList(spec.Expose), Restart: "unless-stopped",
		Networks: map[string]*composetypes.ServiceNetworkConfig{
			zone.Name: {Aliases: []string{backingendpoint.New(serviceID)}},
		},
	}
	project := &composetypes.Project{
		Name:     "groundplane-backing",
		Services: composetypes.Services{spec.ServiceName: service},
		Networks: composetypes.Networks{
			zone.Name: {
				Internal: zone.Internal,
				Ipam:     composetypes.IPAMConfig{Config: []*composetypes.IPAMPool{{Subnet: zone.Subnet}}},
			},
		},
	}
	if volume != nil {
		service.Volumes = []composetypes.ServiceVolumeConfig{
			{Type: "volume", Source: volume.Key, Target: spec.MountPath},
		}
		project.Volumes = composetypes.Volumes{volume.Key: {}}
	}
	if spec.HasEnvironment() {
		environmentFile := environmentfile.EnvFileName(environment.ID)
		service.EnvFiles = []composetypes.EnvFile{
			{Path: filepath.Join(environment.VolumeDir, filepath.FromSlash(environmentFile)), Required: true},
		}
	}
	if spec.HasHealthcheck() {
		service.HealthCheck = &composetypes.HealthCheckConfig{
			Test: composetypes.HealthCheckTest(backingComposeShell(spec.HealthCommand)),
		}
	}
	project.Services[spec.ServiceName] = service
	return project
}

// Compiled adapter commands are runtime shell inputs, never Compose template
// inputs. Escape once at this serialization boundary without changing the spec.
func backingComposeShell(command []string) []string {
	if command == nil {
		return nil
	}
	escaped := make([]string, len(command))
	for index, argument := range command {
		escaped[index] = strings.ReplaceAll(argument, "$", "$$")
	}
	return escaped
}
