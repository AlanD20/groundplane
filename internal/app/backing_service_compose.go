package app

import (
	"path/filepath"
	"strings"

	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/common/backingendpoint"
	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

func backingComposeProject(
	spec adapters.CreationSpec,
	serviceID string,
	image string,
	zone core.Zone,
	volume core.Volume,
	environment etcd.EnvironmentRecord,
) *composetypes.Project {
	environmentFile := controller.EnvFileName(environment.ID)
	return &composetypes.Project{
		Name: "groundplane-backing",
		Services: composetypes.Services{spec.ServiceName: {
			Name: spec.ServiceName, Image: image, Command: composetypes.ShellCommand(backingComposeShell(spec.Command)),
			Expose: composetypes.StringOrNumberList(spec.Expose), Restart: "unless-stopped",
			Networks: map[string]*composetypes.ServiceNetworkConfig{
				zone.Name: {Aliases: []string{backingendpoint.New(serviceID)}},
			},
			Volumes: []composetypes.ServiceVolumeConfig{
				{Type: "volume", Source: volume.Key, Target: spec.MountPath},
			},
			EnvFiles: []composetypes.EnvFile{
				{Path: filepath.Join(environment.VolumeDir, filepath.FromSlash(environmentFile)), Required: true},
			},
			HealthCheck: &composetypes.HealthCheckConfig{
				Test: composetypes.HealthCheckTest(backingComposeShell(spec.HealthCommand)),
			},
		}},
		Networks: composetypes.Networks{
			zone.Name: {
				Internal: zone.Internal,
				Ipam:     composetypes.IPAMConfig{Config: []*composetypes.IPAMPool{{Subnet: zone.Subnet}}},
			},
		},
		Volumes: composetypes.Volumes{volume.Key: {}},
	}
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
