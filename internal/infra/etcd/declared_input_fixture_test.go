package etcd

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/compose-spec/compose-go/v2/types"
	"gopkg.in/yaml.v3"
)

// Repository fixtures must contain the same authored resources as their sealed
// runtime; reads no longer use runtime rendering as a substitute for input.
func declaredComposeFixture(projection environmentprojection.EnvironmentComposeProjection) []byte {
	generated := map[string]bool{}
	for _, component := range projection.Components {
		for _, id := range component.Runtime.GeneratedServices {
			generated[id] = true
		}
	}
	project := types.Project{Services: types.Services{}, Networks: types.Networks{}, Volumes: types.Volumes{}}
	for _, record := range projection.DesiredServices {
		s := record.Desired
		if generated[s.ID] {
			continue
		}
		config := types.ServiceConfig{
			Name:       s.Name,
			Image:      s.Image,
			Command:    s.Command,
			Entrypoint: s.Entrypoint,
			WorkingDir: s.WorkingDir,
			User:       s.User,
			Expose:     s.Expose,
			Restart:    s.Restart,
			Networks:   map[string]*types.ServiceNetworkConfig{},
		}
		if s.Replicas > 0 {
			replicas := s.Replicas
			config.Scale = &replicas
		}
		for _, zone := range s.Zones {
			config.Networks[zone] = &types.ServiceNetworkConfig{Aliases: s.Aliases[zone]}
		}
		project.Services[s.Name] = config
	}
	for _, record := range projection.DesiredZones {
		zone := record.Desired
		network := types.NetworkConfig{
			Name:     zone.Name,
			Internal: zone.Internal,
			Ipam:     types.IPAMConfig{Config: []*types.IPAMPool{{Subnet: zone.Subnet}}},
		}
		if zone.AllowGPDNS {
			network.Extensions = types.Extensions{"x-gp-dns": true}
		}
		project.Networks[zone.Name] = network
	}
	for _, volume := range projection.Volumes {
		project.Volumes[volume.Key] = types.VolumeConfig{}
	}
	value, err := yaml.Marshal(project)
	if err != nil {
		panic(err)
	}
	return value
}
