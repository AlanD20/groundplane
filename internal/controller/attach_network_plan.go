package controller

import (
	"sort"
	"strings"

	composetypes "github.com/compose-spec/compose-go/v2/types"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type environmentComposeTransform func(
	*composetypes.Project,
	etcd.EnvironmentComposeProjection,
) ([]ComposeResourceIdentity, error)

func attachNetworkTransform(input etcd.AttachTaskRenderInput) environmentComposeTransform {
	return func(
		project *composetypes.Project,
		projection etcd.EnvironmentComposeProjection,
	) ([]ComposeResourceIdentity, error) {
		return ProjectAttachNetworks(project, projection, input.NetworkJoins)
	}
}

// ProjectAttachNetworks replaces the reserved Attach overlay on an owned
// Compose project with the supplied complete network-membership union.
func ProjectAttachNetworks(
	project *composetypes.Project,
	projection etcd.EnvironmentComposeProjection,
	joins []etcd.AttachTaskNetworkJoin,
) ([]ComposeResourceIdentity, error) {
	if project == nil || ids.Validate(ids.KindEnvironment, projection.EnvironmentID) != nil {
		return nil, errs.New(errs.KindInternal, "Attach network projection input is invalid")
	}
	removeManagedAttachNetworks(project)
	identities, err := ComposeIdentitySnapshotFromProjection(projection)
	if err != nil {
		return nil, err
	}
	serviceNames := make(map[string]string, len(identities.Services))
	for _, service := range identities.Services {
		serviceNames[service.ID] = service.Name
	}
	if project.Networks == nil {
		project.Networks = make(composetypes.Networks)
	}
	external := make([]ComposeResourceIdentity, 0, len(joins))
	for _, join := range joins {
		composeName := "gp_attach_" + strings.ToLower(join.NetworkID)
		if ids.Validate(ids.KindNetwork, join.NetworkID) != nil {
			return nil, errs.New(errs.KindInternal, "Attach network projection contains an invalid network")
		}
		if _, exists := project.Networks[composeName]; exists {
			return nil, errs.New(
				errs.KindValidationFailed,
				"managed Attach network conflicts with authored Compose",
			)
		}
		project.Networks[composeName] = composetypes.NetworkConfig{External: true}
		external = append(external, ComposeResourceIdentity{ID: join.NetworkID, Name: composeName})
		for _, serviceID := range join.ServiceIDs {
			serviceName, exists := serviceNames[serviceID]
			if !exists {
				return nil, errs.New(
					errs.KindInternal,
					"Attach network consumer is absent from the pinned projection",
				)
			}
			service, active := project.Services[serviceName]
			if !active {
				service, exists = project.DisabledServices[serviceName]
			}
			if !exists {
				return nil, errs.New(errs.KindInternal, "Attach network consumer is absent from the Blueprint")
			}
			if service.NetworkMode != "" {
				return nil, errs.New(
					errs.KindValidationFailed,
					"Attach network conflicts with service network_mode",
				)
			}
			if service.Networks == nil {
				service.Networks = make(map[string]*composetypes.ServiceNetworkConfig)
			}
			if _, exists := service.Networks[composeName]; exists {
				return nil, errs.New(errs.KindValidationFailed, "managed Attach network membership is duplicated")
			}
			service.Networks[composeName] = &composetypes.ServiceNetworkConfig{}
			if active {
				project.Services[serviceName] = service
			} else {
				project.DisabledServices[serviceName] = service
			}
		}
	}
	sort.Slice(external, func(i, j int) bool { return external[i].Name < external[j].Name })
	return external, nil
}

func removeManagedAttachNetworks(project *composetypes.Project) {
	managed := make(map[string]struct{})
	for name := range project.Networks {
		if _, ok := managedAttachNetworkID(name); ok {
			managed[name] = struct{}{}
			delete(project.Networks, name)
		}
	}
	removeMemberships := func(services composetypes.Services) {
		for name, service := range services {
			if len(service.Networks) == 0 {
				continue
			}
			networks := make(map[string]*composetypes.ServiceNetworkConfig, len(service.Networks))
			for network, config := range service.Networks {
				if _, remove := managed[network]; !remove {
					networks[network] = config
				}
			}
			service.Networks = networks
			services[name] = service
		}
	}
	removeMemberships(project.Services)
	removeMemberships(project.DisabledServices)
}

func managedAttachExternalNetworks(project *composetypes.Project) ([]ComposeResourceIdentity, error) {
	external := make([]ComposeResourceIdentity, 0)
	for name, network := range project.Networks {
		networkID, managed := managedAttachNetworkID(name)
		if !managed {
			continue
		}
		if !network.External {
			return nil, errs.New(errs.KindInternal, "managed Attach network is not external")
		}
		external = append(external, ComposeResourceIdentity{ID: networkID, Name: name})
	}
	sort.Slice(external, func(left int, right int) bool { return external[left].Name < external[right].Name })
	return external, nil
}

func managedAttachNetworkID(composeName string) (string, bool) {
	const prefix = "gp_attach_net_"
	if !strings.HasPrefix(composeName, prefix) {
		return "", false
	}
	networkID := "net_" + strings.ToUpper(strings.TrimPrefix(composeName, prefix))
	return networkID, ids.Validate(ids.KindNetwork, networkID) == nil
}
