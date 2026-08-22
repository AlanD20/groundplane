package controller

import (
	"sort"
	"strings"

	composetypes "github.com/compose-spec/compose-go/v2/types"

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
		serviceNames := make(map[string]string, len(projection.Services))
		for _, service := range projection.Services {
			serviceNames[service.ID] = service.Name
		}
		if project.Networks == nil {
			project.Networks = make(composetypes.Networks)
		}
		external := make([]ComposeResourceIdentity, 0, len(input.NetworkJoins))
		for _, join := range input.NetworkJoins {
			composeName := "gp_attach_" + strings.ToLower(join.NetworkID)
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
				service, exists := project.Services[serviceName]
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
				project.Services[serviceName] = service
			}
		}
		sort.Slice(external, func(i, j int) bool { return external[i].Name < external[j].Name })
		return external, nil
	}
}
