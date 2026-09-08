package controller

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

const (
	serviceProxyConfigPath = "/etc/caddy/groundplane-proxy.json"
)

func renderServiceProxyTopology(
	project *composetypes.Project,
	input ComposeRenderInput,
	names []string,
	identities map[string]string,
) ([]*agentpb.ComposeService, error) {
	configs := make(composetypes.Configs, len(project.Configs)+len(names))
	for name, config := range project.Configs {
		configs[name] = config
	}
	project.Configs = configs
	result := make([]*agentpb.ComposeService, 0, len(names)*3)
	for _, name := range names {
		authored, active := input.Project.Services[name]
		if !active {
			authored = input.Project.DisabledServices[name]
		}
		serviceID := identities[name]
		exposures := make([]string, len(authored.Expose))
		for index, exposure := range authored.Expose {
			exposures[index] = fmt.Sprint(exposure)
		}
		for _, published := range authored.Ports {
			if published.Target != 0 {
				exposures = append(exposures, fmt.Sprint(published.Target))
			}
		}
		identity, hasRelease := input.Releases[serviceID]
		if !hasRelease {
			labels, expected, err := composeOwnershipLabels(authored.Labels, "service", serviceID, input)
			if err != nil {
				return nil, err
			}
			authored.Name = name
			authored.Labels = labels
			authored.Extensions, err = composeResourceExtensions(
				authored.Extensions,
				"service",
				serviceID,
				input.EnvironmentID,
			)
			if err != nil {
				return nil, err
			}
			project.Services[name] = authored
			replicas := authored.GetScale()
			if replicas <= 0 {
				return nil, errs.New(errs.KindValidationFailed, "Service replicas must be positive")
			}
			service := &agentpb.ComposeService{
				ServiceId: serviceID, ComposeName: name, ExpectedLabels: expected,
				OwnerComponentId: composeServiceComponentOwner(input.Identities.Services, serviceID),
				ExpectedReplicas: expectedRuntimeReplicas(active, replicas),
				HasHealthcheck:   authored.HealthCheck != nil && !authored.HealthCheck.Disable,
			}
			if err := bindEnvironmentComponentImage(input.Identities.Services, authored, service); err != nil {
				return nil, err
			}
			result = append(result, service)
			continue
		}
		if len(exposures) == 0 {
			if identity.Strategy == domain.StrategyBlueGreen {
				return nil, errs.New(
					errs.KindValidationFailed,
					"blue-green release requires an addressable TCP service",
				)
			}
			if identity.Strategy == domain.StrategyRecreate {
				replicas := authored.GetScale()
				if replicas <= 0 {
					return nil, errs.New(errs.KindValidationFailed, "service replicas must be positive")
				}
				authored.Image = identity.Image
				if identity.Image == "" {
					authored.Image = input.Project.Services[name].Image
					if authored.Image == "" {
						authored.Image = input.Project.DisabledServices[name].Image
					}
				}
				labels, expected, err := composeServiceRuntimeLabels(
					authored.Labels,
					serviceID,
					"singleton",
					"",
					identity.ReleaseID,
					input,
				)
				if err != nil {
					return nil, err
				}
				authored.Name, authored.Labels = name, labels
				authored.Extensions, err = composeResourceExtensions(
					authored.Extensions,
					"service",
					serviceID,
					input.EnvironmentID,
				)
				if err != nil {
					return nil, err
				}
				project.Services[name] = authored
				result = append(result, &agentpb.ComposeService{
					ServiceId: serviceID, ComposeName: name, ExpectedLabels: expected,
					ExpectedReplicas: expectedRuntimeReplicas(active, replicas),
					HasHealthcheck:   authored.HealthCheck != nil && !authored.HealthCheck.Disable,
					ImageReference:   authored.Image,
					Role:             agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON,
					OwnerComponentId: composeServiceComponentOwner(input.Identities.Services, serviceID),
				})
				continue
			}
			return nil, errs.New(errs.KindValidationFailed, "release strategy is unsupported")
		}
		ports, err := domain.ProxyPorts(exposures)
		if err != nil {
			return nil, err
		}
		if identity.Strategy == domain.StrategyBlueGreen && authored.GetScale() != 1 {
			return nil, errs.New(errs.KindValidationFailed, "blue-green release requires exactly one replica")
		}
		activeTarget := identity.ServingTarget
		activeRelease := identity.ServingReleaseID
		activeGeneration := uint64(1)
		if activeTarget == "" {
			activeTarget = domain.WorkloadSingleton
		}
		if activeRelease == "" {
			activeRelease = identity.ReleaseID
		}
		if identity.ServingProxyGeneration != 0 {
			activeGeneration = identity.ServingProxyGeneration
		}
		proxyConfig, err := domain.RenderProxyConfig(name, activeRelease, activeTarget, activeGeneration, ports)
		if err != nil {
			return nil, err
		}
		configName := "gp-proxy-" + strings.ToLower(serviceID)
		if _, collision := project.Configs[configName]; collision {
			return nil, errs.New(
				errs.KindValidationFailed,
				"generated Service proxy config collides with authored Compose config",
			)
		}
		project.Configs[configName] = composetypes.ConfigObjConfig(
			composetypes.FileObjectConfig{Content: string(proxyConfig.JSON)},
		)

		proxy := composetypes.ServiceConfig{
			Name: name, Networks: cloneProxyNetworks(authored.Networks),
			Ports: slices.Clone(authored.Ports), Expose: slices.Clone(authored.Expose), Restart: authored.Restart,
			Profiles: slices.Clone(authored.Profiles),
			Command:  composetypes.ShellCommand{"caddy", "run", "--config", serviceProxyConfigPath},
			Configs: []composetypes.ServiceConfigObjConfig{
				composetypes.ServiceConfigObjConfig(
					composetypes.FileReferenceConfig{Source: configName, Target: serviceProxyConfigPath},
				),
			},
		}
		proxyLabels, proxyExpected, err := composeServiceRuntimeLabels(
			authored.Labels,
			serviceID,
			"proxy",
			"",
			"",
			input,
		)
		if err != nil {
			return nil, err
		}
		proxy.Labels = proxyLabels
		proxy.Extensions, err = composeResourceExtensions(nil, "service", serviceID, input.EnvironmentID)
		if err != nil {
			return nil, err
		}
		proxyService := &agentpb.ComposeService{
			ServiceId: serviceID, ComposeName: name, ExpectedLabels: proxyExpected,
			OwnerComponentId: composeServiceComponentOwner(input.Identities.Services, serviceID),
			ExpectedReplicas: expectedRuntimeReplicas(active, 1),
			Role:             agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY,
			ProxyConfigJson:  slices.Clone(proxyConfig.JSON), ProxyConfigSha256: slices.Clone(proxyConfig.SHA256[:]),
		}
		if err := bindServiceProxyImage(identity.ProxyImage, &proxy, proxyService); err != nil {
			return nil, err
		}
		project.Services[name] = proxy
		result = append(result, proxyService)

		targets := []domain.WorkloadTarget{domain.WorkloadSingleton}
		if identity.Strategy == domain.StrategyBlueGreen {
			targets = []domain.WorkloadTarget{domain.WorkloadBlue, domain.WorkloadGreen}
		}
		for _, target := range targets {
			workloadName, err := domain.WorkloadComposeName(name, target)
			if err != nil {
				return nil, err
			}
			workload := authored
			workload.Name = workloadName
			workload.ContainerName = ""
			workload.Ports = nil
			workload.Networks = cloneProxyNetworks(authored.Networks)
			for _, network := range workload.Networks {
				if network != nil {
					network.Aliases = append(network.Aliases, workloadName)
					sort.Strings(network.Aliases)
				}
			}
			releaseID := ""
			if identity.ReleaseID != "" && identity.Target == target {
				releaseID = identity.ReleaseID
				workload.Image = identity.Image
			}
			role := "singleton"
			serviceRole := agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON
			slot := ""
			if target != domain.WorkloadSingleton {
				role = "slot"
				serviceRole = agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT
				slot = string(target)
			}
			labels, expected, err := composeServiceRuntimeLabels(
				authored.Labels,
				serviceID,
				role,
				slot,
				releaseID,
				input,
			)
			if err != nil {
				return nil, err
			}
			workload.Labels = labels
			workload.Extensions, err = composeResourceExtensions(
				authored.Extensions,
				"service",
				serviceID,
				input.EnvironmentID,
			)
			if err != nil {
				return nil, err
			}
			project.Services[workloadName] = workload
			replicas := workload.GetScale()
			if replicas <= 0 {
				return nil, errs.New(errs.KindValidationFailed, "Service slot replicas must be positive")
			}
			result = append(result, &agentpb.ComposeService{
				ServiceId: serviceID, ComposeName: workloadName, ExpectedLabels: expected,
				OwnerComponentId: composeServiceComponentOwner(input.Identities.Services, serviceID),
				ExpectedReplicas: expectedRuntimeReplicas(active, replicas),
				HasHealthcheck:   workload.HealthCheck != nil && !workload.HealthCheck.Disable,
				ImageReference:   workload.Image,
				Role:             serviceRole, Slot: slot,
			})
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].ServiceId != result[j].ServiceId {
			return result[i].ServiceId < result[j].ServiceId
		}
		return result[i].ComposeName < result[j].ComposeName
	})
	return result, nil
}

func composeServiceComponentOwner(identities []ComposeResourceIdentity, serviceID string) string {
	for _, identity := range identities {
		if identity.ID == serviceID {
			return identity.ComponentID
		}
	}
	return ""
}

func expectedRuntimeReplicas(active bool, configured int) uint32 {
	if !active {
		return 0
	}
	return uint32(configured)
}

func cloneProxyNetworks(
	values map[string]*composetypes.ServiceNetworkConfig,
) map[string]*composetypes.ServiceNetworkConfig {
	result := make(map[string]*composetypes.ServiceNetworkConfig, len(values))
	for name, value := range values {
		if value == nil {
			result[name] = nil
			continue
		}
		clone := *value
		clone.Aliases = slices.Clone(value.Aliases)
		result[name] = &clone
	}
	return result
}

func composeServiceRuntimeLabels(
	authored composetypes.Labels,
	serviceID, role, slot, releaseID string,
	input ComposeRenderInput,
) (composetypes.Labels, []*agentpb.LabelPair, error) {
	labels, expected, err := composeOwnershipLabels(authored, "service", serviceID, input)
	if err != nil {
		return nil, nil, err
	}
	labels[composeLabelRuntimeRole] = role
	expected = append(expected, &agentpb.LabelPair{Key: composeLabelRuntimeRole, Value: role})
	if slot != "" {
		labels[composeLabelSlot] = slot
		expected = append(expected, &agentpb.LabelPair{Key: composeLabelSlot, Value: slot})
	}
	if releaseID != "" {
		labels[composeLabelReleaseID] = releaseID
		expected = append(expected, &agentpb.LabelPair{Key: composeLabelReleaseID, Value: releaseID})
	}
	sort.Slice(expected, func(i, j int) bool { return expected[i].Key < expected[j].Key })
	return labels, expected, nil
}
