package executionplan

import (
	"bytes"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/rollingrelease"
	"github.com/AlanD20/groundplane/internal/common/serviceproxy"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func validateReleaseWorkTraffic(input *agentpb.ReleaseWorkInput, artifacts ReleaseWorkArtifacts) error {
	if artifacts.Prior != nil && (artifacts.Proxy == nil) != (artifacts.Prior.Proxy == nil) {
		return invalidReleaseWorkArtifacts()
	}
	if artifacts.Proxy == nil {
		if len(input.TrafficConfigurations) != 0 {
			return invalidReleaseWorkArtifacts()
		}
		return nil
	}
	base, err := OpenProxyConfiguration(artifacts.Proxy)
	if err != nil {
		return err
	}
	var changes []rollingrelease.Change
	expected := 1
	if artifacts.Rolling != nil {
		changes, err = artifacts.Rolling.Changes()
		if err != nil {
			return err
		}
		expected = len(changes)
	}
	if len(input.TrafficConfigurations) != expected {
		return invalidReleaseWorkArtifacts()
	}
	priorGeneration := uint64(0)
	if artifacts.Prior != nil && artifacts.Prior.Proxy != nil {
		prior, err := OpenProxyConfiguration(artifacts.Prior.Proxy)
		if err != nil {
			return err
		}
		priorGeneration = prior.Generation
		if input.Strategy != agentpb.WorkReleaseStrategy_WORK_RELEASE_STRATEGY_RECREATE &&
			(base.Implementation != prior.Implementation || !slices.Equal(base.Ports, prior.Ports)) {
			return invalidReleaseWorkArtifacts()
		}
	}
	for index, raw := range input.TrafficConfigurations {
		configuration, err := serviceproxy.Read(base.Implementation, raw)
		if err != nil {
			return err
		}
		if configuration.ReleaseID != input.ReleaseId || !slices.Equal(configuration.Ports, base.Ports) ||
			configuration.Generation <= priorGeneration || index > 0 && configuration.Generation-priorGeneration != 1 {
			return invalidReleaseWorkArtifacts()
		}
		names := make([]string, 0, len(artifacts.Workloads))
		if artifacts.Rolling == nil {
			for _, workload := range artifacts.Workloads {
				names = append(names, workload.ComposeName)
			}
		} else {
			for _, replica := range changes[index].After {
				names = append(names, replica.RoutingName())
			}
		}
		slices.Sort(names)
		if !slices.Equal(configuration.Upstreams, names) {
			return invalidReleaseWorkArtifacts()
		}
		canonical, err := serviceproxy.Render(configuration)
		if err != nil || !bytes.Equal(canonical, raw) {
			return invalidReleaseWorkArtifacts()
		}
		priorGeneration = configuration.Generation
	}
	return nil
}
