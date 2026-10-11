package releaserender

import (
	releases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/serviceproxy"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/core/serviceports"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func validateOrdinaryPriorRuntime(render ReleaseRenderInput) error {
	witness := render.PriorRuntime
	if witness == nil {
		return nil
	}
	if witness.ServiceID != render.ServiceID || render.PriorWorkload == nil ||
		executionplan.ValidateNativePredecessorWitness(render.EnvironmentID, render.ServiceID,
			witness.CurrentArtifact, witness.RetainedPriorArtifact) != nil {
		return releases.CorruptReleaseRecord()
	}
	artifact, err := taskassignments.OpenRestorationWitness(render.EnvironmentID, witness.CurrentArtifact)
	if err != nil || artifact.ArtifactId != render.PriorArtifactID || artifact.ArtifactId == render.ArtifactID {
		return releases.CorruptReleaseRecord()
	}
	previous := domain.ProxySelection{}
	found := false
	for _, service := range artifact.GetServices() {
		if service.GetServiceId() != render.ServiceID ||
			service.GetRole() != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
			continue
		}
		if found {
			return releases.CorruptReleaseRecord()
		}
		found = true
		implementation, err := executionplan.OpenProxyImplementation(service.GetProxyImplementation())
		if err != nil {
			return releases.CorruptReleaseRecord()
		}
		configuration, err := serviceproxy.Read(implementation, service.GetProxyConfigJson())
		if err != nil {
			return releases.CorruptReleaseRecord()
		}
		previous.Implementation = implementation
		previous.Ports = make([]domain.ProxyPort, len(configuration.Ports))
		for index, port := range configuration.Ports {
			previous.Ports[index] = domain.ProxyPort{Port: port.Number, Protocol: serviceports.Protocol(port.Protocol)}
		}
	}
	return domain.ValidateProxyTransition(render.ServiceName, render.Strategy,
		domain.ProxySelection{Implementation: render.ProxyImplementation, Ports: render.ProxyPorts}, &previous)
}
