package releaseoperation

import (
	"encoding/hex"

	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func configureReleaseProxy(
	render *etcd.ReleaseRenderInput,
	expose []string,
	priorReleaseID string,
	revision uint64,
) error {
	ports, err := domain.ProxyPorts(expose)
	if err != nil {
		if render.Strategy == domain.StrategyBlueGreen {
			return errs.New(errs.KindValidationFailed, "blue-green release requires an addressable TCP service")
		}
		render.PriorSlot = ""
		return nil
	}
	render.ProxyGeneration = revision + 1
	if render.ProxyGeneration == 1 {
		render.ProxyGeneration = 2
	}
	render.ProxyPorts = ports
	candidate, err := domain.RenderProxyConfig(
		render.ServiceName,
		render.ReleaseID,
		render.CandidateTarget,
		render.ProxyGeneration,
		ports,
	)
	if err != nil {
		return err
	}
	render.ProxyConfigDigest = hex.EncodeToString(candidate.SHA256[:])
	if render.PriorWorkload == nil {
		return nil
	}
	render.PriorProxyGeneration = render.ProxyGeneration - 1
	prior, err := domain.RenderProxyConfig(
		render.ServiceName,
		priorReleaseID,
		render.PriorTarget,
		render.PriorProxyGeneration,
		ports,
	)
	if err != nil {
		return err
	}
	render.PriorProxyDigest = hex.EncodeToString(prior.SHA256[:])
	return nil
}
