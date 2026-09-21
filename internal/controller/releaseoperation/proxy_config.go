package releaseoperation

import (
	"encoding/hex"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"math"

	domain "github.com/AlanD20/groundplane/internal/core/release"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func configureReleaseProxy(
	render *releaserender.ReleaseRenderInput,
	expose []string,
	prior *releaserender.ReleaseRenderInput,
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
	if revision == math.MaxUint64 || prior != nil && prior.ProxyGeneration == math.MaxUint64 {
		return errs.New(errs.KindStateConflict, "release proxy generation is exhausted")
	}
	render.ProxyGeneration = revision + 1
	if render.ProxyGeneration == 1 {
		render.ProxyGeneration = 2
	}
	if prior != nil {
		render.ProxyGeneration = max(render.ProxyGeneration, prior.ProxyGeneration+1)
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
	if prior == nil || prior.ProxyGeneration == 0 || prior.ProxyConfigDigest == "" {
		return errs.New(errs.KindStateConflict, "serving Release proxy authority is missing")
	}
	render.PriorProxyGeneration, render.PriorProxyDigest = prior.ProxyGeneration, prior.ProxyConfigDigest
	return nil
}
