package releaseoperation

import (
	"encoding/hex"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
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
	if len(expose) == 0 {
		if render.Strategy == domain.StrategyBlueGreen {
			return errs.New(
				errs.KindValidationFailed,
				"blue-green requires an exposed container TCP port; configure expose or select recreate for a portless Service",
			)
		}
		render.PriorSlot = ""
		return nil
	}
	ports, err := domain.ProxyPorts(expose)
	if err != nil {
		return err
	}
	render.ProxyImplementation, err = projectionrecord.ServiceProxyImplementation(render.Projection)
	if err != nil {
		return err
	}
	if err := domain.ValidateProxyForwarding(render.ProxyImplementation, ports); err != nil {
		return err
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
	if render.Strategy == domain.StrategyRolling {
		if err := releaserender.SealRollingProxy(render, render.ProxyGeneration); err != nil {
			return err
		}
	} else {
		candidate, err := domain.RenderProxyConfig(
			render.ProxyImplementation,
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
	}
	if render.PriorWorkload == nil {
		return nil
	}
	if prior == nil || prior.ProxyGeneration == 0 || prior.ProxyConfigDigest == "" {
		return errs.New(errs.KindStateConflict, "serving Release proxy authority is missing")
	}
	render.PriorProxyGeneration, render.PriorProxyDigest = prior.ProxyGeneration, prior.ProxyConfigDigest
	return nil
}
