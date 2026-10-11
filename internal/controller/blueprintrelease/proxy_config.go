package blueprintrelease

import (
	"encoding/hex"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"

	domain "github.com/AlanD20/groundplane/internal/core/release"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// configureBlueprintProxy binds current exposure decisions without replacing
// the predecessor's independently captured proxy generation or config digest.
func configureBlueprintProxy(render *releaserender.ReleaseRenderInput, exposures []string) error {
	priorAddressable := render.PriorProxyGeneration != 0
	if render.PriorWorkload != nil && priorAddressable != (len(exposures) != 0) {
		return errs.New(
			errs.KindStateConflict,
			"Blueprint proxy addressability transition has no sealed prior authority",
		)
	}
	if len(exposures) == 0 {
		if render.Strategy == domain.StrategyBlueGreen {
			return errs.New(
				errs.KindValidationFailed,
				"blue-green requires an exposed container TCP port; configure expose or select recreate for a portless Service",
			)
		}
		return nil
	}
	ports, err := domain.ProxyPorts(exposures)
	if err != nil {
		return err
	}
	render.ProxyPorts = ports
	render.ProxyImplementation, err = projectionrecord.ServiceProxyImplementation(render.Projection)
	if err != nil {
		return err
	}
	render.ProxyGeneration = render.PriorProxyGeneration + 1
	if render.Strategy == domain.StrategyRolling {
		if render.ProxyGeneration == 1 {
			render.ProxyGeneration = 2 // Generation 1 is the unready initial proxy.
		}
		return releaserender.SealRollingProxy(render, render.ProxyGeneration)
	}
	config, err := domain.RenderProxyConfig(
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
	render.ProxyConfigDigest = hex.EncodeToString(config.SHA256[:])
	return nil
}
