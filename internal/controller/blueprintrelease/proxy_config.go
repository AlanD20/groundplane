package blueprintrelease

import (
	"encoding/hex"
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
		return nil
	}
	ports, err := domain.ProxyPorts(exposures)
	if err != nil {
		return err
	}
	render.ProxyPorts = ports
	render.ProxyGeneration = render.PriorProxyGeneration + 1
	config, err := domain.RenderProxyConfig(
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
