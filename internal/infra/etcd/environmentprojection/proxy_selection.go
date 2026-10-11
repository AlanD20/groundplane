package environmentprojection

import (
	"github.com/AlanD20/groundplane/internal/common/serviceproxy"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ServiceProxyImplementation reads only this frozen Environment's single
// router slot. Disabling HTTP Routes does not silently change the slot's engine
// or select a second implementation for internal Service forwarding.
func ServiceProxyImplementation(projection EnvironmentComposeProjection) (serviceproxy.Implementation, error) {
	if len(projection.Components) == 0 {
		return "", errs.New(errs.KindStateConflict, "Environment snapshot has no captured proxy selection")
	}
	if err := validateEnvironmentComponentProjection(projection.EnvironmentID, projection.Components); err != nil {
		return "", err
	}
	for _, component := range projection.Components {
		switch component.Desired.Kind {
		case core.ComponentKindIngressCaddy:
			return serviceproxy.Caddy, nil
		case core.ComponentKindIngressEnvoy:
			return serviceproxy.Envoy, nil
		}
	}
	return "", errs.New(errs.KindStateConflict, "Environment snapshot has no captured proxy selection")
}
