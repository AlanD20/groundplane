package release

import (
	"github.com/AlanD20/groundplane/internal/common/rollingrelease"
	"github.com/AlanD20/groundplane/internal/common/serviceproxy"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ValidateDeploymentReadiness is shared by direct Deploy, image-only rollback
// and Blueprint preparation before images are selected or Tasks are published.
func ValidateDeploymentReadiness(
	name string, strategy Strategy, replicas uint32, healthcheckEnabled bool, exposures []string,
) error {
	if replicas == 0 {
		return errs.New(errs.KindValidationFailed, "release replica count must be positive")
	}
	if (strategy == StrategyBlueGreen || strategy == StrategyRolling) && !healthcheckEnabled {
		return errs.Newf(errs.KindValidationFailed,
			"Service %q: %s requires an enabled healthcheck with an explicit command; configure one or select recreate",
			name, strategy)
	}
	if strategy == StrategyBlueGreen && replicas != 1 {
		return errs.New(
			errs.KindValidationFailed,
			"blue-green requires one replica; use rolling or recreate for multiple replicas",
		)
	}
	if strategy == StrategyRolling && replicas > rollingrelease.MaximumReplicas {
		return errs.New(errs.KindValidationFailed, "rolling requires 1 to 16 replicas")
	}
	if (strategy == StrategyBlueGreen || strategy == StrategyRolling) && len(exposures) != 0 {
		ports, err := ProxyPorts(exposures)
		if err != nil {
			return err
		}
		return serviceproxy.ValidateTrafficSwitchTransports(proxyConfigurationPorts(ports))
	}
	return nil
}
