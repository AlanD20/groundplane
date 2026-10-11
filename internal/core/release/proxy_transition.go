package release

import (
	"slices"

	"github.com/AlanD20/groundplane/internal/common/serviceproxy"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ProxySelection is the listener and implementation decision of one captured
// Release. Upstreams can change without replacing these stable listeners.
type ProxySelection struct {
	Implementation serviceproxy.Implementation
	Ports          []ProxyPort
}

// ValidateProxyTransition runs before publication. Recreate can replace a
// proxy process; blue-green and rolling switch targets within the existing
// process, so they cannot also replace its implementation or listener set.
// A nil predecessor denotes first deployment, not inferred container absence.
func ValidateProxyTransition(
	name string,
	strategy Strategy,
	candidate ProxySelection,
	previous *ProxySelection,
) error {
	if strategy != StrategyRecreate && strategy != StrategyBlueGreen && strategy != StrategyRolling {
		return errs.New(errs.KindValidationFailed, "Service proxy transition has an invalid release strategy")
	}
	if err := validateProxySelection(candidate); err != nil {
		return err
	}
	if strategy != StrategyRecreate && len(candidate.Ports) != 0 {
		if err := serviceproxy.ValidateTrafficSwitchPorts(candidate.Implementation, proxyConfigurationPorts(candidate.Ports)); err != nil {
			return errs.Newf(
				errs.KindValidationFailed,
				"Service %q cannot deploy using %s: %s",
				name,
				strategy,
				err.Error(),
			)
		}
	}
	if previous == nil {
		return nil
	}
	if err := validateProxySelection(*previous); err != nil {
		return errs.New(errs.KindStateConflict, "captured predecessor proxy listeners are invalid")
	}
	if (len(candidate.Ports) == 0) != (len(previous.Ports) == 0) {
		return errs.Newf(
			errs.KindStateConflict,
			"Service %q cannot add or remove its stable proxy in this Release; the captured recovery plan requires unchanged proxy addressability",
			name,
		)
	}
	if len(candidate.Ports) == 0 || strategy == StrategyRecreate {
		return nil
	}
	if candidate.Implementation != previous.Implementation {
		return errs.Newf(
			errs.KindValidationFailed,
			"Service %q cannot change its proxy from %s to %s using %s: replacing the proxy interrupts existing connections; select recreate explicitly before switching the Environment proxy",
			name,
			previous.Implementation,
			candidate.Implementation,
			strategy,
		)
	}
	if !slices.Equal(candidate.Ports, previous.Ports) {
		return errs.Newf(
			errs.KindValidationFailed,
			"Service %q cannot change its exposed port/protocol pairs using %s: this strategy retains the existing proxy listeners; select recreate explicitly for this change",
			name,
			strategy,
		)
	}
	return nil
}

func validateProxySelection(selection ProxySelection) error {
	if len(selection.Ports) == 0 {
		if selection.Implementation != "" {
			return errs.New(errs.KindValidationFailed, "portless Service has a proxy implementation")
		}
		return nil
	}
	return ValidateProxyForwarding(selection.Implementation, selection.Ports)
}
