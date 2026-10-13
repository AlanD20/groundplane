package serviceproxy

import "github.com/AlanD20/groundplane/pkg/errs"

// Target-only releases retire old upstreams after TCP draining. That procedure
// does not establish UDP session or reply continuity, so it cannot authorize a
// UDP listener even if the proxy can otherwise forward its datagrams.
func ValidateTrafficSwitchPorts(implementation Implementation, ports []Port) error {
	if err := ValidatePorts(implementation, ports); err != nil {
		return err
	}
	return ValidateTrafficSwitchTransports(ports)
}

// ValidateTrafficSwitchTransports applies before a runtime or proxy exists.
// Its callers supply already parsed port declarations.
func ValidateTrafficSwitchTransports(ports []Port) error {
	for _, port := range ports {
		if port.Protocol == UDP {
			return errs.Newf(
				errs.KindValidationFailed,
				"UDP forwarding on port %d cannot use blue-green or rolling: GP cannot preserve UDP sessions while retiring the old workload; select recreate explicitly, which interrupts traffic",
				port.Number,
			)
		}
	}
	return nil
}
