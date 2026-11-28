package runner

import (
	"net/netip"
	"slices"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// NewIsolationPolicy uses the dedicated internal API endpoint. A
// loopback-only Controller remains usable, but cannot provision a Runner.
func NewIsolationPolicy(
	pool netip.Prefix,
	denied []netip.Prefix,
	internal string,
) (IsolationPolicy, error) {
	policy := IsolationPolicy{RunnerPool: pool, DeniedCIDRs: slices.Clone(denied)}
	endpoint, err := netip.ParseAddrPort(internal)
	if err != nil || !endpoint.Addr().Is4() || endpoint.Port() == 0 ||
		(endpoint.Addr() != netip.MustParseAddr("127.0.0.1") && !endpoint.Addr().IsPrivate()) {
		return IsolationPolicy{}, errs.New(errs.KindValidationFailed, "Runner internal Controller endpoint is invalid")
	}
	if endpoint.Addr().IsPrivate() {
		policy.ControllerEndpoint = endpoint
	}
	if !slices.ContainsFunc(policy.DeniedCIDRs, func(prefix netip.Prefix) bool {
		return prefix.Contains(endpoint.Addr())
	}) {
		policy.DeniedCIDRs = append(policy.DeniedCIDRs, netip.PrefixFrom(endpoint.Addr(), 32))
	}
	slices.SortFunc(policy.DeniedCIDRs, func(left, right netip.Prefix) int {
		return left.Addr().Compare(right.Addr())
	})
	return policy, nil
}

// NewRunnerRegistrationPolicy keeps a newly registered Runner off both
// Groundplane transports until a verified GitHub registration is migrated.
func NewRunnerRegistrationPolicy(pool netip.Prefix, denied []netip.Prefix, internal string) (IsolationPolicy, error) {
	policy, err := NewIsolationPolicy(pool, denied, internal)
	if err != nil {
		return IsolationPolicy{}, err
	}
	policy.RegistrationOnly = true
	if err := policy.validateScopedEndpoints(); err != nil {
		return IsolationPolicy{}, err
	}
	return policy, nil
}

// NewScopedIsolationPolicy seals distinct Runner-only listeners in new plans.
// An empty registry address grants no private registry access.
func NewScopedIsolationPolicy(
	pool netip.Prefix, denied []netip.Prefix, internal, scopedAPI, scopedRegistry string,
) (IsolationPolicy, error) {
	policy, err := NewIsolationPolicy(pool, denied, internal)
	if err != nil {
		return IsolationPolicy{}, err
	}
	policy.ScopedEndpoint, err = netip.ParseAddrPort(scopedAPI)
	if err != nil {
		return IsolationPolicy{}, errs.New(errs.KindValidationFailed, "scoped Runner API endpoint is invalid")
	}
	if scopedRegistry != "" {
		policy.ScopedRegistry, err = netip.ParseAddrPort(scopedRegistry)
		if err != nil {
			return IsolationPolicy{}, errs.New(errs.KindValidationFailed, "scoped Runner registry endpoint is invalid")
		}
	}
	if err := policy.validateScopedEndpoints(); err != nil {
		return IsolationPolicy{}, err
	}
	return policy, nil
}

func (policy IsolationPolicy) RequireControllerEndpoint() error {
	endpoint := policy.ControllerEndpoint
	if !endpoint.IsValid() || !endpoint.Addr().Is4() || !endpoint.Addr().IsPrivate() || endpoint.Port() == 0 {
		return errs.New(
			errs.KindValidationFailed,
			"Runner creation requires a private controller listen.internal_http address",
		)
	}
	return nil
}

func (policy IsolationPolicy) validateScopedEndpoints() error {
	api, registry := policy.ScopedEndpoint, policy.ScopedRegistry
	if policy.RegistrationOnly {
		if api != (netip.AddrPort{}) || registry != (netip.AddrPort{}) || policy.ControllerEndpoint.Port() == 53 {
			return errs.New(errs.KindValidationFailed, "Runner registration cannot expose scoped service listeners")
		}
		return policy.RequireControllerEndpoint()
	}
	if api == (netip.AddrPort{}) && registry == (netip.AddrPort{}) {
		return nil
	}
	controller := policy.ControllerEndpoint
	if controller.Port() == 53 || controller.Port() == 5000 ||
		!validScopedEndpoint(api, controller) ||
		registry != (netip.AddrPort{}) && (!validScopedEndpoint(registry, controller) || registry == api) {
		return errs.New(errs.KindValidationFailed, "Runner-only listeners must be distinct private endpoints")
	}
	return nil
}

func validScopedEndpoint(candidate, controller netip.AddrPort) bool {
	return candidate.IsValid() && candidate.Addr().Is4() && candidate.Addr().IsPrivate() &&
		candidate.Port() != 0 && candidate.Port() != 53 && candidate.Port() != 5000 && candidate != controller
}
