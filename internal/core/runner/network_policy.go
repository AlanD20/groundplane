package runner

import (
	"net/netip"
	"slices"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// NewIsolationPolicy selects the first configured private API listener. A
// loopback-only Controller remains usable, but cannot provision a Runner.
func NewIsolationPolicy(
	pool netip.Prefix,
	denied []netip.Prefix,
	listeners []string,
) (IsolationPolicy, error) {
	policy := IsolationPolicy{RunnerPool: pool, DeniedCIDRs: slices.Clone(denied)}
	for _, address := range listeners {
		endpoint, err := netip.ParseAddrPort(address)
		if err != nil || !endpoint.Addr().Is4() || endpoint.Port() == 0 ||
			(!endpoint.Addr().IsLoopback() && !endpoint.Addr().IsPrivate()) {
			return IsolationPolicy{}, errs.New(errs.KindValidationFailed, "Runner Controller listener is invalid")
		}
		if endpoint.Addr().IsPrivate() && !policy.ControllerEndpoint.IsValid() {
			policy.ControllerEndpoint = endpoint
		}
		if !slices.ContainsFunc(policy.DeniedCIDRs, func(prefix netip.Prefix) bool {
			return prefix.Contains(endpoint.Addr())
		}) {
			policy.DeniedCIDRs = append(policy.DeniedCIDRs, netip.PrefixFrom(endpoint.Addr(), 32))
		}
	}
	slices.SortFunc(policy.DeniedCIDRs, func(left, right netip.Prefix) int {
		return left.Addr().Compare(right.Addr())
	})
	return policy, nil
}

func (policy IsolationPolicy) RequireControllerEndpoint() error {
	endpoint := policy.ControllerEndpoint
	if !endpoint.IsValid() || !endpoint.Addr().Is4() || !endpoint.Addr().IsPrivate() || endpoint.Port() == 0 {
		return errs.New(
			errs.KindValidationFailed,
			"Runner creation requires a trusted private controller listen.http address",
		)
	}
	return nil
}
