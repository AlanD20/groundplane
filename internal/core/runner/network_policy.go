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
