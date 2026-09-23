package core

import "slices"

// EqualComponentConfig preserves the distinction between absent and empty
// collections when comparing the closed Component configuration variants.
func EqualComponentConfig(left, right ComponentConfig) bool {
	if (left.Caddy == nil) != (right.Caddy == nil) ||
		(left.CloudflareTunnel == nil) != (right.CloudflareTunnel == nil) ||
		(left.CoreDNS == nil) != (right.CoreDNS == nil) {
		return false
	}
	if left.Caddy != nil && (left.Caddy.Alias != right.Caddy.Alias ||
		left.Caddy.CaddyfileTemplate != right.Caddy.CaddyfileTemplate ||
		!equalComponentSlice(left.Caddy.ZoneIDs, right.Caddy.ZoneIDs)) {
		return false
	}
	if left.CloudflareTunnel != nil && (left.CloudflareTunnel.SecretID != right.CloudflareTunnel.SecretID ||
		!equalComponentSlice(left.CloudflareTunnel.ZoneIDs, right.CloudflareTunnel.ZoneIDs)) {
		return false
	}
	if left.CoreDNS == nil {
		return true
	}
	if left.CoreDNS.CorefileTemplate != right.CoreDNS.CorefileTemplate ||
		left.CoreDNS.UpstreamAuto != right.CoreDNS.UpstreamAuto ||
		left.CoreDNS.TailnetDelegation != right.CoreDNS.TailnetDelegation ||
		!equalComponentSlice(left.CoreDNS.UpstreamResolvers, right.CoreDNS.UpstreamResolvers) ||
		(left.CoreDNS.Forwarders == nil) != (right.CoreDNS.Forwarders == nil) ||
		len(left.CoreDNS.Forwarders) != len(right.CoreDNS.Forwarders) {
		return false
	}
	for index := range left.CoreDNS.Forwarders {
		if left.CoreDNS.Forwarders[index].Domain != right.CoreDNS.Forwarders[index].Domain ||
			!equalComponentSlice(left.CoreDNS.Forwarders[index].Resolvers, right.CoreDNS.Forwarders[index].Resolvers) {
			return false
		}
	}
	return true
}

func equalComponentSlice[E comparable](left, right []E) bool {
	return (left == nil) == (right == nil) && slices.Equal(left, right)
}
