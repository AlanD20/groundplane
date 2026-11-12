// Package controllerendpoint identifies supported operator network addresses.
package controllerendpoint

import "net/netip"

// IsTailnet reports whether an address belongs to Tailscale IPv4 space.
func IsTailnet(address netip.Addr) bool {
	return netip.MustParsePrefix("100.64.0.0/10").Contains(address)
}
