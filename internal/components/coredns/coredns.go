// Package coredns registers the platform-owned host DNS resolver component.
package coredns

import (
	"github.com/AlanD20/groundplane/internal/components"
	"github.com/AlanD20/groundplane/internal/core"
)

// Register adds CoreDNS metadata to the shared component registry.
func Register() {
	components.Register(components.Registration{
		Kind:          core.ComponentKindCoreDNS,
		Label:         "CoreDNS (host resolver)",
		AllowedOwners: []core.ComponentOwner{core.ComponentOwnerPlatform},
		ApplyStrategy: components.PlatformUpdate,
		ConfigSchema: []string{
			"upstream_auto",
			"upstream_resolvers",
			"forwarders",
			"tailnet_delegation",
		},
	})
}
