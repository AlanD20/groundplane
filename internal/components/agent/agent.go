// Package agent registers the platform-owned Groundplane Agent component.
package agent

import (
	"github.com/AlanD20/groundplane/internal/components"
	"github.com/AlanD20/groundplane/internal/core"
)

// Register adds Agent metadata to the shared component registry.
func Register() {
	components.Register(components.Registration{
		Kind:          core.ComponentKindAgent,
		Label:         "Groundplane Agent",
		AllowedOwners: []core.ComponentOwner{core.ComponentOwnerPlatform},
		ApplyStrategy: components.PlatformUpdate,
		ConfigSchema:  []string{"pull_interval", "max_concurrent", "labels"},
	})
}
