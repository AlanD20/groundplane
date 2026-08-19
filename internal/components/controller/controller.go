// Package controller registers the platform-owned Groundplane Controller
// component.
package controller

import (
	"github.com/AlanD20/groundplane/internal/components"
	"github.com/AlanD20/groundplane/internal/core"
)

// Register adds Controller metadata to the shared component registry.
func Register() {
	components.Register(components.Registration{
		Kind:          core.ComponentKindController,
		Label:         "Groundplane Controller",
		AllowedOwners: []core.ComponentOwner{core.ComponentOwnerPlatform},
		ApplyStrategy: components.PlatformUpdate,
	})
}
