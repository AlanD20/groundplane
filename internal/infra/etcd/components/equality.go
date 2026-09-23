package components

import (
	"slices"

	"github.com/AlanD20/groundplane/internal/core"
)

// EqualRecord compares the complete stored Component authority.
func EqualRecord(left, right Record) bool {
	return left.Desired.ID == right.Desired.ID && left.Desired.Owner == right.Desired.Owner &&
		left.Desired.OwnerID == right.Desired.OwnerID && left.Desired.Kind == right.Desired.Kind &&
		left.Desired.Enabled == right.Desired.Enabled &&
		core.EqualComponentConfig(left.Desired.Config, right.Desired.Config) &&
		(left.Runtime.GeneratedServices == nil) == (right.Runtime.GeneratedServices == nil) &&
		slices.Equal(left.Runtime.GeneratedServices, right.Runtime.GeneratedServices) &&
		left.Runtime.PinnedIPv4 == right.Runtime.PinnedIPv4 && left.Runtime.Healthy == right.Runtime.Healthy
}
