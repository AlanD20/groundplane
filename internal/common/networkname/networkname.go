// Package networkname owns the canonical physical name for a managed Network.
package networkname

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const prefix = "gp_net_"

// New returns the one physical Docker name derived from a canonical Network id.
func New(networkID string) (string, error) {
	if ids.Validate(ids.KindNetwork, networkID) != nil {
		return "", errs.New(errs.KindValidationFailed, "Network id is invalid")
	}
	return prefix + networkID, nil
}
