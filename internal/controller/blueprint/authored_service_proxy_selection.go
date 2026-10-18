package blueprint

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
)

// The router unit serializes changes to its shared forwarding choice against
// these Service resources. Claims survive cancellation/recovery; acquiring them
// does not authorize this Component child to mutate a Service workload.
func (planner *authoredUnitPlanner) componentForwardingClaims(capability string) []blueprintunits.ResourceKey {
	if core.ComponentCapability(capability) != core.ComponentCapabilityHTTPRouter ||
		planner.input.owner.TenantID == "" {
		return nil
	}
	var claims []blueprintunits.ResourceKey
	// Desired portlessness does not prove that a retained runtime has no proxy.
	// Include every stable identity, not just newly exposed Services.
	for _, identity := range planner.identities[ids.KindService] {
		claims = append(claims, authoredResourceKey(ids.KindService, identity.ID))
	}
	for _, omission := range planner.omissions {
		if omission.Target.Kind == ids.KindService {
			claims = append(claims, omission.Target)
		}
	}
	return canonicalAuthoredUnitKeys(claims)
}
