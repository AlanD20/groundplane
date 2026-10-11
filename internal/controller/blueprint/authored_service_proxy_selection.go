package blueprint

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/serviceproxy"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Bind only from this parent's complete sealed runtime projection. Including
// the selected engine in each affected Service fingerprint prevents a router
// engine change from leaving old stable proxies marked already applied.
// Backing containers have direct Attach endpoints; portless Services have no
// stable proxy. Neither acquires an unrelated Component dependency.
func (planner *authoredUnitPlanner) bindServiceProxySelections(
	runtime projectionrecord.EnvironmentComposeProjection,
) error {
	if runtime.EnvironmentID != planner.input.desired.EnvironmentID ||
		runtime.RevisionID != planner.input.desired.RevisionID ||
		runtime.RenderGeneration != planner.input.desired.RenderGeneration {
		return errs.New(errs.KindStateConflict, "Blueprint proxy selection belongs to another runtime projection")
	}
	var implementation serviceproxy.Implementation
	seen := make(map[string]bool, len(runtime.DesiredServices))
	for _, projection := range runtime.DesiredServices {
		desired := projection.Desired
		identity, found := planner.identities[ids.KindService][desired.Name]
		if !found || identity.ID != desired.ID || seen[desired.Name] {
			return errs.New(errs.KindStateConflict, "Blueprint proxy selection Service identity changed")
		}
		seen[desired.Name] = true
		if desired.Adapter != "" || len(desired.Expose) == 0 {
			continue
		}
		if implementation == "" {
			var err error
			implementation, err = projectionrecord.ServiceProxyImplementation(runtime)
			if err != nil {
				return err
			}
		}
		planner.serviceProxies[desired.Name] = implementation
	}
	if len(seen) != len(planner.identities[ids.KindService]) {
		return errs.New(errs.KindStateConflict, "Blueprint runtime projection omits an authored Service")
	}
	return nil
}

// The router unit serializes changes to its shared forwarding choice against
// these Service resources. Claims survive cancellation/recovery; acquiring them
// does not authorize this Component child to mutate a Service workload.
func (planner *authoredUnitPlanner) componentForwardingClaims(capability string) []blueprintunits.ResourceKey {
	if core.ComponentCapability(capability) != core.ComponentCapabilityHTTPRouter ||
		planner.input.owner.WorkspaceType == taskjournal.TaskWorkspacePlatform {
		return nil
	}
	var claims []blueprintunits.ResourceKey
	// Desired portlessness does not prove that a retained runtime has no proxy.
	// Include every stable identity, not just newly exposed Services.
	for _, identity := range planner.identities[ids.KindService] {
		claims = append(claims, authoredResourceKey(ids.KindService, identity.ID))
	}
	for _, omission := range planner.input.identities.PendingOmissions {
		if omission.Kind == ids.KindService {
			claims = append(claims, authoredResourceKey(ids.KindService, omission.ID))
		}
	}
	return canonicalAuthoredUnitKeys(claims)
}
