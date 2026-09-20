package taskplanning

import (
	composeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	composerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/compose-spec/compose-go/v2/types"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/ipam"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ProjectZoneProjection builds the complete immutable Zone projection for
// owned Compose networks. Blueprint networks must carry the operator's one
// explicit IPv4 subnet decision; Docker-derived IPAM is never desired state.
func ProjectZoneProjection(
	project *types.Project,
	identities composeidentity.Snapshot,
	ownerKind core.ZoneOwnerKind,
	ownerID string,
) ([]core.Zone, error) {
	ownerIDKind := ids.KindEnvironment
	if ownerKind == core.ZoneOwnerBackingProject {
		ownerIDKind = ids.KindProject
	} else if ownerKind != core.ZoneOwnerEnvironment {
		return nil, errs.New(errs.KindInternal, "Blueprint Zone projection ownership is invalid")
	}
	if project == nil || ids.Validate(ownerIDKind, ownerID) != nil {
		return nil, errs.New(errs.KindInternal, "Blueprint Zone projection ownership is invalid")
	}
	names, err := composeidentity.OwnedNetworkNames(project)
	if err != nil {
		return nil, err
	}
	networkIDs, err := composerender.IndexComposeIdentities(ids.KindNetwork, names, identities.Networks)
	if err != nil {
		return nil, err
	}
	zones := make([]core.Zone, 0, len(names))
	for _, name := range names {
		network := project.Networks[name]
		if (network.Driver != "" && network.Driver != "bridge") || len(network.DriverOpts) != 0 ||
			network.Attachable || network.EnableIPv4 != nil && !*network.EnableIPv4 ||
			network.EnableIPv6 != nil && *network.EnableIPv6 ||
			(network.Ipam.Driver != "" && network.Ipam.Driver != "default") ||
			len(network.Ipam.Options) != 0 || len(network.Ipam.Extensions) != 0 || len(network.Ipam.Config) != 1 {
			return nil, errs.Newf(
				errs.KindValidationFailed,
				"Blueprint network %q must be one MVP bridge Zone with one explicit IPv4 subnet",
				name,
			)
		}
		pool := network.Ipam.Config[0]
		if pool == nil || pool.Gateway != "" || pool.IPRange != "" || len(pool.AuxiliaryAddresses) != 0 ||
			len(pool.Extensions) != 0 {
			return nil, errs.Newf(
				errs.KindValidationFailed,
				"Blueprint network %q IPAM may declare only its subnet in the MVP",
				name,
			)
		}
		subnet, err := ipam.ParseIPv4Prefix(pool.Subnet)
		if err != nil || subnet.String() != pool.Subnet {
			return nil, errs.Newf(
				errs.KindValidationFailed,
				"Blueprint network %q subnet must be a canonical IPv4 CIDR",
				name,
			)
		}
		zones = append(zones, core.Zone{
			ID: networkIDs[name], Name: name, Subnet: subnet.String(), Internal: network.Internal,
			OwnerKind: ownerKind, OwnerID: ownerID,
		})
	}
	return zones, nil
}
