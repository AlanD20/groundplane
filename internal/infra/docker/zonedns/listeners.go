// Package zonedns selects existing owned bridge gateways without changing host
// networking or opening another network for a workload.
package zonedns

import (
	"context"
	"net/netip"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/networkname"
	model "github.com/AlanD20/groundplane/internal/common/zonedns"
	"github.com/AlanD20/groundplane/pkg/errs"
	containerderrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/client"
)

type Inspector struct{}

// Capture inspects only the selected GP networks on the local managed daemon.
// Missing networks are not yet materialized; their gateways must not be bound.
func (Inspector) Capture(ctx context.Context, candidates []model.Candidate) (listeners []netip.Addr, err error) {
	if len(candidates) > 256 {
		return nil, errs.New(errs.KindValidationFailed, "at most 256 internal DNS Zones are supported")
	}
	if len(candidates) == 0 {
		return nil, nil
	}
	engine, err := client.New(client.WithHost("unix:///var/run/docker.sock"))
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	defer func() {
		if closeErr := engine.Close(); closeErr != nil && err == nil {
			err = errs.Wrap(errs.KindInternal, closeErr)
		}
	}()
	for _, candidate := range candidates {
		if ids.Validate(ids.KindEnvironment, candidate.EnvironmentID) != nil {
			return nil, errs.New(errs.KindInternal, "DNS Zone owner is invalid")
		}
		name, err := networkname.New(candidate.ZoneID)
		if err != nil {
			return nil, err
		}
		gateway, err := model.Gateway(candidate.Subnet)
		if err != nil {
			return nil, err
		}
		result, err := engine.NetworkInspect(ctx, name, client.NetworkInspectOptions{})
		if containerderrdefs.IsNotFound(err) {
			continue
		}
		if err != nil {
			return nil, errs.Wrap(errs.KindStorageUnavailable, err)
		}
		network := result.Network
		if network.Name != name || network.Driver != "bridge" || !network.Internal || !network.EnableIPv4 ||
			network.EnableIPv6 ||
			network.Attachable ||
			len(network.Options) != 0 ||
			len(network.IPAM.Config) != 1 ||
			network.IPAM.Driver != "default" ||
			len(network.IPAM.Options) != 0 ||
			network.IPAM.Config[0].Subnet.String() != candidate.Subnet ||
			network.IPAM.Config[0].Gateway.String() != gateway ||
			network.IPAM.Config[0].IPRange.IsValid() ||
			len(network.IPAM.Config[0].AuxAddress) != 0 ||
			network.Labels["com.groundplane.managed"] != "true" ||
			network.Labels["com.groundplane.kind"] != "network" ||
			network.Labels["com.groundplane.environment-id"] != candidate.EnvironmentID {
			return nil, errs.New(
				errs.KindStateConflict,
				"internal DNS Zone bridge differs from its current owned topology",
			)
		}
		address, err := netip.ParseAddr(gateway)
		if err != nil {
			return nil, errs.Wrap(errs.KindInternal, err)
		}
		listeners = append(listeners, address)
	}
	slices.SortFunc(listeners, func(left, right netip.Addr) int { return left.Compare(right) })
	return slices.Compact(listeners), nil
}
