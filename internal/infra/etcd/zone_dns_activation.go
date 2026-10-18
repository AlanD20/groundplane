package etcd

import (
	"context"
	"net/netip"
	"slices"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/platformcomponents"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ZoneDNSActivation pins successful serving evidence before a Service child can
// be published. Waiting happens in the Controller, not an occupied Agent worker.
type ZoneDNSActivation struct {
	conditions []keyvalue.Condition
}

func (repository *EnvironmentBlueprintRepository) ReadZoneDNSActivation(
	ctx context.Context,
	upstreams []string,
) (ZoneDNSActivation, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return ZoneDNSActivation{}, err
	}
	if len(upstreams) == 0 {
		return ZoneDNSActivation{}, nil
	}
	if len(upstreams) > 256 {
		return ZoneDNSActivation{}, errs.New(
			errs.KindValidationFailed,
			"Service has more than 256 internal Zone DNS upstreams",
		)
	}
	parsed := make([]netip.Addr, len(upstreams))
	for index, upstream := range upstreams {
		address, err := netip.ParseAddr(upstream)
		if err != nil || !address.Is4() || !address.IsPrivate() || address.String() != upstream {
			return ZoneDNSActivation{}, errs.New(
				errs.KindValidationFailed,
				"internal Zone DNS upstream is not canonical private IPv4",
			)
		}
		parsed[index] = address
	}
	slices.SortFunc(parsed, func(left, right netip.Addr) int { return left.Compare(right) })
	parsed = slices.Compact(parsed)
	addresses := make([]string, len(parsed))
	for index, address := range parsed {
		addresses[index] = address.String()
	}
	if _, err := platformcomponents.ParseZoneListeners(addresses); err != nil {
		return ZoneDNSActivation{}, err
	}
	// Read an exact key only to capture the current MVCC view; the successful
	// acknowledgement and its complete authority are read at that same revision.
	read, err := repository.store.Get(ctx, platformcomponents.PlatformComponentKindKey(core.ComponentKindCoreDNS))
	if err != nil {
		return ZoneDNSActivation{}, err
	}
	if read == nil || read.ReadRevision <= 0 {
		return ZoneDNSActivation{}, errs.New(errs.KindInternal, "Zone DNS activation read is incomplete")
	}
	ack, ready, err := readResolverAcknowledgement(ctx, repository.store, read.ReadRevision)
	if err != nil {
		return ZoneDNSActivation{}, err
	}
	if ready {
		for _, address := range addresses {
			if !slices.Contains(ack.input.ZoneListeners, address) {
				ready = false
				break
			}
		}
	}
	if !ready {
		if ack.attempt != nil {
			failed, _, err := failedResolverConfigurationRequest(ctx, repository.store, read.ReadRevision, ack)
			if err != nil {
				return ZoneDNSActivation{}, err
			}
			if failed != "" {
				return ZoneDNSActivation{}, errs.Newf(
					errs.KindValidationFailed,
					"coredns update Task %s failed; repair the resolver and submit a new Blueprint Apply",
					failed,
				)
			}
		}
		return ZoneDNSActivation{}, errs.New(
			errs.KindResourceInUse,
			"waiting for CoreDNS to activate this Service's internal Zone listeners",
		)
	}
	return ZoneDNSActivation{conditions: slices.Clone(ack.conditions)}, nil
}

func (publication ReleaseWorkPublication) WithZoneDNSActivation(
	activation ZoneDNSActivation,
) (ReleaseWorkPublication, error) {
	if len(activation.conditions) == 0 {
		return publication, nil
	}
	if publication.IsZero() {
		return ReleaseWorkPublication{}, errs.New(
			errs.KindInternal,
			"Zone DNS activation requires a Service release publication",
		)
	}
	conditions := slices.Clone(publication.conditions)
	for _, candidate := range activation.conditions {
		for _, existing := range conditions {
			if existing.Key == candidate.Key && existing != candidate {
				return ReleaseWorkPublication{}, errs.New(
					errs.KindResourceInUse,
					"coredns activation changed while preparing the Service release",
				)
			}
		}
		conditions = appendHostResolutionCondition(conditions, candidate)
	}
	publication.conditions = conditions
	return publication, nil
}
