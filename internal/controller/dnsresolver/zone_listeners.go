package dnsresolver

import (
	"context"
	"net/netip"

	"github.com/AlanD20/groundplane/internal/common/zonedns"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/platformcomponents"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type zoneDNSCandidateReader interface {
	ResolveZoneDNSCandidates(context.Context, int64) ([]zonedns.Candidate, []keyvalue.Condition, error)
}

type zoneDNSNetworkInspector interface {
	Capture(context.Context, []zonedns.Candidate) ([]netip.Addr, error)
}

// ZoneListenerSource separates current operator decisions from host evidence.
// Only a present, owned internal bridge can contribute a resolver bind address.
type ZoneListenerSource struct {
	decisions zoneDNSCandidateReader
	networks  zoneDNSNetworkInspector
}

func NewZoneListenerSource(
	decisions zoneDNSCandidateReader,
	networks zoneDNSNetworkInspector,
) (*ZoneListenerSource, error) {
	if decisions == nil || networks == nil {
		return nil, errs.New(errs.KindInternal, "Zone DNS listener dependencies are required")
	}
	return &ZoneListenerSource{decisions: decisions, networks: networks}, nil
}

func (source *ZoneListenerSource) Resolve(
	ctx context.Context,
	revision int64,
) (platformcomponents.ZoneDNSListenerSelection, error) {
	candidates, conditions, err := source.decisions.ResolveZoneDNSCandidates(ctx, revision)
	if err != nil {
		return platformcomponents.ZoneDNSListenerSelection{}, err
	}
	addresses, err := source.networks.Capture(ctx, candidates)
	if err != nil {
		return platformcomponents.ZoneDNSListenerSelection{}, err
	}
	selection := platformcomponents.ZoneDNSListenerSelection{ReadRevision: revision, Conditions: conditions}
	for _, address := range addresses {
		selection.Addresses = append(selection.Addresses, address.String())
	}
	return selection, selection.Validate()
}
