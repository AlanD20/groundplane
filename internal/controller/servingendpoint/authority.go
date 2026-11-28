package servingendpoint

import (
	"context"
	"maps"
	"net/netip"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ingressexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/hostplacement"
	domain "github.com/AlanD20/groundplane/internal/core/servingendpoint"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ProxySource supplies sealed proxy candidates from a verified, acknowledged
// Release at the requested revision. Implementations include conditions for
// the Service, Release projection, immutable inputs and Agent runtime receipt.
type ProxySource interface {
	CaptureProxyAt(context.Context, workexecution.Identity, string, string, uint16, int64) (
		ingressexecution.NetworkRoute, []keyvalue.Condition, error,
	)
}

type NetworkRequest struct {
	ServingRequest
	Executor              workexecution.Identity
	ProcessGeneration     [16]byte
	EdgeExecutor          workexecution.Identity
	EdgeProcessGeneration [16]byte
	Proxy                 ingressexecution.NetworkRoute
}

type NetworkProof struct {
	Executor              workexecution.Identity
	ProcessGeneration     [16]byte
	EdgeExecutor          workexecution.Identity
	EdgeProcessGeneration [16]byte
	ReleaseID             string
	ProxyDigest           string
	Address               netip.AddrPort
	ObservedAt            time.Time
	Ready                 bool
	Conditions            []keyvalue.Condition
}

// NetworkSource requires authenticated evidence of the exact stable Service
// proxy being ready and reachable from the approved edge path. A Compose
// bridge address or a successful deployment alone cannot satisfy this proof.
type NetworkSource interface {
	VerifyServingProxy(context.Context, NetworkRequest) (NetworkProof, error)
}

type Authority struct {
	placement *hostplacement.Resolver
	proxies   ProxySource
	network   NetworkSource
}

func NewAuthority(placement *hostplacement.Resolver, proxies ProxySource, network NetworkSource) (*Authority, error) {
	if placement == nil || proxies == nil || network == nil {
		return nil, errs.New(
			errs.KindValidationFailed,
			"serving endpoint authority requires Host, Release and private network evidence",
		)
	}
	return &Authority{placement: placement, proxies: proxies, network: network}, nil
}

func (authority *Authority) CaptureServing(ctx context.Context, request ServingRequest) (ServingEvidence, error) {
	var noEvidence ServingEvidence
	var selected hostplacement.Snapshot
	var err error
	if request.HostID == request.EdgeHostID {
		selected, err = authority.placement.Resolve(ctx, request.HostID, request.ReadRevision, workexecution.Release)
	} else {
		selected, err = authority.placement.ResolveRemote(ctx, request.HostID, request.ReadRevision, workexecution.Release)
	}
	if err != nil {
		return noEvidence, err
	}
	edge, err := authority.placement.Resolve(ctx, request.EdgeHostID, request.ReadRevision, workexecution.PublicIngress)
	if err != nil {
		return noEvidence, err
	}
	target := request.Target
	proxy, fences, err := authority.proxies.CaptureProxyAt(ctx, selected.Identity,
		target.EnvironmentID, target.ServiceID, target.Port, request.ReadRevision)
	if err != nil {
		return noEvidence, err
	}
	if proxy.EnvironmentID != target.EnvironmentID || proxy.ServiceID != target.ServiceID ||
		proxy.AgentGeneration != selected.Identity.Generation || proxy.ReleaseID == "" || proxy.ProxyDigest == "" ||
		proxy.Target == nil || len(proxy.Addresses) == 0 || len(fences) == 0 {
		return noEvidence, conflict("acknowledged Release proxy evidence is incomplete")
	}
	approvedAddresses := slices.Collect(maps.Values(proxy.Addresses))
	network, err := authority.network.VerifyServingProxy(ctx, NetworkRequest{
		ServingRequest: request, Executor: selected.Identity, ProcessGeneration: selected.ProcessGeneration, Proxy: proxy,
		EdgeExecutor: edge.Identity, EdgeProcessGeneration: edge.ProcessGeneration,
	})
	if err != nil {
		return noEvidence, err
	}
	if network.Executor != selected.Identity || network.ProcessGeneration != selected.ProcessGeneration ||
		network.EdgeExecutor != edge.Identity || network.EdgeProcessGeneration != edge.ProcessGeneration ||
		network.ReleaseID != proxy.ReleaseID || network.ProxyDigest != proxy.ProxyDigest ||
		network.Address.Port() != target.Port || !slices.Contains(approvedAddresses, network.Address) ||
		len(network.Conditions) == 0 || !network.Ready {
		return noEvidence, conflict("network evidence does not prove an approved serving proxy for this edge")
	}
	endpoint := domain.Endpoint{
		HostID: selected.Identity.HostID, AgentID: selected.Identity.AgentID, AgentGeneration: selected.Identity.Generation,
		ReleaseID: proxy.ReleaseID, Address: network.Address,
	}
	return ServingEvidence{
		Authority: domain.ServingAuthority{Target: target, Endpoint: endpoint},
		Proof: domain.ServingProof{
			Target:     target,
			Endpoint:   endpoint,
			ObservedAt: network.ObservedAt,
			Ready:      network.Ready,
		},
		Conditions: slices.Concat(selected.Conditions, edge.Conditions, fences, network.Conditions),
	}, nil
}
