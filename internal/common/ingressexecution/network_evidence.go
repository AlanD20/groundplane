package ingressexecution

import (
	"net/netip"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

// NetworkRequest carries only identity and endpoints from the Controller's
// captured serving release and host authority, never workflow or app secrets.
type NetworkRequest struct {
	HostID, AgentID string
	Addresses       []netip.Addr
	Routes          []NetworkRoute
}

const RelayPort = 29080
const RelayOriginHeader = "X-Groundplane-Relay-Origin"
const RelayProofHeader = "X-Groundplane-Relay-Proof"

// NetworkEndpoint keeps the verified Service endpoint distinct from the
// optional Host-owned egress relay reached from the isolated Caddy namespace.
type NetworkEndpoint struct {
	Address netip.AddrPort
	Relay   netip.AddrPort
	Proof   string
}

type NetworkRoute struct {
	RouteID, EnvironmentID, ServiceID, ReleaseID string
	ProxyComposeName                             string
	ProxyDigest                                  string
	AgentGeneration                              uint64
	Addresses                                    map[string]netip.AddrPort
	Target                                       *agentpb.ServiceObservationTarget
}
