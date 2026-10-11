package serviceproxy

import (
	"encoding/json"
	"strings"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// Envoy forwards Service transport to captured workloads. Environment HTTP
// host/path routing is a separate registered-Component plan, not a health probe.
type envoyDocument struct {
	Admin           envoyAdmin     `json:"admin"`
	StaticResources envoyResources `json:"static_resources"`
}
type envoyAdmin struct {
	Address envoyAddress `json:"address"`
}
type envoyAddress struct {
	Socket envoySocket `json:"socket_address"`
}
type envoySocket struct {
	Address  string `json:"address"`
	Port     uint16 `json:"port_value"`
	Protocol string `json:"protocol"`
}
type envoyResources struct {
	Clusters  []envoyCluster  `json:"clusters"`
	Listeners []envoyListener `json:"listeners"`
}
type envoyListener struct {
	Address         envoyAddress       `json:"address"`
	FilterChains    []envoyFilterChain `json:"filter_chains,omitempty"`
	ListenerFilters []envoyUDPFilter   `json:"listener_filters,omitempty"`
	Name            string             `json:"name"`
	UDPConfig       *envoyUDPListener  `json:"udp_listener_config,omitempty"`
}
type envoyUDPListener struct {
	DownstreamSocket envoyDatagramSocket `json:"downstream_socket_config"`
}
type envoyDatagramSocket struct {
	// Envoy uses a protobuf UInt64Value; admin JSON emits its decimal string.
	MaximumDatagramSize uint64 `json:"max_rx_datagram_size,string"`
}
type envoyFilterChain struct {
	Filters []envoyTCPFilter `json:"filters"`
}
type envoyTCPFilter struct {
	Name   string         `json:"name"`
	Config envoyTCPConfig `json:"typed_config"`
}
type envoyTCPConfig struct {
	Type       string `json:"@type"`
	Cluster    string `json:"cluster"`
	StatPrefix string `json:"stat_prefix"`
}
type envoyUDPFilter struct {
	Name   string         `json:"name"`
	Config envoyUDPConfig `json:"typed_config"`
}
type envoyUDPConfig struct {
	Type           string              `json:"@type"`
	Matcher        envoyUDPMatcher     `json:"matcher"`
	StatPrefix     string              `json:"stat_prefix"`
	UpstreamSocket envoyDatagramSocket `json:"upstream_socket_config"`
}
type envoyUDPMatcher struct {
	NoMatch envoyUDPMatchAction `json:"on_no_match"`
}
type envoyUDPMatchAction struct {
	Action envoyUDPRouteAction `json:"action"`
}
type envoyUDPRouteAction struct {
	Name   string        `json:"name"`
	Config envoyUDPRoute `json:"typed_config"`
}
type envoyUDPRoute struct {
	Type    string `json:"@type"`
	Cluster string `json:"cluster"`
}
type envoyCluster struct {
	ConnectTimeout  string          `json:"connect_timeout"`
	DNSLookupFamily string          `json:"dns_lookup_family"`
	Policy          string          `json:"lb_policy"`
	Assignment      envoyAssignment `json:"load_assignment"`
	Name            string          `json:"name"`
	Type            string          `json:"type"`
}
type envoyAssignment struct {
	ClusterName string          `json:"cluster_name"`
	Endpoints   []envoyLocality `json:"endpoints"`
}
type envoyLocality struct {
	Endpoints []envoyLoadEndpoint `json:"lb_endpoints"`
}
type envoyLoadEndpoint struct {
	Endpoint envoyEndpoint `json:"endpoint"`
}
type envoyEndpoint struct {
	Address envoyAddress `json:"address"`
}

func renderEnvoy(configuration Configuration) ([]byte, error) {
	encoded, err := json.Marshal(envoyConfigurationDocument(configuration))
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return encoded, nil
}

func envoyConfigurationDocument(configuration Configuration) envoyDocument {
	document := envoyDocument{Admin: envoyAdmin{Address: envoySocketAddress("127.0.0.1", 9901, TCP)}}
	document.StaticResources.Listeners = make([]envoyListener, 0, len(configuration.Ports))
	document.StaticResources.Clusters = make([]envoyCluster, 0, len(configuration.Ports))
	for _, port := range configuration.Ports {
		name := listenerName(configuration, port)
		listener := envoyListener{Name: name, Address: envoySocketAddress("0.0.0.0", port.Number, port.Protocol)}
		if port.Protocol == TCP {
			listener.FilterChains = []envoyFilterChain{{Filters: []envoyTCPFilter{{
				Name: "envoy.filters.network.tcp_proxy", Config: envoyTCPConfig{
					Type:       "type.googleapis.com/envoy.extensions.filters.network.tcp_proxy.v3.TcpProxy",
					StatPrefix: name, Cluster: name,
				},
			}}}}
		} else {
			// Preserve a UDP session's upstream. Switching a routing set is not a
			// claim that TCP draining also preserves UDP sessions.
			listener.UDPConfig = &envoyUDPListener{DownstreamSocket: envoyDatagramSocket{MaximumDatagramSize: 65535}}
			listener.ListenerFilters = []envoyUDPFilter{{Name: "envoy.filters.udp_listener.udp_proxy",
				Config: envoyUDPConfig{
					Type:       "type.googleapis.com/envoy.extensions.filters.udp.udp_proxy.v3.UdpProxyConfig",
					StatPrefix: name, UpstreamSocket: envoyDatagramSocket{MaximumDatagramSize: 65535},
					Matcher: envoyUDPMatcher{NoMatch: envoyUDPMatchAction{Action: envoyUDPRouteAction{
						Name: "route", Config: envoyUDPRoute{
							Type: "type.googleapis.com/envoy.extensions.filters.udp.udp_proxy.v3.Route", Cluster: name,
						},
					}}},
				},
			}}
		}
		endpoints := make([]envoyLoadEndpoint, len(configuration.Upstreams))
		for index, upstream := range configuration.Upstreams {
			endpoints[index] = envoyLoadEndpoint{
				Endpoint: envoyEndpoint{Address: envoySocketAddress(upstream, port.Number, port.Protocol)},
			}
		}
		document.StaticResources.Listeners = append(document.StaticResources.Listeners, listener)
		document.StaticResources.Clusters = append(document.StaticResources.Clusters, envoyCluster{
			Name: name, Type: "STRICT_DNS", ConnectTimeout: "5s", DNSLookupFamily: "V4_ONLY", Policy: "ROUND_ROBIN",
			Assignment: envoyAssignment{ClusterName: name, Endpoints: []envoyLocality{{Endpoints: endpoints}}},
		})
	}
	return document
}

func envoySocketAddress(address string, port uint16, protocol Protocol) envoyAddress {
	return envoyAddress{Socket: envoySocket{Protocol: strings.ToUpper(string(protocol)), Address: address, Port: port}}
}
