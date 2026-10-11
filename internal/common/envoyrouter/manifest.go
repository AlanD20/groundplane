package envoyrouter

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"time"

	bootstrapv3 "github.com/envoyproxy/go-control-plane/envoy/config/bootstrap/v3"
	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	endpointv3 "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	listenerv3 "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	routerv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/router/v3"
	connectionv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/AlanD20/groundplane/internal/common/dnsname"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Generated protocol types close the decoding boundary, while explicit expected
// shapes close the feature boundary. A valid Envoy plugin/config is not thereby
// an admitted Groundplane HTTP router recipe.
func validateManifest(manifest *bootstrapv3.Bootstrap) error {
	if manifest.Admin == nil ||
		!proto.Equal(manifest.Admin, &bootstrapv3.Admin{Address: socketAddress("127.0.0.1", 9901)}) ||
		manifest.GetStaticResources() == nil ||
		len(manifest.StaticResources.Listeners) != 1 {
		return invalidSnapshot()
	}
	listener := manifest.StaticResources.Listeners[0]
	manager, err := readManager(listener)
	if err != nil || manager.GetRouteConfig() == nil {
		return invalidSnapshot()
	}
	filter, err := anypb.New(&routerv3.Router{})
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	expectedManager := &connectionv3.HttpConnectionManager{StatPrefix: "gp_http",
		RouteSpecifier: &connectionv3.HttpConnectionManager_RouteConfig{RouteConfig: manager.GetRouteConfig()},
		HttpFilters: []*connectionv3.HttpFilter{{Name: "envoy.filters.http.router",
			ConfigType: &connectionv3.HttpFilter_TypedConfig{TypedConfig: filter}}},
		UpgradeConfigs:    []*connectionv3.HttpConnectionManager_UpgradeConfig{{UpgradeType: "websocket"}},
		StreamIdleTimeout: zeroTimeout(),
	}
	if !proto.Equal(manager, expectedManager) {
		return invalidSnapshot()
	}
	// Preserve only the admitted HTTP listener and selected typed manager, then
	// compare the complete Bootstrap to reject extra listeners/resources/plugins.
	expectedListener := &listenerv3.Listener{Name: "gp_http", Address: socketAddress("0.0.0.0", 80),
		FilterChains: []*listenerv3.FilterChain{{Filters: []*listenerv3.Filter{{
			Name: "envoy.filters.network.http_connection_manager", ConfigType: listener.FilterChains[0].Filters[0].ConfigType,
		}}}}}
	if !sameListener(listener, expectedListener) {
		return invalidSnapshot()
	}
	expected := &bootstrapv3.Bootstrap{Admin: &bootstrapv3.Admin{Address: socketAddress("127.0.0.1", 9901)},
		StaticResources: &bootstrapv3.Bootstrap_StaticResources{Listeners: []*listenerv3.Listener{expectedListener},
			Clusters: manifest.StaticResources.Clusters}}
	if !sameBootstrap(manifest, expected) {
		return invalidSnapshot()
	}
	clusters := make(map[string]struct{}, len(manifest.StaticResources.Clusters))
	previousName := ""
	for _, cluster := range manifest.StaticResources.Clusters {
		if cluster == nil || cluster.Name <= previousName || !validCluster(cluster) {
			return invalidSnapshot()
		}
		previousName = cluster.Name
		clusters[cluster.Name] = struct{}{}
	}
	return validateRoutes(manager.GetRouteConfig(), clusters)
}

func validCluster(cluster *clusterv3.Cluster) bool {
	if !strings.HasPrefix(cluster.Name, "gp_backend_") || len(cluster.Name) != len("gp_backend_")+sha256.Size*2 {
		return false
	}
	if _, err := hex.DecodeString(strings.TrimPrefix(cluster.Name, "gp_backend_")); err != nil {
		return false
	}
	assignment := cluster.GetLoadAssignment()
	if assignment == nil || len(assignment.Endpoints) != 1 || assignment.Endpoints[0] == nil ||
		len(assignment.Endpoints[0].LbEndpoints) != 1 || assignment.Endpoints[0].LbEndpoints[0] == nil {
		return false
	}
	address := assignment.Endpoints[0].LbEndpoints[0].GetEndpoint().GetAddress()
	socket := address.GetSocketAddress()
	if socket == nil || !validServiceName(socket.Address) || socket.GetPortValue() == 0 ||
		socket.GetPortValue() > 65535 {
		return false
	}
	expected := &clusterv3.Cluster{Name: cluster.Name,
		ClusterDiscoveryType: &clusterv3.Cluster_Type{Type: clusterv3.Cluster_STRICT_DNS},
		ConnectTimeout:       durationpb.New(5 * time.Second), DnsLookupFamily: clusterv3.Cluster_V4_ONLY,
		LoadAssignment: &endpointv3.ClusterLoadAssignment{ClusterName: cluster.Name,
			Endpoints: []*endpointv3.LocalityLbEndpoints{{LbEndpoints: []*endpointv3.LbEndpoint{{
				HostIdentifier: &endpointv3.LbEndpoint_Endpoint{Endpoint: &endpointv3.Endpoint{
					Address: socketAddress(socket.Address, socket.GetPortValue()),
				}},
			}}}},
		},
	}
	return proto.Equal(cluster, expected)
}

func validateRoutes(configuration *routev3.RouteConfiguration, clusters map[string]struct{}) error {
	if configuration == nil || configuration.Name != routeConfigurationName || len(configuration.VirtualHosts) == 0 {
		return invalidSnapshot()
	}
	expected := &routev3.RouteConfiguration{Name: routeConfigurationName, VirtualHosts: configuration.VirtualHosts,
		ValidateClusters: wrapperspb.Bool(true), IgnorePortInHostMatching: true}
	if !proto.Equal(configuration, expected) {
		return invalidSnapshot()
	}
	domains := make(map[string]struct{}, len(configuration.VirtualHosts))
	used := make(map[string]struct{}, len(clusters))
	for _, host := range configuration.VirtualHosts {
		if host == nil || len(host.Domains) != 1 || len(host.Routes) == 0 {
			return invalidSnapshot()
		}
		domain := host.Domains[0]
		hash := sha256.Sum256([]byte(domain))
		name := "gp_host_" + hex.EncodeToString(hash[:])
		if host.Name == "gp_unmatched_host" && domain == "*" && len(host.Routes) == 1 {
			name = host.Name
		}
		if domain != "*" && !dnsname.Valid(domain) ||
			!proto.Equal(host, &routev3.VirtualHost{Name: name, Domains: []string{domain}, Routes: host.Routes}) {
			return invalidSnapshot()
		}
		if _, duplicate := domains[domain]; duplicate {
			return invalidSnapshot()
		}
		domains[domain] = struct{}{}
		for index, route := range host.Routes {
			if !validRoute(route, index == len(host.Routes)-1, clusters, used) {
				return invalidSnapshot()
			}
		}
	}
	if _, found := domains["*"]; !found || len(used) != len(clusters) {
		return invalidSnapshot()
	}
	return nil
}

func validRoute(route *routev3.Route, last bool, clusters, used map[string]struct{}) bool {
	if route == nil {
		return false
	}
	if last {
		return proto.Equal(route, &routev3.Route{Name: "gp_not_found", Match: &routev3.RouteMatch{
			PathSpecifier: &routev3.RouteMatch_Prefix{Prefix: "/"}},
			Action: &routev3.Route_DirectResponse{DirectResponse: &routev3.DirectResponseAction{Status: 404}}})
	}
	action := route.GetRoute()
	if route.Name == "" || route.Match == nil || action == nil {
		return false
	}
	if _, found := clusters[action.GetCluster()]; !found {
		return false
	}
	match := &routev3.RouteMatch{}
	switch path := route.Match.PathSpecifier.(type) {
	case *routev3.RouteMatch_Path:
		if !validPath(path.Path) {
			return false
		}
		match.PathSpecifier = &routev3.RouteMatch_Path{Path: path.Path}
	case *routev3.RouteMatch_Prefix:
		if !validPath(path.Prefix) {
			return false
		}
		match.PathSpecifier = &routev3.RouteMatch_Prefix{Prefix: path.Prefix}
	default:
		return false
	}
	expected := &routev3.Route{Name: route.Name, Match: match, Action: &routev3.Route_Route{
		Route: &routev3.RouteAction{
			ClusterSpecifier: &routev3.RouteAction_Cluster{Cluster: action.GetCluster()},
			Timeout:          zeroTimeout(),
		}}}
	if !proto.Equal(route, expected) {
		return false
	}
	used[action.GetCluster()] = struct{}{}
	return true
}

func validPath(value string) bool {
	return strings.HasPrefix(value, "/") && !strings.ContainsAny(value, "\x00\r\n\t *?#")
}

func validServiceName(value string) bool {
	if value == "" {
		return false
	}
	for _, character := range value {
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' || character == '_' || character == '-' || character == '.' {
			continue
		}
		return false
	}
	return true
}
