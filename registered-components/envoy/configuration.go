package envoy

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/AlanD20/groundplane-component-sdk/component"
)

const (
	clusterType            = "type.googleapis.com/envoy.config.cluster.v3.Cluster"
	routeType              = "type.googleapis.com/envoy.config.route.v3.RouteConfiguration"
	connectionManagerType  = "type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager"
	routerFilterType       = "type.googleapis.com/envoy.extensions.filters.http.router.v3.Router"
	routeConfigurationName = "gp_http_routes"
)

type bootstrap struct {
	Admin   admin             `json:"admin"`
	Dynamic *dynamicResources `json:"dynamic_resources,omitempty"`
	Static  resources         `json:"static_resources"`
}
type admin struct {
	Address address `json:"address"`
}
type address struct {
	Socket socket `json:"socket_address"`
}
type socket struct {
	Address  string `json:"address"`
	Port     uint16 `json:"port_value"`
	Protocol string `json:"protocol"`
}
type resources struct {
	Listeners []listener `json:"listeners"`
	Clusters  []cluster  `json:"clusters"`
}
type listener struct {
	Name    string        `json:"name"`
	Address address       `json:"address"`
	Chains  []filterChain `json:"filter_chains"`
}
type filterChain struct {
	Filters []networkFilter `json:"filters"`
}
type networkFilter struct {
	Name          string            `json:"name"`
	Configuration connectionManager `json:"typed_config"`
}
type connectionManager struct {
	Type              string              `json:"@type"`
	Codec             string              `json:"codec_type"`
	StatPrefix        string              `json:"stat_prefix"`
	Routes            *routeConfiguration `json:"route_config,omitempty"`
	Discovery         *routeDiscovery     `json:"rds,omitempty"`
	Filters           []httpFilter        `json:"http_filters"`
	Upgrades          []upgrade           `json:"upgrade_configs"`
	StreamIdleTimeout string              `json:"stream_idle_timeout"`
}
type httpFilter struct {
	Name          string         `json:"name"`
	Configuration typedExtension `json:"typed_config"`
}
type typedExtension struct {
	Type string `json:"@type"`
}
type upgrade struct {
	Type string `json:"upgrade_type"`
}
type routeConfiguration struct {
	Name             string        `json:"name"`
	VirtualHosts     []virtualHost `json:"virtual_hosts"`
	ValidateClusters bool          `json:"validate_clusters"`
	IgnoreHostPort   bool          `json:"ignore_port_in_host_matching"`
}
type virtualHost struct {
	Name    string   `json:"name"`
	Domains []string `json:"domains"`
	Routes  []route  `json:"routes"`
}
type route struct {
	Name    string          `json:"name"`
	Match   routeMatch      `json:"match"`
	Forward *routeAction    `json:"route,omitempty"`
	Direct  *directResponse `json:"direct_response,omitempty"`
}
type routeMatch struct {
	Path   string `json:"path,omitempty"`
	Prefix string `json:"prefix,omitempty"`
}
type routeAction struct {
	Cluster string `json:"cluster"`
	Timeout string `json:"timeout"`
}
type directResponse struct {
	Status uint16 `json:"status"`
}
type cluster struct {
	Name           string     `json:"name"`
	Type           string     `json:"type"`
	ConnectTimeout string     `json:"connect_timeout"`
	LookupFamily   string     `json:"dns_lookup_family"`
	Policy         string     `json:"lb_policy"`
	Assignment     assignment `json:"load_assignment"`
}
type assignment struct {
	Name      string     `json:"cluster_name"`
	Endpoints []locality `json:"endpoints"`
}
type locality struct {
	Endpoints []loadEndpoint `json:"lb_endpoints"`
}
type loadEndpoint struct {
	Endpoint endpoint `json:"endpoint"`
}
type endpoint struct {
	Address address `json:"address"`
}
type dynamicResources struct {
	Clusters configSource `json:"cds_config"`
}
type configSource struct {
	Path    pathSource `json:"path_config_source"`
	Version string     `json:"resource_api_version"`
}
type pathSource struct {
	Path string `json:"path"`
}
type routeDiscovery struct {
	Source configSource `json:"config_source"`
	Name   string       `json:"route_config_name"`
}
type clusterResource struct {
	TypeURL string `json:"@type"`
	cluster
}
type routesResource struct {
	TypeURL string `json:"@type"`
	routeConfiguration
}
type clusterResponse struct {
	Version   string            `json:"version_info"`
	TypeURL   string            `json:"type_url"`
	Resources []clusterResource `json:"resources"`
}
type routeResponse struct {
	Version   string           `json:"version_info"`
	TypeURL   string           `json:"type_url"`
	Resources []routesResource `json:"resources"`
}

// One immutable source binds bootstrap, CDS, RDS and the complete offline
// validation configuration. No mutable tag or runtime-derived endpoint is used.
func renderConfiguration(routes []component.HTTPRoute) ([]byte, error) {
	routeSet, clusters, err := buildRouteResources(routes)
	if err != nil {
		return nil, err
	}
	manager := connectionManager{Type: connectionManagerType, Codec: "AUTO", StatPrefix: "gp_http",
		Routes: &routeSet, Filters: []httpFilter{{Name: "envoy.filters.http.router", Configuration: typedExtension{Type: routerFilterType}}},
		Upgrades: []upgrade{{Type: "websocket"}}, StreamIdleTimeout: "0s"}
	manifest := bootstrap{Admin: admin{Address: socketAddress("127.0.0.1", 9901)},
		Static: resources{
			Clusters: clusters,
			Listeners: []listener{{Name: "gp_http", Address: socketAddress("0.0.0.0", 80),
				Chains: []filterChain{
					{
						Filters: []networkFilter{
							{Name: "envoy.filters.network.http_connection_manager", Configuration: manager},
						},
					},
				}}},
		}}
	manifestJSON, err := json.Marshal(manifest)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(manifestJSON)
	version := "sha256:" + hex.EncodeToString(digest[:])
	clusterSet := make([]clusterResource, len(clusters))
	for index, value := range clusters {
		clusterSet[index] = clusterResource{TypeURL: clusterType, cluster: value}
	}
	cds, err := json.Marshal(clusterResponse{Version: version, TypeURL: clusterType, Resources: clusterSet})
	if err != nil {
		return nil, err
	}
	rds, err := json.Marshal(routeResponse{Version: version, TypeURL: routeType,
		Resources: []routesResource{{TypeURL: routeType, routeConfiguration: routeSet}}})
	if err != nil {
		return nil, err
	}
	// Replace only the listener's route source. Its socket/filter identity is
	// stable as Routes change; activation must acknowledge CDS before RDS.
	manager.Routes = nil
	manager.Discovery = &routeDiscovery{Source: filesystemSource(RoutesTarget), Name: routeConfigurationName}
	manifest.Static.Listeners[0].Chains[0].Filters[0].Configuration = manager
	manifest.Static.Clusters = []cluster{}
	manifest.Dynamic = &dynamicResources{Clusters: filesystemSource(ClustersTarget)}
	bootstrapJSON, err := json.Marshal(manifest)
	if err != nil {
		return nil, err
	}
	content := make([]byte, 0, len(bootstrapJSON)+len(cds)+len(rds)+len(manifestJSON)+4)
	for _, value := range [][]byte{bootstrapJSON, cds, rds, manifestJSON} {
		content = append(content, value...)
		content = append(content, '\n')
	}
	return content, nil
}

func buildRouteResources(routes []component.HTTPRoute) (routeConfiguration, []cluster, error) {
	result := routeConfiguration{Name: routeConfigurationName, VirtualHosts: []virtualHost{},
		ValidateClusters: true, IgnoreHostPort: true}
	clusters := make(map[string]cluster)
	wildcard := false
	for start := 0; start < len(routes); {
		end := start + 1
		for end < len(routes) && routes[start].Host == routes[end].Host {
			end++
		}
		domain := routes[start].Host
		if domain == "" {
			domain, wildcard = "*", true
		}
		host := virtualHost{Name: "gp_host_" + digestName(domain), Domains: []string{domain}, Routes: []route{}}
		for _, input := range routes[start:end] {
			if strings.Contains(input.Host, "*") || strings.ContainsAny(input.Host, ":/?#@") ||
				strings.Contains(input.Path, "*") &&
					(strings.Count(input.Path, "*") != 1 || !strings.HasSuffix(input.Path, "*")) {
				return routeConfiguration{}, nil, fmt.Errorf(
					"envoy: Route host or path is not an exact host and terminal-wildcard path",
				)
			}
			name := "gp_backend_" + digestName(
				input.BackendServiceID+"\x00"+input.BackendServiceName+fmt.Sprintf("\x00%d", input.TargetPort),
			)
			clusters[name] = cluster{
				Name:           name,
				Type:           "STRICT_DNS",
				ConnectTimeout: "5s",
				LookupFamily:   "V4_ONLY",
				Policy:         "ROUND_ROBIN",
				Assignment: assignment{Name: name, Endpoints: []locality{{Endpoints: []loadEndpoint{{
					Endpoint: endpoint{Address: socketAddress(input.BackendServiceName, input.TargetPort)},
				}}}}},
			}
			match := routeMatch{Path: input.Path}
			if input.Path == "/" || strings.HasSuffix(input.Path, "*") {
				match = routeMatch{Prefix: strings.TrimSuffix(input.Path, "*")}
			}
			host.Routes = append(host.Routes, route{Name: input.ID, Match: match,
				Forward: &routeAction{Cluster: name, Timeout: "0s"}})
		}
		host.Routes = append(host.Routes, notFoundRoute())
		result.VirtualHosts = append(result.VirtualHosts, host)
		start = end
	}
	if !wildcard {
		result.VirtualHosts = append(result.VirtualHosts, virtualHost{Name: "gp_unmatched_host", Domains: []string{"*"},
			Routes: []route{notFoundRoute()}})
	}
	names := make([]string, 0, len(clusters))
	for name := range clusters {
		names = append(names, name)
	}
	sort.Strings(names)
	ordered := make([]cluster, len(names))
	for index, name := range names {
		ordered[index] = clusters[name]
	}
	return result, ordered, nil
}

func notFoundRoute() route {
	return route{Name: "gp_not_found", Match: routeMatch{Prefix: "/"}, Direct: &directResponse{Status: 404}}
}

func filesystemSource(target string) configSource {
	return configSource{Path: pathSource{Path: target}, Version: "V3"}
}

func socketAddress(host string, port uint16) address {
	return address{Socket: socket{Address: host, Port: port, Protocol: "TCP"}}
}

func digestName(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}
