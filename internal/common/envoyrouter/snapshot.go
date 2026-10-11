package envoyrouter

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"

	bootstrapv3 "github.com/envoyproxy/go-control-plane/envoy/config/bootstrap/v3"
	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	listenerv3 "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	connectionv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"
	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	MaximumSourceBytes     = 4 * 1024 * 1024
	RuntimeDirectory       = "/tmp/groundplane-envoy-router"
	BootstrapTarget        = RuntimeDirectory + "/bootstrap.json"
	ClustersTarget         = RuntimeDirectory + "/clusters.json"
	RoutesTarget           = RuntimeDirectory + "/routes.json"
	ManifestTarget         = RuntimeDirectory + "/manifest.json"
	clusterType            = "type.googleapis.com/envoy.config.cluster.v3.Cluster"
	routeType              = "type.googleapis.com/envoy.config.route.v3.RouteConfiguration"
	routeConfigurationName = "gp_http_routes"
)

// Snapshot is opened from a captured compiled-provider file, never from latest
// desired state or an admin version label. Its four documents must describe the
// same complete router; accessors isolate the retained authority from callers.
type Snapshot struct {
	source        []byte
	bootstrapJSON []byte
	clustersJSON  []byte
	routesJSON    []byte
	manifestJSON  []byte
	bootstrap     *bootstrapv3.Bootstrap
	clusters      *discoveryv3.DiscoveryResponse
	routes        *discoveryv3.DiscoveryResponse
	digest        [sha256.Size]byte
}

func Read(value []byte) (Snapshot, error) {
	if len(value) == 0 || len(value) > MaximumSourceBytes {
		return Snapshot{}, invalidSnapshot()
	}
	parts := bytes.Split(value, []byte{'\n'})
	if len(parts) != 5 || len(parts[4]) != 0 {
		return Snapshot{}, invalidSnapshot()
	}
	bootstrap, manifest := &bootstrapv3.Bootstrap{}, &bootstrapv3.Bootstrap{}
	clusters, routes := &discoveryv3.DiscoveryResponse{}, &discoveryv3.DiscoveryResponse{}
	for index, target := range []proto.Message{bootstrap, clusters, routes, manifest} {
		if _, err := jcs.Canonicalize(parts[index]); err != nil {
			return Snapshot{}, invalidSnapshot()
		}
		if err := protojson.Unmarshal(parts[index], target); err != nil {
			return Snapshot{}, invalidSnapshot()
		}
	}
	if err := validateManifest(manifest); err != nil {
		return Snapshot{}, err
	}
	manifestDigest := sha256.Sum256(parts[3])
	version := "sha256:" + hex.EncodeToString(manifestDigest[:])
	if clusters.GetTypeUrl() != clusterType || routes.GetTypeUrl() != routeType ||
		clusters.GetVersionInfo() != version || routes.GetVersionInfo() != version ||
		clusters.GetNonce() != "" || routes.GetNonce() != "" ||
		clusters.GetControlPlane() != nil || routes.GetControlPlane() != nil ||
		len(clusters.Resources) != len(manifest.StaticResources.Clusters) || len(routes.Resources) != 1 {
		return Snapshot{}, invalidSnapshot()
	}
	for index, resource := range clusters.Resources {
		decoded := &clusterv3.Cluster{}
		if resource == nil || resource.UnmarshalTo(decoded) != nil ||
			!proto.Equal(decoded, manifest.StaticResources.Clusters[index]) {
			return Snapshot{}, invalidSnapshot()
		}
	}
	manager, err := readManager(manifest.StaticResources.Listeners[0])
	if err != nil {
		return Snapshot{}, err
	}
	decodedRoutes := &routev3.RouteConfiguration{}
	if routes.Resources[0] == nil || routes.Resources[0].UnmarshalTo(decodedRoutes) != nil ||
		!proto.Equal(decodedRoutes, manager.GetRouteConfig()) {
		return Snapshot{}, invalidSnapshot()
	}
	if !proto.Equal(
		clusters,
		&discoveryv3.DiscoveryResponse{VersionInfo: version, TypeUrl: clusterType, Resources: clusters.Resources},
	) ||
		!proto.Equal(
			routes,
			&discoveryv3.DiscoveryResponse{VersionInfo: version, TypeUrl: routeType, Resources: routes.Resources},
		) {
		return Snapshot{}, invalidSnapshot()
	}
	expectedBootstrap := proto.Clone(manifest).(*bootstrapv3.Bootstrap)
	expectedBootstrap.StaticResources.Clusters = nil
	expectedBootstrap.DynamicResources = &bootstrapv3.Bootstrap_DynamicResources{
		CdsConfig: filesystemSource(ClustersTarget),
	}
	manager.RouteSpecifier = &connectionv3.HttpConnectionManager_Rds{Rds: &connectionv3.Rds{
		ConfigSource: filesystemSource(RoutesTarget), RouteConfigName: routeConfigurationName,
	}}
	encodedManager, err := anypb.New(manager)
	if err != nil {
		return Snapshot{}, errs.Wrap(errs.KindInternal, err)
	}
	expectedBootstrap.StaticResources.Listeners[0].FilterChains[0].Filters[0].ConfigType =
		&listenerv3.Filter_TypedConfig{TypedConfig: encodedManager}
	if !sameBootstrap(bootstrap, expectedBootstrap) {
		return Snapshot{}, invalidSnapshot()
	}
	return Snapshot{source: bytes.Clone(value), bootstrapJSON: bytes.Clone(parts[0]),
		clustersJSON: bytes.Clone(parts[1]), routesJSON: bytes.Clone(parts[2]), manifestJSON: bytes.Clone(parts[3]),
		bootstrap: bootstrap, clusters: clusters, routes: routes, digest: sha256.Sum256(value)}, nil
}

func (snapshot Snapshot) Source() []byte            { return bytes.Clone(snapshot.source) }
func (snapshot Snapshot) Bootstrap() []byte         { return bytes.Clone(snapshot.bootstrapJSON) }
func (snapshot Snapshot) Clusters() []byte          { return bytes.Clone(snapshot.clustersJSON) }
func (snapshot Snapshot) Routes() []byte            { return bytes.Clone(snapshot.routesJSON) }
func (snapshot Snapshot) Manifest() []byte          { return bytes.Clone(snapshot.manifestJSON) }
func (snapshot Snapshot) Digest() [sha256.Size]byte { return snapshot.digest }

func readManager(listener *listenerv3.Listener) (*connectionv3.HttpConnectionManager, error) {
	if listener == nil || len(listener.FilterChains) != 1 || listener.FilterChains[0] == nil ||
		len(listener.FilterChains[0].Filters) != 1 || listener.FilterChains[0].Filters[0] == nil ||
		listener.FilterChains[0].Filters[0].Name != "envoy.filters.network.http_connection_manager" {
		return nil, invalidSnapshot()
	}
	manager := &connectionv3.HttpConnectionManager{}
	configuration := listener.FilterChains[0].Filters[0].GetTypedConfig()
	if configuration == nil || configuration.UnmarshalTo(manager) != nil {
		return nil, invalidSnapshot()
	}
	return manager, nil
}

// Compare the typed connection manager separately: protobuf Any's opaque bytes
// are not a substitute for comparing the contained configuration semantically.
func sameListener(actual, expected *listenerv3.Listener) bool {
	actualManager, actualErr := readManager(actual)
	expectedManager, expectedErr := readManager(expected)
	if actualErr != nil || expectedErr != nil || !proto.Equal(actualManager, expectedManager) {
		return false
	}
	actualCopy, expectedCopy := proto.Clone(actual).(*listenerv3.Listener), proto.Clone(expected).(*listenerv3.Listener)
	actualCopy.FilterChains[0].Filters[0].ConfigType = nil
	expectedCopy.FilterChains[0].Filters[0].ConfigType = nil
	return proto.Equal(actualCopy, expectedCopy)
}

func sameBootstrap(actual, expected *bootstrapv3.Bootstrap) bool {
	if actual == nil || actual.GetStaticResources() == nil || expected.GetStaticResources() == nil ||
		len(actual.StaticResources.Listeners) != 1 || len(expected.StaticResources.Listeners) != 1 ||
		!sameListener(actual.StaticResources.Listeners[0], expected.StaticResources.Listeners[0]) {
		return false
	}
	actualCopy, expectedCopy := proto.Clone(actual).(*bootstrapv3.Bootstrap), proto.Clone(expected).(*bootstrapv3.Bootstrap)
	actualCopy.StaticResources.Listeners = nil
	expectedCopy.StaticResources.Listeners = nil
	return proto.Equal(actualCopy, expectedCopy)
}

func filesystemSource(path string) *corev3.ConfigSource {
	return &corev3.ConfigSource{ResourceApiVersion: corev3.ApiVersion_V3,
		ConfigSourceSpecifier: &corev3.ConfigSource_PathConfigSource{
			PathConfigSource: &corev3.PathConfigSource{Path: path},
		}}
}

func socketAddress(host string, port uint32) *corev3.Address {
	return &corev3.Address{Address: &corev3.Address_SocketAddress{SocketAddress: &corev3.SocketAddress{
		Address: host, Protocol: corev3.SocketAddress_TCP, PortSpecifier: &corev3.SocketAddress_PortValue{PortValue: port},
	}}}
}

func invalidSnapshot() error {
	return errs.New(
		errs.KindStateConflict,
		"captured HTTP router configuration files do not describe one complete routing snapshot",
	)
}

func zeroTimeout() *durationpb.Duration { return durationpb.New(0) }
