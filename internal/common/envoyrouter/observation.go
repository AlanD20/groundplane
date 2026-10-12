package envoyrouter

import (
	"bytes"
	"crypto/sha256"

	adminv3 "github.com/envoyproxy/go-control-plane/envoy/admin/v3"
	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	listenerv3 "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	routev3 "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/AlanD20/groundplane/internal/common/envoyadmin"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const MaximumObservationBytes = 4 * MaximumSourceBytes

// Restart files are returned in the compiled source's four-line order, followed
// by the live admin response. Exec completion, exact container ownership/image,
// non-TTY framing and output bounds remain the infrastructure caller's duty.
const ObservationCommand = "test \"$(tr '\\000' '\\n' < /proc/1/cmdline)\" = 'envoy\n--config-path\n" +
	BootstrapTarget + "\n--disable-hot-restart\n--service-node\ngroundplane-http-router\n" +
	"--service-cluster\ngroundplane-http-router'; " +
	"test ! -L " + RuntimeDirectory + "; test ! -L " + BootstrapTarget + "; " +
	"test ! -L " + ClustersTarget + "; test ! -L " + RoutesTarget + "; test ! -L " + ManifestTarget + "; " +
	"cat " + BootstrapTarget + " " + ClustersTarget + " " + RoutesTarget + " " + ManifestTarget + "; " + envoyadmin.ReadCommand

type TransitionPhase string

const (
	BridgeClustersActive  TransitionPhase = "bridge_clusters_active"
	DesiredRoutesActive   TransitionPhase = "desired_routes_active"
	DesiredSnapshotActive TransitionPhase = "desired_snapshot_active"
)

func VerifyObservation(value, expectedDigest []byte) (bool, error) {
	if len(value) == 0 || len(value) > MaximumObservationBytes || len(expectedDigest) != sha256.Size {
		return false, invalidObservation()
	}
	parts := bytes.SplitN(value, []byte{'\n'}, 5)
	if len(parts) != 5 {
		return false, invalidObservation()
	}
	source := bytes.Join(parts[:4], []byte{'\n'})
	source = append(source, '\n')
	snapshot, err := Read(source)
	if err != nil {
		return false, invalidObservation()
	}
	if !bytes.Equal(snapshot.digest[:], expectedDigest) {
		return false, nil
	}
	body, err := envoyadmin.ReadResponse(parts[4], MaximumObservationBytes)
	if err != nil {
		return false, err
	}
	return snapshot.VerifyActiveConfiguration(body)
}

func (snapshot Snapshot) VerifyActiveConfiguration(body []byte) (bool, error) {
	if len(snapshot.source) == 0 {
		return false, invalidSnapshot()
	}
	return verifyActive(
		body,
		snapshot.bootstrap.StaticResources.Listeners[0],
		snapshot.clusters,
		snapshot.routes,
		false,
	)
}

// Intermediate proof still covers the entire listener, cluster and Route set.
// A matching version label alone cannot authorize the next publication.
func (transition Transition) VerifyActiveConfiguration(body []byte, phase TransitionPhase) (bool, error) {
	if transition.bridge == nil || len(transition.previous.source) == 0 || len(transition.desired.source) == 0 {
		return false, invalidSnapshot()
	}
	clusters, routes := transition.bridge, transition.previous.routes
	switch phase {
	case BridgeClustersActive:
	case DesiredRoutesActive:
		routes = transition.desired.routes
	case DesiredSnapshotActive:
		clusters, routes = transition.desired.clusters, transition.desired.routes
	default:
		return false, errs.New(errs.KindInternal, "HTTP router discovery transition phase is invalid")
	}
	return verifyActive(body, transition.desired.bootstrap.StaticResources.Listeners[0], clusters, routes, false)
}

func verifyActive(
	body []byte,
	listener *listenerv3.Listener,
	clusters, routes *discoveryv3.DiscoveryResponse,
	allowRejectedUpdate bool,
) (bool, error) {
	if len(body) == 0 || len(body) > MaximumObservationBytes {
		return false, invalidObservation()
	}
	if _, err := jcs.Canonicalize(body); err != nil {
		return false, invalidObservation()
	}
	dump := &adminv3.ConfigDump{}
	if protojson.Unmarshal(body, dump) != nil || len(dump.Configs) > 16 {
		return false, invalidObservation()
	}
	clustersSeen, routesSeen, listenersSeen := false, false, false
	for _, resource := range dump.Configs {
		if resource == nil {
			return false, invalidObservation()
		}
		switch {
		case resource.MessageIs(&adminv3.ClustersConfigDump{}):
			actual := &adminv3.ClustersConfigDump{}
			if clustersSeen || resource.UnmarshalTo(actual) != nil {
				return false, invalidObservation()
			}
			if !clustersMatch(actual, clusters, allowRejectedUpdate) {
				return false, nil
			}
			clustersSeen = true
		case resource.MessageIs(&adminv3.ListenersConfigDump{}):
			actual := &adminv3.ListenersConfigDump{}
			if listenersSeen || resource.UnmarshalTo(actual) != nil {
				return false, invalidObservation()
			}
			if len(actual.DynamicListeners) != 0 || len(actual.StaticListeners) != 1 ||
				actual.StaticListeners[0] == nil ||
				actual.StaticListeners[0].Listener == nil {
				return false, nil
			}
			actualListener := &listenerv3.Listener{}
			if actual.StaticListeners[0].Listener.UnmarshalTo(actualListener) != nil {
				return false, invalidObservation()
			}
			if !sameListener(actualListener, listener) {
				return false, nil
			}
			listenersSeen = true
		case resource.MessageIs(&adminv3.RoutesConfigDump{}):
			actual := &adminv3.RoutesConfigDump{}
			if routesSeen || resource.UnmarshalTo(actual) != nil {
				return false, invalidObservation()
			}
			if len(actual.StaticRouteConfigs) != 0 || len(actual.DynamicRouteConfigs) != 1 {
				return false, nil
			}
			entry := actual.DynamicRouteConfigs[0]
			if entry == nil || entry.RouteConfig == nil || entry.VersionInfo != routes.VersionInfo ||
				entry.ErrorState != nil && !allowRejectedUpdate {
				return false, nil
			}
			actualRoutes, expectedRoutes := &routev3.RouteConfiguration{}, &routev3.RouteConfiguration{}
			if entry.RouteConfig.UnmarshalTo(actualRoutes) != nil ||
				routes.Resources[0].UnmarshalTo(expectedRoutes) != nil {
				return false, invalidObservation()
			}
			if !proto.Equal(actualRoutes, expectedRoutes) {
				return false, nil
			}
			routesSeen = true
		}
	}
	if !clustersSeen || !routesSeen || !listenersSeen {
		return false, nil
	}
	return true, nil
}

func clustersMatch(
	actual *adminv3.ClustersConfigDump,
	expected *discoveryv3.DiscoveryResponse,
	allowRejectedUpdate bool,
) bool {
	if actual.VersionInfo != expected.VersionInfo || len(actual.StaticClusters) != 0 ||
		len(actual.DynamicWarmingClusters) != 0 || len(actual.DynamicActiveClusters) != len(expected.Resources) {
		return false
	}
	byName := make(map[string]*clusterv3.Cluster, len(expected.Resources))
	for _, resource := range expected.Resources {
		cluster := &clusterv3.Cluster{}
		if resource.UnmarshalTo(cluster) != nil {
			return false
		}
		byName[cluster.Name] = cluster
	}
	for _, entry := range actual.DynamicActiveClusters {
		if entry == nil || entry.Cluster == nil || entry.VersionInfo != expected.VersionInfo ||
			entry.ErrorState != nil && !allowRejectedUpdate {
			return false
		}
		cluster := &clusterv3.Cluster{}
		if entry.Cluster.UnmarshalTo(cluster) != nil || !proto.Equal(cluster, byName[cluster.Name]) {
			return false
		}
		delete(byName, cluster.Name)
	}
	return len(byName) == 0
}

func invalidObservation() error {
	return errs.New(
		errs.KindRequestFailed,
		"HTTP router active Routes, listeners and backends could not be read completely",
	)
}
