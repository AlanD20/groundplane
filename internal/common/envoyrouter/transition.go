package envoyrouter

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"

	clusterv3 "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Transition retains both captured snapshots and a dependency-preserving CDS
// bridge. Executors must prove each publication active before advancing: union
// clusters, desired Routes, then desired clusters. Reversing the same inputs
// constructs compensation without consulting current desired state.
type Transition struct {
	previous   Snapshot
	desired    Snapshot
	bridge     *discoveryv3.DiscoveryResponse
	bridgeJSON []byte
}

func PrepareTransition(previous, desired Snapshot) (Transition, error) {
	if len(previous.source) == 0 || len(desired.source) == 0 ||
		!sameBootstrap(previous.bootstrap, desired.bootstrap) {
		return Transition{}, errs.New(errs.KindStateConflict,
			"HTTP router listener changed; replace the router through the protected Environment proxy switch")
	}
	resources := make(map[string]*anypb.Any, len(previous.clusters.Resources)+len(desired.clusters.Resources))
	for _, snapshot := range []Snapshot{previous, desired} {
		for _, resource := range snapshot.clusters.Resources {
			cluster := &clusterv3.Cluster{}
			if resource.UnmarshalTo(cluster) != nil {
				return Transition{}, invalidSnapshot()
			}
			if old, exists := resources[cluster.Name]; exists {
				oldCluster := &clusterv3.Cluster{}
				if old.UnmarshalTo(oldCluster) != nil || !proto.Equal(oldCluster, cluster) {
					return Transition{}, errs.New(errs.KindStateConflict,
						"HTTP router backend identity names different captured endpoints")
				}
				continue
			}
			resources[cluster.Name] = proto.Clone(resource).(*anypb.Any)
		}
	}
	names := make([]string, 0, len(resources))
	for name := range resources {
		names = append(names, name)
	}
	sort.Strings(names)
	bridge := &discoveryv3.DiscoveryResponse{TypeUrl: clusterType}
	for _, name := range names {
		bridge.Resources = append(bridge.Resources, resources[name])
	}
	options := protojson.MarshalOptions{UseProtoNames: true}
	identity, err := options.Marshal(bridge)
	if err != nil {
		return Transition{}, errs.Wrap(errs.KindInternal, err)
	}
	// ProtoJSON explicitly does not promise stable bytes. The captured bridge
	// generation must be reproducible from the same ordered resource set.
	identity, err = jcs.Canonicalize(identity)
	if err != nil {
		return Transition{}, errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(identity)
	bridge.VersionInfo = "bridge:sha256:" + hex.EncodeToString(digest[:])
	encoded, err := options.Marshal(bridge)
	if err != nil {
		return Transition{}, errs.Wrap(errs.KindInternal, err)
	}
	encoded, err = jcs.Canonicalize(encoded)
	if err != nil {
		return Transition{}, errs.Wrap(errs.KindInternal, err)
	}
	return Transition{previous: previous, desired: desired, bridge: bridge, bridgeJSON: encoded}, nil
}

func (transition Transition) BridgeClusters() []byte {
	return append([]byte(nil), transition.bridgeJSON...)
}
func (transition Transition) Desired() Snapshot  { return transition.desired }
func (transition Transition) Previous() Snapshot { return transition.previous }
