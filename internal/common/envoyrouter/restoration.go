package envoyrouter

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"

	discoveryv3 "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/AlanD20/groundplane/internal/common/envoyadmin"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Restoration admits only restart-file states reachable by this captured update
// or by its own compensation. A rejected/asynchronous forward publication need
// not become active before recovery can put back the prior Routes. Live proof
// still covers a complete captured listener, cluster set and Route set.
type Restoration struct {
	prior              Snapshot
	failed             Snapshot
	transition         Transition
	paths              [][]activationState
	identity           [sha256.Size]byte
	recoveryRoutes     *discoveryv3.DiscoveryResponse
	recoveryRoutesJSON []byte
}

type RestorationProgress struct {
	identity [sha256.Size]byte
	path     int
	index    int
	active   bool
	restored bool
}

func (progress RestorationProgress) Restored() bool { return progress.restored }
func (progress RestorationProgress) ReadyToPublish() bool {
	return progress.active && !progress.restored
}

func PrepareRestoration(priorSource, failedSource []byte) (Restoration, error) {
	if len(priorSource) == 0 {
		return Restoration{}, errs.New(errs.KindStateConflict,
			"new HTTP router recovery must prove candidate absence, not invent a prior configuration")
	}
	forward, err := PrepareActivation(priorSource, failedSource)
	if err != nil {
		return Restoration{}, err
	}
	prior, err := Read(priorSource)
	if err != nil {
		return Restoration{}, err
	}
	transition, err := PrepareTransition(forward.desired, prior)
	if err != nil {
		return Restoration{}, err
	}
	identity := append([]byte("restore\x00"), priorSource...)
	identity = append(identity, 0)
	identity = append(identity, failedSource...)
	restoration := Restoration{prior: prior, failed: forward.desired, transition: transition,
		identity: sha256.Sum256(identity)}
	restoration.recoveryRoutes = proto.Clone(prior.routes).(*discoveryv3.DiscoveryResponse)
	restoration.recoveryRoutes.VersionInfo = "restore:sha256:" + hex.EncodeToString(restoration.identity[:])
	encodedRoutes, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(restoration.recoveryRoutes)
	if err != nil {
		return Restoration{}, errs.Wrap(errs.KindInternal, err)
	}
	restoration.recoveryRoutesJSON, err = jcs.Canonicalize(encodedRoutes)
	if err != nil {
		return Restoration{}, errs.Wrap(errs.KindInternal, err)
	}
	for _, start := range forward.states {
		// The start is captured by the forward program. Publication follows the
		// same dependency order from each possible interruption, preserving
		// both backend sets until the prior Routes are acknowledged active.
		path := Activation{states: []activationState{start}}
		path.appendPublication(1, ClustersTarget, transition.BridgeClusters(), BridgeClustersActive)
		path.appendPublication(2, RoutesTarget, restoration.recoveryRoutesJSON, DesiredRoutesActive)
		path.appendPublication(1, ClustersTarget, prior.Clusters(), DesiredSnapshotActive)
		path.appendPublication(2, RoutesTarget, prior.Routes(), DesiredSnapshotActive)
		path.appendPublication(0, BootstrapTarget, prior.Bootstrap(), DesiredSnapshotActive)
		path.appendPublication(3, ManifestTarget, prior.Manifest(), DesiredSnapshotActive)
		restoration.paths = append(restoration.paths, path.states)
	}
	return restoration, nil
}

func (restoration Restoration) Observe(value []byte) (RestorationProgress, error) {
	if len(restoration.paths) == 0 || len(value) == 0 || len(value) > MaximumObservationBytes {
		return RestorationProgress{}, invalidObservation()
	}
	parts := bytes.SplitN(value, []byte{'\n'}, 5)
	if len(parts) != 5 {
		return RestorationProgress{}, invalidObservation()
	}
	source := bytes.Join(parts[:4], []byte{'\n'})
	source = append(source, '\n')
	body, err := envoyadmin.ReadResponse(parts[4], MaximumObservationBytes)
	if err != nil {
		return RestorationProgress{}, err
	}
	// Final files can also be the start of a rejected/outstanding update.
	// A temporary captured recovery generation makes acknowledgement distinct;
	// identical file bytes alone never clear a rejection or prove completion.
	if bytes.Equal(source, restoration.prior.source) {
		proven, err := restoration.prior.VerifyActiveConfiguration(body)
		if err != nil {
			return RestorationProgress{}, err
		}
		if proven {
			return RestorationProgress{identity: restoration.identity, restored: true, active: true}, nil
		}
		ready, err := restoration.readyForPublication(body, restoration.paths[0][1])
		if err != nil {
			return RestorationProgress{}, err
		}
		return RestorationProgress{identity: restoration.identity, path: 0, index: 0, active: ready}, nil
	}
	for pathIndex, path := range restoration.paths {
		for index := len(path) - 1; index >= 0; index-- {
			if !bytes.Equal(source, path[index].source) {
				continue
			}
			complete := index == len(path)-1
			var active bool
			if complete {
				active, err = restoration.prior.VerifyActiveConfiguration(body)
			} else {
				active, err = restoration.readyForPublication(body, path[index+1])
			}
			if err != nil {
				return RestorationProgress{}, err
			}
			return RestorationProgress{identity: restoration.identity, path: pathIndex, index: index,
				active: active, restored: complete && active}, nil
		}
	}
	return RestorationProgress{}, errs.New(errs.KindStateConflict,
		"HTTP router recovery files are not reachable from this Task's captured configurations")
}

// A union publication preserves either captured traffic set. Prior Routes need
// the union active; removing failed backends needs prior Routes active. Metadata
// publication waits for the complete prior resources. No version-only shortcut.
func (restoration Restoration) readyForPublication(body []byte, next activationState) (bool, error) {
	switch {
	case next.target == ClustersTarget && bytes.Equal(next.content, restoration.transition.bridgeJSON):
		return restoration.verifyCapturedActive(body)
	case next.target == RoutesTarget:
		if bytes.Equal(next.content, restoration.recoveryRoutesJSON) {
			return restoration.verifyUnionActive(body)
		}
		return restoration.verifyRecoveryActive(body, restoration.prior.clusters)
	case next.target == ClustersTarget:
		return restoration.verifyRecoveryActive(body, restoration.transition.bridge)
	case next.target == BootstrapTarget || next.target == ManifestTarget:
		return restoration.prior.VerifyActiveConfiguration(body)
	default:
		return false, errs.New(errs.KindInternal, "HTTP router recovery publication target is invalid")
	}
}

func (restoration Restoration) verifyCapturedActive(body []byte) (bool, error) {
	matched, err := verifyActive(body, restoration.prior.bootstrap.StaticResources.Listeners[0],
		restoration.prior.clusters, restoration.prior.routes, true)
	if matched || err != nil {
		return matched, err
	}
	matched, err = verifyActive(body, restoration.failed.bootstrap.StaticResources.Listeners[0],
		restoration.failed.clusters, restoration.failed.routes, true)
	if matched || err != nil {
		return matched, err
	}
	return restoration.verifyUnionActive(body)
}

func (restoration Restoration) verifyUnionActive(body []byte) (bool, error) {
	for _, routes := range []*discoveryv3.DiscoveryResponse{restoration.failed.routes, restoration.prior.routes, restoration.recoveryRoutes} {
		matched, err := verifyActive(body, restoration.prior.bootstrap.StaticResources.Listeners[0],
			restoration.transition.bridge, routes, true)
		if matched || err != nil {
			return matched, err
		}
	}
	return false, nil
}

func (restoration Restoration) verifyRecoveryActive(
	body []byte,
	clusters *discoveryv3.DiscoveryResponse,
) (bool, error) {
	return verifyActive(
		body,
		restoration.prior.bootstrap.StaticResources.Listeners[0],
		clusters,
		restoration.recoveryRoutes,
		false,
	)
}

func (restoration Restoration) Publication(progress RestorationProgress) (Publication, error) {
	if progress.identity != restoration.identity || !progress.ReadyToPublish() || progress.path < 0 ||
		progress.path >= len(restoration.paths) || progress.index < 0 ||
		progress.index+1 >= len(restoration.paths[progress.path]) {
		return Publication{}, errs.New(
			errs.KindInternal,
			"HTTP router recovery publication has no captured predecessor",
		)
	}
	path := restoration.paths[progress.path]
	return preparePublication(path[progress.index], path[progress.index+1]), nil
}
