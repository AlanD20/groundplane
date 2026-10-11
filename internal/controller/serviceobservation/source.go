// Package serviceobservation owns serving-source selection and freshness for
// read-only Service observations. It has no rendering or mutation dependency.
package serviceobservation

import (
	"context"

	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backingpostgresruntime"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasequeries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Services is the fixed-revision storage read seam, not a desired-state writer.
type Services interface {
	ListServices(context.Context, string, etcdstore.PageRequest) (etcdstore.Page[servicerecord.ServiceRecord], error)
}

// Releases exposes only the immutable serving-source and runtime-receipt reads.
type Releases interface {
	GetPostgresRuntimeAt(
		context.Context,
		string,
		string,
		int64,
	) (etcdstore.Versioned[backingpostgresruntime.Record], bool, error)
	GetAppliedProjectionAt(
		context.Context,
		string,
		int64,
	) (etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection], bool, error)
	ResolveServing(context.Context, string, string, int64) (releasequeries.ServingRelease, error)
	GetReleaseRenderInputAt(
		context.Context,
		string,
		int64,
	) (etcdstore.Versioned[releaserender.ReleaseRenderInput], error)
	LoadAcknowledgedServiceRuntimesAtRevision(
		context.Context,
		string,
		[]string,
		int64,
	) ([]etcdstore.Versioned[serviceruntimerecord.Record], error)
}

type source struct {
	target                                                              *agentpb.ServiceObservationTarget
	expected                                                            uint32
	runtimeRevision, projectionRevision, intentRevision, renderRevision int64
	acknowledgedRuntimeRevision                                         int64
}

func capture(
	ctx context.Context,
	releases Releases,
	service etcdstore.Versioned[servicerecord.ServiceRecord],
) (source, bool) {
	if service.Record.BackingNetworkID != "" {
		return captureBacking(ctx, releases, service)
	}
	environmentID, serviceID, revision := service.Record.EnvironmentID, service.Record.Desired.ID, service.ReadRevision
	serving, err := releases.ResolveServing(ctx, environmentID, serviceID, revision)
	if err != nil || serving.Revision != revision || serving.ProjectionRevision <= 0 || serving.IntentRevision <= 0 {
		return source{}, false
	}
	render, err := releases.GetReleaseRenderInputAt(ctx, serving.Intent.ID, revision)
	if err != nil || render.ReadRevision != revision || render.Revision <= 0 {
		return source{}, false
	}
	intent, projection, input := serving.Intent, serving.Projection, render.Record
	// Valid individual records are not enough: bind them at the same revision.
	if intent.EnvironmentID != environmentID || intent.ServiceID != serviceID ||
		projection.EnvironmentID != environmentID || projection.ServiceID != serviceID ||
		projection.ServingReleaseID != intent.ID || projection.ServingSlot != intent.Slot ||
		input.EnvironmentID != environmentID || input.Projection.EnvironmentID != environmentID ||
		input.ServiceID != serviceID || input.ReleaseID != intent.ID || input.ArtifactID != intent.RenderInputID ||
		input.CandidateWorkload != intent.CandidateWorkload || input.Strategy != intent.Strategy || input.Slot != intent.Slot {
		return source{}, false
	}
	digest, err := domain.Digest(input)
	if err != nil || digest != intent.RenderInputDigest {
		return source{}, false
	}
	target, err := domain.TargetFor(intent.Strategy, intent.Slot)
	if err != nil || target != input.CandidateTarget || domain.ValidateWorkloadSeal(intent.CandidateWorkload) != nil {
		return source{}, false
	}
	// Attach/Entry operations can change physical ownership without creating a
	// Release. Observe the acknowledged runtime, not reconstructed deployment names.
	runtimes, err := releases.LoadAcknowledgedServiceRuntimesAtRevision(
		ctx,
		environmentID,
		[]string{serviceID},
		revision,
	)
	if err != nil || len(runtimes) != 1 || runtimes[0].Revision <= 0 || runtimes[0].Revision > revision ||
		runtimes[0].ReadRevision != revision {
		return source{}, false
	}
	runtime := runtimes[0].Record
	if runtime.EnvironmentID != environmentID || runtime.Runtime.ServiceID != serviceID ||
		runtime.Runtime.ReleaseID != intent.ID || runtime.Runtime.Target != string(target) {
		return source{}, false
	}
	observation, expected, err := serviceruntimerecord.WorkloadObservation(runtime)
	if err != nil || expected != intent.CandidateWorkload.ReplicaCount ||
		(observation.ProxyComposeName != "") != (len(input.ProxyPorts) != 0) {
		return source{}, false
	}
	return source{target: observation, expected: expected,
		runtimeRevision: servicerecord.ServiceRuntimeRevision(service), projectionRevision: serving.ProjectionRevision,
		intentRevision: serving.IntentRevision, renderRevision: render.Revision,
		acknowledgedRuntimeRevision: runtimes[0].Revision}, true
}

func (captured source) unchanged(current source) bool {
	return captured.runtimeRevision == current.runtimeRevision &&
		captured.projectionRevision == current.projectionRevision &&
		captured.intentRevision == current.intentRevision && captured.renderRevision == current.renderRevision &&
		captured.acknowledgedRuntimeRevision == current.acknowledgedRuntimeRevision &&
		captured.expected == current.expected && proto.Equal(captured.target, current.target)
}
