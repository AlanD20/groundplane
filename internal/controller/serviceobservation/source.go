// Package serviceobservation owns serving-source selection and freshness for
// read-only Service observations. It has no rendering or mutation dependency.
package serviceobservation

import (
	"context"

	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Services is the fixed-revision storage read seam, not a desired-state writer.
type Services interface {
	ListServices(context.Context, string, etcd.PageRequest) (etcd.Page[etcd.ServiceRecord], error)
}

// Releases exposes only the two immutable serving-source reads.
type Releases interface {
	ResolveServing(context.Context, string, string, int64) (etcd.ServingRelease, error)
	GetReleaseRenderInputAt(context.Context, string, int64) (etcd.Versioned[etcd.ReleaseRenderInput], error)
}

type source struct {
	target                                                              *agentpb.ServiceObservationTarget
	expected                                                            uint32
	runtimeRevision, projectionRevision, intentRevision, renderRevision int64
}

func capture(ctx context.Context, releases Releases, service etcd.Versioned[etcd.ServiceRecord]) (source, bool) {
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
	// Storage decodes each record. This cross-record binding prevents a valid
	// but unrelated render input or successful Release from becoming authority.
	if intent.EnvironmentID != environmentID || intent.ServiceID != serviceID ||
		projection.EnvironmentID != environmentID || projection.ServiceID != serviceID ||
		projection.ServingReleaseID != intent.ID || projection.ServingSlot != intent.Slot ||
		input.EnvironmentID != environmentID || input.Projection.EnvironmentID != environmentID ||
		input.ServiceID != serviceID || input.ReleaseID != intent.ID || input.ArtifactID != intent.RenderInputID ||
		input.CandidateWorkload != intent.CandidateWorkload || input.Strategy != intent.Strategy || input.Slot != intent.Slot {
		return source{}, false
	}
	// The published digest is over the canonical encoded render input. This
	// serializes a validated value for hashing; it does not convert model types.
	digest, err := domain.Digest(input)
	if err != nil || digest != intent.RenderInputDigest {
		return source{}, false
	}
	target, err := domain.TargetFor(intent.Strategy, intent.Slot)
	if err != nil || target != input.CandidateTarget || domain.ValidateWorkloadSeal(intent.CandidateWorkload) != nil {
		return source{}, false
	}
	name, role := input.ServiceName, "singleton"
	if len(input.ProxyPorts) != 0 {
		name, err = domain.WorkloadComposeName(input.ServiceName, target)
		if err != nil {
			return source{}, false
		}
	}
	if target != domain.WorkloadSingleton {
		role = "slot"
	}
	return source{
		target: &agentpb.ServiceObservationTarget{
			EnvironmentId: environmentID, ServiceId: serviceID, ReleaseId: intent.ID,
			PlanId: input.PlanID, RenderGeneration: input.Projection.RenderGeneration,
			ComposeName: name, RuntimeRole: role, Slot: string(intent.Slot),
		},
		expected:        intent.CandidateWorkload.ReplicaCount,
		runtimeRevision: etcd.ServiceRuntimeRevision(service), projectionRevision: serving.ProjectionRevision,
		intentRevision: serving.IntentRevision, renderRevision: render.Revision,
	}, true
}

func (captured source) unchanged(current source) bool {
	return captured.runtimeRevision == current.runtimeRevision &&
		captured.projectionRevision == current.projectionRevision &&
		captured.intentRevision == current.intentRevision && captured.renderRevision == current.renderRevision &&
		captured.expected == current.expected &&
		captured.target.EnvironmentId == current.target.EnvironmentId &&
		captured.target.ServiceId == current.target.ServiceId && captured.target.ReleaseId == current.target.ReleaseId &&
		captured.target.PlanId == current.target.PlanId && captured.target.RenderGeneration == current.target.RenderGeneration &&
		captured.target.ComposeName == current.target.ComposeName && captured.target.RuntimeRole == current.target.RuntimeRole &&
		captured.target.Slot == current.target.Slot
}
