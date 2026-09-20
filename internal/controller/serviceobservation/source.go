// Package serviceobservation owns serving-source selection and freshness for
// read-only Service observations. It has no rendering or mutation dependency.
package serviceobservation

import (
	"bytes"
	"context"
	"crypto/sha256"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Services is the fixed-revision storage read seam, not a desired-state writer.
type Services interface {
	ListServices(context.Context, string, etcdstore.PageRequest) (etcdstore.Page[etcd.ServiceRecord], error)
}

// Releases exposes only the immutable serving-source and runtime-receipt reads.
type Releases interface {
	ResolveServing(context.Context, string, string, int64) (etcd.ServingRelease, error)
	GetReleaseRenderInputAt(context.Context, string, int64) (etcdstore.Versioned[etcd.ReleaseRenderInput], error)
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

func capture(ctx context.Context, releases Releases, service etcdstore.Versioned[etcd.ServiceRecord]) (source, bool) {
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
	proxy, proxyRevision, ok := captureProxy(ctx, releases, environmentID, serviceID, revision, intent, input)
	if !ok {
		return source{}, false
	}
	return source{
		target: &agentpb.ServiceObservationTarget{
			EnvironmentId: environmentID, ServiceId: serviceID, ReleaseId: intent.ID,
			PlanId: input.PlanID, RenderGeneration: input.Projection.RenderGeneration,
			ComposeName: name, RuntimeRole: role, Slot: string(intent.Slot),
			ProxyComposeName: proxy.composeName, ProxyPlanId: proxy.planID,
			ProxyRenderGeneration: proxy.renderGeneration, ProxyConfigSha256: proxy.configSHA256,
		},
		expected:        intent.CandidateWorkload.ReplicaCount,
		runtimeRevision: etcd.ServiceRuntimeRevision(service), projectionRevision: serving.ProjectionRevision,
		intentRevision: serving.IntentRevision, renderRevision: render.Revision,
		acknowledgedRuntimeRevision: proxyRevision,
	}, true
}

type proxySource struct {
	composeName, planID string
	renderGeneration    uint64
	configSHA256        []byte
}

func captureProxy(
	ctx context.Context,
	releases Releases,
	environmentID, serviceID string,
	revision int64,
	intent domain.Intent,
	input etcd.ReleaseRenderInput,
) (proxySource, int64, bool) {
	if len(input.ProxyPorts) == 0 {
		return proxySource{}, 0, true
	}
	runtimes, err := releases.LoadAcknowledgedServiceRuntimesAtRevision(
		ctx,
		environmentID,
		[]string{serviceID},
		revision,
	)
	if err != nil || len(runtimes) != 1 || runtimes[0].Revision <= 0 ||
		runtimes[0].Revision > revision || runtimes[0].ReadRevision != revision {
		return proxySource{}, 0, false
	}
	record := runtimes[0].Record
	if serviceruntimerecord.Validate(record) != nil || record.EnvironmentID != environmentID ||
		record.Runtime.ServiceID != serviceID || record.Runtime.ReleaseID != intent.ID ||
		record.Runtime.Target != string(input.CandidateTarget) {
		return proxySource{}, 0, false
	}
	artifact := &agentpb.ComposeArtifact{}
	if (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(record.Runtime.CurrentArtifact, artifact) != nil {
		return proxySource{}, 0, false
	}
	var proxy *agentpb.ComposeService
	for _, service := range artifact.GetServices() {
		if service.GetRole() != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
			continue
		}
		if proxy != nil {
			return proxySource{}, 0, false
		}
		proxy = service
	}
	if proxy == nil || proxy.GetServiceId() != serviceID || proxy.GetComposeName() == "" ||
		len(proxy.GetProxyConfigJson()) > executionplan.MaximumArtifactYAMLBytes {
		return proxySource{}, 0, false
	}
	canonical, err := jcs.RequireCanonical(proxy.GetProxyConfigJson())
	if err != nil || !bytes.Equal(canonical, proxy.GetProxyConfigJson()) {
		return proxySource{}, 0, false
	}
	digest := sha256.Sum256(canonical)
	if !bytes.Equal(digest[:], proxy.GetProxyConfigSha256()) ||
		!bytes.Equal(digest[:], record.Runtime.ProxyConfigSHA256) {
		return proxySource{}, 0, false
	}
	planID, generation, ok := proxyOwnership(proxy, environmentID, serviceID)
	if !ok {
		return proxySource{}, 0, false
	}
	return proxySource{
		composeName: proxy.GetComposeName(), planID: planID, renderGeneration: generation,
		configSHA256: append([]byte(nil), digest[:]...),
	}, runtimes[0].Revision, true
}

func proxyOwnership(proxy *agentpb.ComposeService, environmentID, serviceID string) (string, uint64, bool) {
	values := make(map[string]string, len(proxy.GetExpectedLabels()))
	for _, pair := range proxy.GetExpectedLabels() {
		if pair == nil {
			return "", 0, false
		}
		values[pair.GetKey()] = pair.GetValue()
	}
	generation, err := strconv.ParseUint(values["com.groundplane.render-generation"], 10, 64)
	planID := values["com.groundplane.plan-id"]
	if err != nil || generation == 0 || strconv.FormatUint(generation, 10) !=
		values["com.groundplane.render-generation"] || ids.Validate(ids.KindPlan, planID) != nil ||
		values["com.groundplane.managed"] != "true" || values["com.groundplane.kind"] != "service" ||
		values["com.groundplane.environment-id"] != environmentID ||
		values["com.groundplane.service-id"] != serviceID || values["com.groundplane.runtime-role"] != "proxy" ||
		values["com.groundplane.release-id"] != "" || values["com.groundplane.slot"] != "" ||
		values["com.groundplane.component-id"] != "" {
		return "", 0, false
	}
	return planID, generation, true
}

func (captured source) unchanged(current source) bool {
	return captured.runtimeRevision == current.runtimeRevision &&
		captured.projectionRevision == current.projectionRevision &&
		captured.intentRevision == current.intentRevision && captured.renderRevision == current.renderRevision &&
		captured.acknowledgedRuntimeRevision == current.acknowledgedRuntimeRevision &&
		captured.expected == current.expected &&
		captured.target.EnvironmentId == current.target.EnvironmentId &&
		captured.target.ServiceId == current.target.ServiceId && captured.target.ReleaseId == current.target.ReleaseId &&
		captured.target.PlanId == current.target.PlanId && captured.target.RenderGeneration == current.target.RenderGeneration &&
		captured.target.ComposeName == current.target.ComposeName && captured.target.RuntimeRole == current.target.RuntimeRole &&
		captured.target.Slot == current.target.Slot &&
		captured.target.ProxyComposeName == current.target.ProxyComposeName &&
		captured.target.ProxyPlanId == current.target.ProxyPlanId &&
		captured.target.ProxyRenderGeneration == current.target.ProxyRenderGeneration &&
		bytes.Equal(captured.target.ProxyConfigSha256, current.target.ProxyConfigSha256)
}
