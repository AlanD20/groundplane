package publicingressauthority

import (
	"context"
	"encoding/hex"
	"net/netip"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/serviceproxy"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/core"
	edge "github.com/AlanD20/groundplane/internal/core/publicingress"
	"github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/core/serviceports"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
)

func (authority *Authority) currentExecutor(
	ctx context.Context, binding hostbindings.Binding, revision int64,
) (workexecution.Identity, keyvalue.Condition, error) {
	key := workqueue.ExecutorKey(workexecution.Identity{Executor: workexecution.Agent, HostID: binding.HostID})
	read, err := authority.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return workexecution.Identity{}, keyvalue.Condition{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return workexecution.Identity{}, keyvalue.Condition{}, incomplete(
			"public ingress Agent execution evidence is incomplete",
		)
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	if value != nil && value.Key == key && value.ModRevision > 0 {
		executor, err := workqueue.DecodeExecutor(value.Value)
		if err == nil && executor.Ready && executor.Identity.HostID == binding.HostID &&
			executor.Identity.AgentID == binding.AgentID && executor.Identity.Executor == workexecution.Agent {
			return executor.Identity, keyvalue.Condition{Key: key, ModRevision: value.ModRevision}, nil
		}
	}
	return workexecution.Identity{}, keyvalue.Condition{}, conflict(
		"public ingress requires a ready Agent on the selected Host",
	)
}

// CaptureProxyAt shares native serving proof with callers that already resolve
// Host placement and fence executor authority at the supplied revision.
func (authority *Authority) CaptureProxyAt(
	ctx context.Context, executor workexecution.Identity, environmentID, serviceID string, port uint16, revision int64,
) (NetworkRoute, []keyvalue.Condition, error) {
	_, route, guards, err := authority.proxy(ctx, executor, environmentID, serviceID, "", port, revision)
	return route, guards, err
}

func (authority *Authority) proxy(
	ctx context.Context, executor workexecution.Identity, environmentID, serviceID, routeID string,
	port uint16, revision int64,
) (edge.Endpoint, NetworkRoute, []keyvalue.Condition, error) {
	var endpoint edge.Endpoint
	var evidence NetworkRoute
	if authority == nil || authority.releases == nil || authority.store == nil || executor.Validate() != nil ||
		executor.Executor != workexecution.Agent || ids.Validate(ids.KindEnvironment, environmentID) != nil ||
		ids.Validate(ids.KindService, serviceID) != nil || port == 0 || revision <= 0 {
		return endpoint, evidence, nil, conflict(
			"proxy capture requires a Service, port, and current Host Agent authority",
		)
	}
	serving, err := authority.releases.ResolveServing(ctx, environmentID, serviceID, revision)
	if err != nil {
		return endpoint, evidence, nil, err
	}
	intent, projection := serving.Intent, serving.Projection
	if intent.EnvironmentID != environmentID || intent.ServiceID != serviceID ||
		intent.Workspace.Kind != release.WorkspaceTenant ||
		projection.ServingReleaseID != intent.ID || projection.ServingSlot != intent.Slot ||
		serving.Revision != revision || serving.IntentRevision <= 0 || serving.ProjectionRevision <= 0 {
		return endpoint, evidence, nil, conflict(
			"the public Route does not target an acknowledged serving application Release",
		)
	}
	running, err := authority.runningServiceCondition(ctx, environmentID, serviceID, revision)
	if err != nil {
		return endpoint, evidence, nil, err
	}
	render, err := authority.releases.GetReleaseRenderInputAt(ctx, intent.ID, revision)
	if err != nil {
		return endpoint, evidence, nil, err
	}
	source := render.Record
	digest, err := release.Digest(source)
	if err != nil || digest != intent.RenderInputDigest || source.ArtifactID != intent.RenderInputID ||
		render.ReadRevision != revision || render.Revision <= 0 || source.EnvironmentID != environmentID ||
		source.ServiceID != serviceID || source.ReleaseID != intent.ID || source.CandidateWorkload != intent.CandidateWorkload {
		return endpoint, evidence, nil, incomplete("the serving Release has inconsistent stable proxy inputs")
	}
	if !slices.ContainsFunc(source.ProxyPorts, func(listener release.ProxyPort) bool {
		return listener.Port == port && listener.Protocol == serviceports.TCP
	}) {
		return endpoint, evidence, nil, conflict(
			"the public Route's target port is not a captured TCP Service proxy listener",
		)
	}
	runtimes, err := authority.releases.LoadAcknowledgedServiceRuntimesAtRevision(
		ctx,
		environmentID,
		[]string{serviceID},
		revision,
	)
	if err != nil {
		return endpoint, evidence, nil, err
	}
	if len(runtimes) != 1 || runtimes[0].ReadRevision != revision || runtimes[0].Revision <= 0 {
		return endpoint, evidence, nil, incomplete("the public Route's Service runtime evidence is incomplete")
	}
	runtime := runtimes[0].Record
	observation, expected, err := serviceruntimerecord.WorkloadObservation(runtime)
	if err != nil {
		return endpoint, evidence, nil, err
	}
	if expected != intent.CandidateWorkload.ReplicaCount || observation.GetProxyComposeName() == "" {
		return endpoint, evidence, nil, conflict("the public Route requires an acknowledged stable Service proxy")
	}
	if runtime.Source.Executor.AgentID != executor.AgentID || runtime.Source.Executor.HostID != executor.HostID ||
		runtime.Source.Executor.Generation > executor.Generation || runtime.EnvironmentID != environmentID ||
		runtime.Runtime.ServiceID != serviceID || runtime.Runtime.ReleaseID != intent.ID ||
		runtime.Runtime.Target != string(source.CandidateTarget) {
		return endpoint, evidence, nil, conflict(
			"the public Route's Service runtime was not acknowledged on the selected Host",
		)
	}
	native, err := executionplan.OpenNativeServingRuntime(environmentID, serviceID, runtime.Runtime.CurrentArtifact,
		runtime.Runtime.RetainedPriorArtifact)
	if err != nil {
		return endpoint, evidence, nil, err
	}
	configuration, err := executionplan.OpenProxyConfiguration(native.Proxy)
	if err != nil {
		return endpoint, evidence, nil, err
	}
	if configuration.ReleaseID != intent.ID || configuration.Generation != runtime.Runtime.ProxyGeneration ||
		!slices.ContainsFunc(configuration.Ports, func(listener serviceproxy.Port) bool {
			return listener.Number == port && listener.Protocol == serviceproxy.TCP
		}) {
		return endpoint, evidence, nil, conflict(
			"the acknowledged stable proxy does not serve the selected Release and TCP port",
		)
	}
	addresses := make(map[string]netip.AddrPort)
	for zone, raw := range source.ProxyAddresses {
		address, err := netip.ParseAddr(raw)
		if err == nil && address.IsPrivate() && address.Is4() {
			addresses[zone] = netip.AddrPortFrom(address, port)
		}
	}
	if len(addresses) == 0 {
		return endpoint, evidence, nil, conflict("the acknowledged Service proxy has no reserved private Zone address")
	}
	return edge.Endpoint{Kind: edge.EndpointServiceProxy, HostID: executor.HostID, EnvironmentID: environmentID,
			ServiceID: serviceID, ReleaseID: intent.ID, RouteID: routeID, AcknowledgedRevision: runtimes[0].Revision},
		NetworkRoute{
			RouteID:          routeID,
			EnvironmentID:    environmentID,
			ServiceID:        serviceID,
			ReleaseID:        intent.ID,
			ProxyComposeName: native.Proxy.GetComposeName(),
			ProxyDigest:      hex.EncodeToString(runtime.Runtime.ProxyConfigSHA256),
			AgentGeneration:  executor.Generation,
			Addresses:        addresses,
			Target:           observation,
		},
		[]keyvalue.Condition{
			running,
			{Key: releases.ReleaseProjectionKey(serviceID), ModRevision: serving.ProjectionRevision},
			{Key: releases.ReleaseIntentStagingKey("", intent.ID), ModRevision: serving.IntentRevision},
			{Key: releases.ReleaseRenderInputStagingKey("", intent.ID), ModRevision: render.Revision},
			{Key: serviceruntimerecord.Key(serviceID), ModRevision: runtimes[0].Revision},
			{Key: deletions.TombstoneKey("service", serviceID)},
		}, nil
}

func (authority *Authority) runningServiceCondition(
	ctx context.Context,
	environmentID, serviceID string,
	revision int64,
) (keyvalue.Condition, error) {
	key := services.ServiceRuntimeKey(serviceID)
	read, err := authority.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return keyvalue.Condition{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return keyvalue.Condition{}, incomplete("public ingress Service runtime intent is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	if value != nil && value.Key == key && value.ModRevision > 0 {
		runtime, err := services.DecodeServiceRuntimeRecord(value.Value)
		if err == nil && runtime.ServiceID == serviceID && runtime.EnvironmentID == environmentID &&
			runtime.BackingNetworkID == "" && runtime.Runtime.RuntimeIntent == core.ServiceRuntimeIntentRunning {
			return keyvalue.Condition{Key: key, ModRevision: value.ModRevision}, nil
		}
	}
	return keyvalue.Condition{}, conflict("public ingress requires a running application Service")
}
