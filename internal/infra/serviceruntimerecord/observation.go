package serviceruntimerecord

import (
	"slices"
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/serviceobservation"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// WorkloadObservation freezes every acknowledged workload's physical ownership.
// The optional proxy is observed separately, never counted as a workload or
// selected as the network context for a consumer access check.
func WorkloadObservation(record Record) (*agentpb.ServiceObservationTarget, uint32, error) {
	if err := Validate(record); err != nil {
		return nil, 0, err
	}
	return NativeWorkloadObservation(record.EnvironmentID, record.Runtime.ServiceID,
		record.Runtime.CurrentArtifact, record.Runtime.RetainedPriorArtifact)
}

// NativeWorkloadObservation observes a captured predecessor without inventing a
// runtime record or reading a newer desired projection during Task preparation.
func NativeWorkloadObservation(
	environmentID, serviceID string,
	current, retained []byte,
) (*agentpb.ServiceObservationTarget, uint32, error) {
	serving, err := executionplan.OpenNativeServingRuntime(environmentID, serviceID, current, retained)
	if err != nil {
		return nil, 0, err
	}
	role, slot := "singleton", ""
	switch serving.Target {
	case "rolling":
		role = "replica"
	case "blue", "green":
		role, slot = "slot", serving.Target
	}
	target := &agentpb.ServiceObservationTarget{EnvironmentId: environmentID,
		ServiceId: serviceID, ReleaseId: serving.ReleaseID, RuntimeRole: role, Slot: slot}
	var replicas uint32
	for _, workload := range serving.Workloads {
		planID, generation, err := observationOwnership(workload)
		if err != nil {
			return nil, 0, err
		}
		target.Workloads = append(target.Workloads, &agentpb.ServiceWorkloadObservation{PlanId: planID,
			RenderGeneration: generation, ComposeName: workload.GetComposeName(), ReplicaOrdinal: workload.GetRollingReplicaOrdinal()})
		replicas += workload.GetExpectedReplicas()
	}
	if serving.Proxy != nil {
		target.ProxyPlanId, target.ProxyRenderGeneration, err = observationOwnership(serving.Proxy)
		if err != nil {
			return nil, 0, err
		}
		target.ProxyComposeName = serving.Proxy.GetComposeName()
		target.ProxyImplementation = serving.Proxy.GetProxyImplementation()
		target.ProxyConfigSha256 = slices.Clone(serving.Proxy.GetProxyConfigSha256())
	}
	if err := serviceobservation.ValidateTarget(target); err != nil {
		return nil, 0, errs.New(errs.KindInternal, "acknowledged workload observation identity is invalid")
	}
	return target, replicas, nil
}

func observationOwnership(service *agentpb.ComposeService) (string, uint64, error) {
	labels := make(map[string]string, len(service.GetExpectedLabels()))
	for _, pair := range service.GetExpectedLabels() {
		if pair == nil {
			return "", 0, invalidRuntimeIdentity()
		}
		if _, duplicate := labels[pair.GetKey()]; duplicate {
			return "", 0, invalidRuntimeIdentity()
		}
		labels[pair.GetKey()] = pair.GetValue()
	}
	generation, err := strconv.ParseUint(labels["com.groundplane.render-generation"], 10, 64)
	if err != nil || strconv.FormatUint(generation, 10) != labels["com.groundplane.render-generation"] {
		return "", 0, invalidRuntimeIdentity()
	}
	return labels["com.groundplane.plan-id"], generation, nil
}
