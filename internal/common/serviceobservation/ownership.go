package serviceobservation

import (
	"strconv"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

// SelectsWorkload excludes other Releases, proxies, unrelated Components and one-off jobs.
func SelectsWorkload(labels map[string]string, target *agentpb.ServiceObservationTarget) bool {
	if target == nil {
		return false
	}
	role := target.RuntimeRole
	if role == "backing" || role == "component" {
		role = ""
	}
	if target.RuntimeRole == "component" && labels["com.docker.compose.project"] != target.ComposeProject {
		return false
	}
	return labels["com.groundplane.managed"] == "true" && labels["com.groundplane.kind"] == "service" &&
		labels["com.groundplane.component-id"] == target.ComponentId &&
		labels["com.groundplane.environment-id"] == target.EnvironmentId &&
		labels["com.groundplane.service-id"] == target.ServiceId &&
		labels["com.groundplane.release-id"] == target.ReleaseId &&
		labels["com.groundplane.runtime-role"] == role &&
		labels["com.groundplane.slot"] == target.Slot && labels["com.docker.compose.oneoff"] != "True"
}

// WorkloadReplica binds a candidate to the exact acknowledged runtime artifact.
func WorkloadReplica(labels map[string]string, target *agentpb.ServiceObservationTarget) (uint64, bool) {
	if !SelectsWorkload(labels, target) || labels["com.docker.compose.oneoff"] != "False" {
		return 0, false
	}
	for _, workload := range target.Workloads {
		if labels["com.groundplane.plan-id"] != workload.PlanId ||
			labels["com.groundplane.render-generation"] != strconv.FormatUint(workload.RenderGeneration, 10) ||
			labels["com.docker.compose.service"] != workload.ComposeName {
			continue
		}
		value := labels["com.docker.compose.container-number"]
		if target.RuntimeRole == "replica" {
			return uint64(workload.ReplicaOrdinal), value == "1" && workload.ReplicaOrdinal != 0 &&
				labels["com.groundplane.replica-ordinal"] == strconv.FormatUint(uint64(workload.ReplicaOrdinal), 10)
		}
		ordinal, err := strconv.ParseUint(value, 10, 32)
		return ordinal, err == nil && ordinal != 0 && strconv.FormatUint(ordinal, 10) == value &&
			labels["com.groundplane.replica-ordinal"] == ""
	}
	return 0, false
}
