package serviceobservation

import (
	"strconv"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

// SelectsWorkload excludes other Releases, proxies, Components and one-off jobs.
func SelectsWorkload(labels map[string]string, target *agentpb.ServiceObservationTarget) bool {
	if target == nil {
		return false
	}
	role := target.RuntimeRole
	if role == "backing" {
		role = ""
	}
	return labels["com.groundplane.managed"] == "true" && labels["com.groundplane.kind"] == "service" &&
		labels["com.groundplane.component-id"] == "" &&
		labels["com.groundplane.environment-id"] == target.EnvironmentId &&
		labels["com.groundplane.service-id"] == target.ServiceId &&
		labels["com.groundplane.release-id"] == target.ReleaseId &&
		labels["com.groundplane.runtime-role"] == role &&
		labels["com.groundplane.slot"] == target.Slot && labels["com.docker.compose.oneoff"] != "True"
}

// WorkloadReplica binds a candidate to the exact acknowledged runtime artifact.
func WorkloadReplica(labels map[string]string, target *agentpb.ServiceObservationTarget) (uint64, bool) {
	if !SelectsWorkload(labels, target) || labels["com.groundplane.plan-id"] != target.PlanId ||
		labels["com.groundplane.render-generation"] != strconv.FormatUint(target.RenderGeneration, 10) ||
		labels["com.docker.compose.service"] != target.ComposeName || labels["com.docker.compose.oneoff"] != "False" {
		return 0, false
	}
	value := labels["com.docker.compose.container-number"]
	ordinal, err := strconv.ParseUint(value, 10, 32)
	return ordinal, err == nil && ordinal != 0 && strconv.FormatUint(ordinal, 10) == value
}
