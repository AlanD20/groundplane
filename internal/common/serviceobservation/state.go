package serviceobservation

import "github.com/AlanD20/groundplane/proto/agentpb"

type State string

const (
	Unavailable State = "unavailable"
	Absent      State = "absent"
	Failed      State = "failed"
	Stopped     State = "stopped"
	Starting    State = "starting"
	Healthy     State = "healthy"
	Running     State = "running"
	Degraded    State = "degraded"
)

// Total widens before adding: malformed wire counts must not wrap to zero.
func Total(counts *agentpb.ServiceReplicaCounts) uint64 {
	return uint64(counts.GetRunning()) + uint64(counts.GetHealthy()) + uint64(counts.GetStarting()) +
		uint64(counts.GetUnhealthy()) + uint64(counts.GetTransitional()) + uint64(counts.GetStopped()) +
		uint64(counts.GetFailed())
}

// Summarize takes sealed expectations, never desired intent or desired replicas.
// Callers still enforce snapshot freshness and the serving-authority fence.
func Summarize(counts *agentpb.ServiceReplicaCounts, expected uint32) State {
	total := Total(counts)
	if counts == nil || len(counts.ProtoReflect().GetUnknown()) != 0 || expected == 0 || total > MaximumContainers {
		return Unavailable
	}
	switch {
	case total == 0:
		return Absent
	case total == uint64(counts.Failed):
		return Failed
	case total == uint64(counts.Stopped):
		return Stopped
	case total == uint64(counts.Transitional)+uint64(counts.Starting):
		return Starting
	case total == uint64(expected) && total == uint64(counts.Healthy):
		return Healthy
	case total == uint64(expected) && total == uint64(counts.Healthy)+uint64(counts.Running):
		return Running
	default:
		return Degraded
	}
}

// SummarizeProxy preserves the workload-only counts while preventing a managed
// stable proxy failure from becoming healthy or running Service evidence.
func SummarizeProxy(
	counts *agentpb.ServiceReplicaCounts,
	expected uint32,
	proxy agentpb.ServiceProxyObservationState,
) State {
	state := Summarize(counts, expected)
	switch proxy {
	case agentpb.ServiceProxyObservationState_SERVICE_PROXY_OBSERVATION_STATE_MATCHING:
		return state
	case agentpb.ServiceProxyObservationState_SERVICE_PROXY_OBSERVATION_STATE_MISSING,
		agentpb.ServiceProxyObservationState_SERVICE_PROXY_OBSERVATION_STATE_STOPPED,
		agentpb.ServiceProxyObservationState_SERVICE_PROXY_OBSERVATION_STATE_CONFIG_MISMATCH:
		if state == Healthy || state == Running {
			return Degraded
		}
		return state
	default:
		return Unavailable
	}
}
