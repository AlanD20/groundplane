package common

import (
	"slices"
	"strconv"
	"time"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

const (
	serviceObservationFreshness = 15 * time.Second
	maximumObservedContainers   = uint64(4096)
)

// ServiceObservationPresentation is the safe, human-readable projection of a
// current serving-workload observation. Observation is also safe to return in
// machine-readable CLI output.
type ServiceObservationPresentation struct {
	Observation      *apiTypes.ServiceObservation
	State            string
	ObservedAt       string
	ExpiresAt        string
	ServingReleaseID string
	ExpectedReplicas string
	Running          string
	Healthy          string
	Starting         string
	Unhealthy        string
	Transitional     string
	Stopped          string
	Failed           string
}

// PresentServiceObservation refuses to display stale or malformed evidence as
// live state. The caller supplies its read time so every row in a page shares
// one expiry boundary.
func PresentServiceObservation(
	observation *apiTypes.ServiceObservation,
	now time.Time,
) ServiceObservationPresentation {
	if !validServiceObservation(observation, now) {
		unavailable := &apiTypes.ServiceObservation{State: apiTypes.ServiceObservationUnavailable}
		return ServiceObservationPresentation{Observation: unavailable, State: string(unavailable.State)}
	}
	if observation.State == apiTypes.ServiceObservationUnavailable {
		unavailable := &apiTypes.ServiceObservation{State: apiTypes.ServiceObservationUnavailable}
		return ServiceObservationPresentation{Observation: unavailable, State: string(unavailable.State)}
	}

	counts := observation.Replicas
	return ServiceObservationPresentation{
		Observation:      observation,
		State:            string(observation.State),
		ObservedAt:       observation.ObservedAt.UTC().Format(time.RFC3339),
		ExpiresAt:        observation.ExpiresAt.UTC().Format(time.RFC3339),
		ServingReleaseID: *observation.ServingReleaseID,
		ExpectedReplicas: strconv.FormatUint(uint64(*observation.ExpectedReplicas), 10),
		Running:          strconv.FormatUint(uint64(counts.Running), 10),
		Healthy:          strconv.FormatUint(uint64(counts.Healthy), 10),
		Starting:         strconv.FormatUint(uint64(counts.Starting), 10),
		Unhealthy:        strconv.FormatUint(uint64(counts.Unhealthy), 10),
		Transitional:     strconv.FormatUint(uint64(counts.Transitional), 10),
		Stopped:          strconv.FormatUint(uint64(counts.Stopped), 10),
		Failed:           strconv.FormatUint(uint64(counts.Failed), 10),
	}
}

// ServiceObservationTable renders runtime intent and live observation as
// separate columns. It also returns Services with observations normalized for
// JSON/YAML rendering by the same list command.
func ServiceObservationTable(
	services []apiTypes.Service,
	now time.Time,
) ([]apiTypes.Service, []string, [][]string) {
	headers := []string{
		"ID", "NAME", "RUNTIME_INTENT", "OBSERVATION_STATE", "OBSERVED_AT", "EXPIRES_AT",
		"SERVING_RELEASE_ID", "EXPECTED_REPLICAS", "RUNNING", "HEALTHY", "STARTING",
		"UNHEALTHY", "TRANSITIONAL", "STOPPED", "FAILED",
	}
	presented := slices.Clone(services)
	rows := make([][]string, len(presented))
	for index := range presented {
		observation := PresentServiceObservation(presented[index].Observation, now)
		presented[index].Observation = observation.Observation
		rows[index] = append([]string{
			presented[index].ID,
			presented[index].Name,
			string(presented[index].RuntimeIntent),
		}, observation.values()...)
	}
	return presented, headers, rows
}

// AddServiceObservationFields adds the nested observation projection to the
// existing Service field map used by the TABLE detail view.
func AddServiceObservationFields(fields map[string]any, observation ServiceObservationPresentation) {
	fields["observation_state"] = observation.State
	if observation.State == string(apiTypes.ServiceObservationUnavailable) {
		return
	}
	fields["observation_observed_at"] = observation.ObservedAt
	fields["observation_expires_at"] = observation.ExpiresAt
	fields["observation_serving_release_id"] = observation.ServingReleaseID
	fields["observation_expected_replicas"] = observation.ExpectedReplicas
	fields["observation_running"] = observation.Running
	fields["observation_healthy"] = observation.Healthy
	fields["observation_starting"] = observation.Starting
	fields["observation_unhealthy"] = observation.Unhealthy
	fields["observation_transitional"] = observation.Transitional
	fields["observation_stopped"] = observation.Stopped
	fields["observation_failed"] = observation.Failed
}

func (presentation ServiceObservationPresentation) values() []string {
	return []string{
		presentation.State,
		presentation.ObservedAt,
		presentation.ExpiresAt,
		presentation.ServingReleaseID,
		presentation.ExpectedReplicas,
		presentation.Running,
		presentation.Healthy,
		presentation.Starting,
		presentation.Unhealthy,
		presentation.Transitional,
		presentation.Stopped,
		presentation.Failed,
	}
}

func validServiceObservation(observation *apiTypes.ServiceObservation, now time.Time) bool {
	if observation == nil {
		return false
	}
	if observation.State == apiTypes.ServiceObservationUnavailable {
		return observation.ObservedAt == nil && observation.ExpiresAt == nil && observation.ServingReleaseID == nil &&
			observation.ExpectedReplicas == nil && observation.Replicas == nil
	}
	if observation.ObservedAt == nil || observation.ExpiresAt == nil || observation.ServingReleaseID == nil ||
		*observation.ServingReleaseID == "" || observation.ExpectedReplicas == nil ||
		*observation.ExpectedReplicas == 0 || observation.Replicas == nil {
		return false
	}
	if !observation.ExpiresAt.Equal(observation.ObservedAt.Add(serviceObservationFreshness)) ||
		now.Before(*observation.ObservedAt) || !now.Before(*observation.ExpiresAt) {
		return false
	}
	workload := summarizeServiceObservation(observation.Replicas, *observation.ExpectedReplicas)
	return observation.State == workload || observation.State == apiTypes.ServiceObservationDegraded &&
		(workload == apiTypes.ServiceObservationHealthy || workload == apiTypes.ServiceObservationRunning)
}

func summarizeServiceObservation(
	counts *apiTypes.ServiceReplicaCounts,
	expected uint32,
) apiTypes.ServiceObservationState {
	total := uint64(counts.Running) + uint64(counts.Healthy) + uint64(counts.Starting) +
		uint64(counts.Unhealthy) + uint64(counts.Transitional) + uint64(counts.Stopped) +
		uint64(counts.Failed)
	switch {
	case total > maximumObservedContainers:
		return apiTypes.ServiceObservationUnavailable
	case total == 0:
		return apiTypes.ServiceObservationAbsent
	case total == uint64(counts.Failed):
		return apiTypes.ServiceObservationFailed
	case total == uint64(counts.Stopped):
		return apiTypes.ServiceObservationStopped
	case total == uint64(counts.Transitional)+uint64(counts.Starting):
		return apiTypes.ServiceObservationStarting
	case total == uint64(expected) && total == uint64(counts.Healthy):
		return apiTypes.ServiceObservationHealthy
	case total == uint64(expected) && total == uint64(counts.Healthy)+uint64(counts.Running):
		return apiTypes.ServiceObservationRunning
	default:
		return apiTypes.ServiceObservationDegraded
	}
}
