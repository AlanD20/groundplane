package runner

import (
	"time"

	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	runnerrecord "github.com/AlanD20/groundplane/internal/infra/etcd/runners"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

func PublicProjection(
	record runnerrecord.RunnerRecord,
	observation *runnerrecord.RunnerObservationRecord,
	tombstone *deletionrecord.DeletionTombstoneRecord,
) (apiTypes.Runner, error) {
	name, err := runnerrecord.RunnerName(record.Desired.ID)
	if err != nil {
		return apiTypes.Runner{}, err
	}
	lifecycle := apiTypes.RunnerLifecycle(record.ProvisioningState)
	var removeTaskID *string
	if tombstone != nil {
		lifecycle = apiTypes.RunnerLifecycleDeleting
		value := tombstone.TaskID
		removeTaskID = &value
	}
	var observedAt *string
	online := false
	if observation != nil {
		value := observation.ObservedAt.UTC().Format(time.RFC3339Nano)
		observedAt = &value
		age := time.Since(observation.ObservedAt)
		online = observation.Online && age >= 0 && age <= ObservationLifetime
	}
	if record.Desired.GitHubName != "" {
		name = record.Desired.GitHubName
	}
	result := apiTypes.Runner{
		ID: record.Desired.ID, Slug: record.Desired.Slug,
		TenantID: record.Desired.TenantID, ProjectID: record.Desired.ProjectID(),
		EnvironmentID: record.Desired.EnvironmentID(),
		GitHubURL:     record.Desired.GitHubURL, RunnerGroup: record.Desired.RunnerGroup, GitHubName: record.Desired.GitHubName, CPUMillis: record.Desired.CPUMillis, MemoryBytes: record.Desired.MemoryBytes, Name: name,
		Labels: append([]string(nil), record.Desired.Labels...), Lifecycle: lifecycle,
		CreateTaskID: record.CreateTaskID, RemoveTaskID: removeTaskID,
		Online: online, ObservedAt: observedAt,
		CreatedAt: record.CreatedAt.UTC().Format(time.RFC3339Nano),
	}
	if record.Software.Image != "" {
		software := record.Software
		result.Software = &software
	}
	return result, nil
}
