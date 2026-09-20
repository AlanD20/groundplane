package runner

import (
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	runnerrecord "github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	"time"

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
	projectID := ""
	if record.Desired.OwnerKind == runnerrecord.RunnerOwnerProject {
		projectID = record.Desired.OwnerID
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
		online = observation.Online
	}
	return apiTypes.Runner{
		ID: record.Desired.ID, Slug: record.Desired.Slug,
		TenantID: record.Desired.TenantID, ProjectID: projectID,
		GitHubURL: record.Desired.GitHubURL, Name: name,
		Labels: append([]string(nil), record.Desired.Labels...), Lifecycle: lifecycle,
		CreateTaskID: record.CreateTaskID, RemoveTaskID: removeTaskID,
		Online: online, ObservedAt: observedAt,
		CreatedAt: record.CreatedAt.UTC().Format(time.RFC3339Nano),
	}, nil
}
