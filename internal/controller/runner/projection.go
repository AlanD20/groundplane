package runner

import (
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

func PublicProjection(
	record etcd.RunnerRecord,
	observation *etcd.RunnerObservationRecord,
	tombstone *etcd.DeletionTombstoneRecord,
) (apiTypes.Runner, error) {
	name, err := etcd.RunnerName(record.Desired.ID)
	if err != nil {
		return apiTypes.Runner{}, err
	}
	projectID := ""
	if record.Desired.OwnerKind == etcd.RunnerOwnerProject {
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
