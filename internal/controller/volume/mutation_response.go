package volume

import (
	"encoding/json"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"net/http"
	"sort"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func stableIDFromTask(kind ids.Kind, taskID string) string {
	if ids.Validate(ids.KindTask, taskID) != nil || len(taskID) <= len("task_") {
		return ""
	}
	return string(kind) + "_" + strings.TrimPrefix(taskID, "task_")
}

func volumeMutationConsumerIDs(projection projectionrecord.EnvironmentComposeProjection, volumeID string) []string {
	seen := make(map[string]struct{})
	for _, mount := range projection.VolumeMounts {
		if mount.VolumeID == volumeID {
			seen[mount.ServiceID] = struct{}{}
		}
	}
	result := make([]string, 0, len(seen))
	for serviceID := range seen {
		result = append(result, serviceID)
	}
	sort.Strings(result)
	return result
}

func volumeMutationResponse(
	request volumeMutationRequest,
	environment hierarchyrecord.EnvironmentRecord,
	taskID string,
) (idempotencyrecord.IdempotencyResponse, error) {
	if request.action == volumeMutationActionRemove {
		value, err := json.Marshal(apiTypes.TaskAccepted{TaskID: taskID})
		if err != nil {
			return idempotencyrecord.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
		}
		return idempotencyrecord.IdempotencyResponse{
			Status: http.StatusAccepted, ContentKind: "application/json", Body: value,
		}, nil
	}
	status := http.StatusCreated
	state := "creating"
	if request.action == volumeMutationActionEdit {
		status = http.StatusOK
		state = "active"
	}
	originTaskID := ""
	if request.action == volumeMutationActionAdd {
		originTaskID = taskID
	}
	volume := volumeView(environment, etcd.VolumeRecord{
		ID: request.volumeID, EnvironmentID: request.environmentID, Slug: request.slug, Key: request.key,
	}, originTaskID, state)
	currentTaskID := taskID
	volume.CurrentTaskID = &currentTaskID
	if request.action == volumeMutationActionAdd {
		createTaskID := taskID
		volume.CreateTaskID = &createTaskID
	}
	body := apiTypes.VolumeMutationResponse{
		Volume: volume,
		TaskID: taskID,
	}
	value, err := json.Marshal(body)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	return idempotencyrecord.IdempotencyResponse{Status: status, ContentKind: "application/json", Body: value}, nil
}
