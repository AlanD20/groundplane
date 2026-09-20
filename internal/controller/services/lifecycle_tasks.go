package services

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func serviceLifecycleContract(taskType etcd.TaskType) (string, core.ServiceRuntimeIntent, error) {
	switch taskType {
	case etcd.TaskStart:
		return serviceStartRoute, core.ServiceRuntimeIntentRunning, nil
	case etcd.TaskStop:
		return serviceStopRoute, core.ServiceRuntimeIntentStopped, nil
	case etcd.TaskDestroy:
		return serviceDestroyRoute, core.ServiceRuntimeIntentAbsent, nil
	default:
		return "", "", errs.New(errs.KindValidationFailed, "Service lifecycle action is invalid")
	}
}

func serviceInComposeProjection(projection etcd.EnvironmentComposeProjection, serviceID string) bool {
	for _, service := range projection.DesiredServices {
		if service.Desired.ID == serviceID {
			return true
		}
	}
	for _, component := range projection.Components {
		for _, generatedServiceID := range component.Runtime.GeneratedServices {
			if generatedServiceID == serviceID {
				return true
			}
		}
	}
	return false
}

func prepareControllerServiceLifecycleTask(
	task etcd.TaskRecord,
	environmentID string,
	serviceRevision int64,
) (etcd.TaskRecord, error) {
	if serviceRevision <= 0 || ids.Validate(ids.KindEnvironment, environmentID) != nil {
		return etcd.TaskRecord{}, errs.New(errs.KindValidationFailed, "Controller Service lifecycle input is invalid")
	}
	task.Executor = etcd.TaskExecutorController
	task.RenderGeneration = 1
	task.Params = map[string]string{
		etcd.TaskResourceKindParam:       etcd.TaskResourceService,
		etcd.TaskServiceEnvironmentParam: environmentID,
	}
	task.Steps = []etcd.TaskStepRecord{{Kind: etcd.TaskStepOperation, ID: ids.New(ids.KindStep)}}
	task.TimeoutSeconds = serviceLifecycleControlTimeoutSeconds
	value, err := json.Marshal(struct {
		Version         int           `json:"version"`
		PlanID          string        `json:"plan_id"`
		Type            etcd.TaskType `json:"type"`
		ServiceID       string        `json:"service_id"`
		EnvironmentID   string        `json:"environment_id"`
		ServiceRevision int64         `json:"service_revision"`
	}{
		Version: 1, PlanID: task.PlanID, Type: task.Type, ServiceID: task.Target,
		EnvironmentID: environmentID, ServiceRevision: serviceRevision,
	})
	if err != nil {
		return etcd.TaskRecord{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(value)
	digest := sha256.Sum256(value)
	task.PlanHash = hex.EncodeToString(digest[:])
	return task, nil
}
