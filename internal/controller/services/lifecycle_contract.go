package services

import (
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func serviceLifecycleContract(action workexecution.Action) (string, core.ServiceRuntimeIntent, error) {
	switch action {
	case workexecution.ActionStart:
		return serviceStartRoute, core.ServiceRuntimeIntentRunning, nil
	case workexecution.ActionStop:
		return serviceStopRoute, core.ServiceRuntimeIntentStopped, nil
	case workexecution.ActionDestroy:
		return serviceDestroyRoute, core.ServiceRuntimeIntentAbsent, nil
	default:
		return "", "", errs.New(errs.KindValidationFailed, "Service lifecycle action is invalid")
	}
}

func serviceInComposeProjection(projection environmentprojection.EnvironmentComposeProjection, serviceID string) bool {
	for _, service := range projection.DesiredServices {
		if service.Desired.ID == serviceID {
			return true
		}
	}
	for _, component := range projection.Components {
		for _, generated := range component.Runtime.GeneratedServices {
			if generated == serviceID {
				return true
			}
		}
	}
	return false
}
