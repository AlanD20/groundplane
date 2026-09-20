package hierarchy

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// EnvironmentProvisioningState is the closed durable directory lifecycle
// recorded independently of runtime health.
type EnvironmentProvisioningState string

const (
	EnvironmentProvisioningProvisioning EnvironmentProvisioningState = "provisioning"
	EnvironmentProvisioningReady        EnvironmentProvisioningState = "ready"
	EnvironmentProvisioningFailed       EnvironmentProvisioningState = "failed"
)

func validateEnvironmentProvisioning(record EnvironmentRecord) error {
	switch record.ProvisioningState {
	case EnvironmentProvisioningProvisioning,
		EnvironmentProvisioningReady,
		EnvironmentProvisioningFailed:
	default:
		return errs.New(errs.KindValidationFailed, "environment provisioning_state is invalid")
	}
	if err := ids.Validate(ids.KindTask, record.CreateTaskID); err != nil {
		return errs.New(errs.KindValidationFailed, "environment create_task_id is invalid")
	}
	return nil
}
