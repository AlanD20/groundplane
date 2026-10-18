package transport

import (
	"github.com/AlanD20/groundplane/internal/controller/agentchannel"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (runtime *Runtime) ConfigureEnvironmentProvision(owner agentchannel.EnvironmentProvisionReporter) error {
	if runtime == nil || owner == nil || runtime.environmentProvision != nil {
		return errs.New(errs.KindInternal, "Environment provisioning reporting is missing or already configured")
	}
	runtime.environmentProvision = owner
	return nil
}

func (runtime *Runtime) ConfigureServiceLifecycle(owner agentchannel.LifecycleReporter) error {
	if runtime == nil || owner == nil || runtime.lifecycleReports != nil {
		return errs.New(errs.KindInternal, "Service lifecycle reporting is missing or already configured")
	}
	runtime.lifecycleReports = owner
	return nil
}

func (runtime *Runtime) ConfigureServiceRemovalRecovery(owner agentchannel.RemovalRecoveryReporter) error {
	if runtime == nil || owner == nil || runtime.removalRecoveryReports != nil {
		return errs.New(errs.KindInternal, "Service removal recovery reporting is missing or already configured")
	}
	runtime.removalRecoveryReports = owner
	return nil
}
