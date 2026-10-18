package hostplatform

import (
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/operationretention"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
)

type softwareWorkRegistrations struct {
	admission  []workdispatch.Registration
	controller []workdispatch.ControllerRegistration
	retry      []workdispatch.RetryRegistration
	retention  []operationretention.WorkRegistration
	completion []workdispatch.CompletionRegistration
	input      []workdispatch.InputRegistration
}

// A foreground Controller without native bootstrap can prepare artifacts, but
// has no activation owner. Do not register a nil handler or a fallback engine.
func (platform *Platform) softwareWork() softwareWorkRegistrations {
	owner := platform.activations
	if owner == nil {
		return softwareWorkRegistrations{}
	}
	return softwareWorkRegistrations{
		admission:  []workdispatch.Registration{{Operation: workexecution.SoftwareStaging, Executor: workexecution.Controller, Owner: owner}},
		controller: []workdispatch.ControllerRegistration{{Operation: workexecution.SoftwareStaging, Owner: owner}},
		retry:      []workdispatch.RetryRegistration{{Operation: workexecution.SoftwareStaging, Owner: owner}},
		retention:  []operationretention.WorkRegistration{{Operation: workexecution.SoftwareStaging, Owner: owner}},
		completion: []workdispatch.CompletionRegistration{{Action: workexecution.ActionActivate, Owner: owner}},
		input: []workdispatch.InputRegistration{
			{Action: workexecution.ActionActivate, Operation: workexecution.ControllerSoftware, Owner: owner},
			{Action: workexecution.ActionActivate, Operation: workexecution.AgentSoftware, Owner: owner},
		},
	}
}
