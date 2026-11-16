package workexecution

import "github.com/AlanD20/groundplane/pkg/errs"

// AgentConcurrency bounds distinct active Tasks and their individual native
// operations. Sequential execution stages do not consume additional capacity.
type AgentConcurrency struct {
	Tasks             uint16
	OperationsPerTask uint16
}

func (limits AgentConcurrency) Capacity() uint16 {
	return limits.Tasks * limits.OperationsPerTask
}

func (limits AgentConcurrency) Validate() error {
	if limits.Tasks == 0 || limits.OperationsPerTask == 0 ||
		uint32(limits.Tasks)*uint32(limits.OperationsPerTask) > 256 {
		return errs.New(
			errs.KindValidationFailed,
			"Agent task and per-Task operation limits must be positive with at most 256 total operations",
		)
	}
	return nil
}
