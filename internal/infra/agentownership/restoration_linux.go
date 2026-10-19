//go:build linux

package agentownership

import (
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// CheckRestoration uses the captured cause without turning a successful Group
// member into a fictitious failed attempt. Failed-attempt cleanup retains its
// existing exact-work/attempt exclusion. Group restoration has a separate native
// work item and may proceed only after the successful source's local worker left.
func (lease *Lease) CheckRestoration(input releaseexecution.RecoveryInput) error {
	if err := input.ValidateIdentity(); err != nil {
		return err
	}
	if input.Group == nil {
		return lease.CheckRecovery(input.Forward, input.Input.Attempt)
	}
	if err := lease.Check(); err != nil {
		return err
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	current, exists := lease.active[input.Input.Attempt.WorkItemID]
	_, sourceRunning := lease.active[input.Forward.WorkItemID]
	if lease.closed || !lease.held || sourceRunning || !exists ||
		!workexecution.SameAssignment(current, input.Input.Attempt) {
		return errs.New(
			errs.KindStateConflict,
			"Group restoration must exclusively own its local worker after the successful member stops executing",
		)
	}
	return nil
}
