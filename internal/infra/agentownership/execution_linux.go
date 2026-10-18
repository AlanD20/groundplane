//go:build linux

package agentownership

import (
	"sync"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Execution holds process-local exclusion until the caller has joined all work
// started for this attempt. It is not a Controller assignment or effect grant.
type Execution struct {
	mu      sync.Mutex
	lease   *Lease
	attempt workexecution.Attempt
	closed  bool
}

func (lease *Lease) Begin(attempt workexecution.Attempt) (*Execution, error) {
	if attempt.Validate() != nil || attempt.Executor.Executor != workexecution.Agent || attempt.FinishedAt != nil {
		return nil, errs.New(errs.KindValidationFailed, "Agent process execution requires an unfinished assigned attempt")
	}
	if err := lease.Check(); err != nil {
		return nil, err
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if lease.closed || !lease.held || lease.active == nil {
		return nil, errs.New(errs.KindStateConflict, "Agent process ownership is closed")
	}
	if _, exists := lease.active[attempt.WorkItemID]; exists {
		return nil, errs.New(errs.KindStateConflict, "this work item already has a local executor")
	}
	lease.active[attempt.WorkItemID] = attempt
	return &Execution{lease: lease, attempt: attempt}, nil
}

// CheckRecovery excludes a still-running local forward execution. For another
// process, possession of the shared lifetime lock excludes its conforming Agent.
// Neither condition proves that an orphan helper or a Docker request has ended.
func (lease *Lease) CheckRecovery(forward, recovery workexecution.Attempt) error {
	if forward.Validate() != nil || recovery.Validate() != nil || forward.Direction != workexecution.Forward ||
		recovery.Direction != workexecution.Recovery || forward.FinishedAt == nil || recovery.FinishedAt != nil ||
		forward.Executor.Executor != workexecution.Agent || recovery.Executor.Executor != workexecution.Agent ||
		forward.Executor.HostID != recovery.Executor.HostID || forward.TaskID != recovery.TaskID ||
		forward.WorkItemID != recovery.WorkItemID || forward.Input != recovery.Input || forward.Number >= recovery.Number {
		return errs.New(errs.KindStateConflict, "recovery process ownership differs from the failed execution")
	}
	if err := lease.Check(); err != nil {
		return err
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	active, exists := lease.active[recovery.WorkItemID]
	if lease.closed || !lease.held || !exists || !workexecution.SameAssignment(active, recovery) {
		return errs.New(errs.KindStateConflict, "recovery does not exclusively own this work item's local executor")
	}
	return nil
}

func (execution *Execution) Close() error {
	if execution == nil {
		return nil
	}
	execution.mu.Lock()
	defer execution.mu.Unlock()
	if execution.closed {
		return nil
	}
	if execution.lease == nil {
		return errs.New(errs.KindStateConflict, "Agent execution has no process owner")
	}
	lease := execution.lease
	lease.mu.Lock()
	defer lease.mu.Unlock()
	active, exists := lease.active[execution.attempt.WorkItemID]
	if !exists || !workexecution.SameAssignment(active, execution.attempt) {
		return errs.New(errs.KindStateConflict, "Agent execution ownership changed before shutdown")
	}
	delete(lease.active, execution.attempt.WorkItemID)
	execution.closed = true
	return nil
}
