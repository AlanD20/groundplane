package agent

import (
	"errors"

	"github.com/AlanD20/groundplane/internal/common/environmentexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (workers *executionWorkers) submitEnvironmentProvision(message *agentpb.EnvironmentProvisionAssignment, available int) error {
	input, err := environmentexecution.DecodeAssignment(message, workers.identity)
	if err != nil {
		return err
	}
	workers.mu.Lock()
	defer workers.mu.Unlock()
	if workers.closed || workers.ctx.Err() != nil {
		return errs.New(errs.KindStateConflict, "Environment provisioning session is closed")
	}
	if active, exists := workers.active[input.Attempt.WorkItemID]; exists {
		if active.environmentProvision == nil || !proto.Equal(active.environmentProvision, message) {
			return errs.New(errs.KindStateConflict, "Environment provisioning already has a different active delivery")
		}
		return nil
	}
	if available <= 0 || len(workers.active) >= workers.limit {
		return errs.New(errs.KindStateConflict, "Agent execution capacity is exhausted")
	}
	lease, err := workers.ownership.Begin(input.Attempt)
	if err != nil {
		return err
	}
	workers.active[input.Attempt.WorkItemID] = activeExecution{attempt: input.Attempt, environmentProvision: proto.CloneOf(message)}
	workers.wait.Add(1)
	go func() {
		defer workers.wait.Done()
		terminal, err := workers.environments.Run(workers.executionContext(input.Attempt), input, workers.exchange)
		err = errors.Join(err, lease.Close())
		select {
		case workers.outputs <- executionOutput{attempt: input.Attempt, environmentProvision: &terminal, err: err}:
		case <-workers.ctx.Done():
		}
	}()
	return nil
}
