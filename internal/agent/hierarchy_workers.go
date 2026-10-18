package agent

import (
	"errors"

	"github.com/AlanD20/groundplane/internal/common/hierarchyexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Removal uses the same local worker bound and process lease as other native
// operations. The Controller remains responsible for conflicts and assignment.
func (workers *executionWorkers) submitEnvironmentRemoval(delivery *agentpb.EnvironmentRemovalAssignment, available int) error {
	input, err := hierarchyexecution.DecodeEnvironmentAssignment(delivery, workers.identity)
	if err != nil {
		return err
	}
	workers.mu.Lock()
	defer workers.mu.Unlock()
	if workers.closed || workers.ctx.Err() != nil {
		return errs.New(errs.KindStateConflict, "Environment removal worker session is closed")
	}
	if active, exists := workers.active[input.Attempt.WorkItemID]; exists {
		if active.environment == nil || !proto.Equal(active.environment, delivery) {
			return errs.New(errs.KindStateConflict, "Environment removal already has a different active assignment")
		}
		return nil
	}
	if available <= 0 || len(workers.active) >= workers.limit {
		return errs.New(errs.KindStateConflict, "Agent execution capacity is exhausted")
	}
	execution, err := workers.ownership.Begin(input.Attempt)
	if err != nil {
		return err
	}
	owned := proto.CloneOf(delivery)
	workers.active[input.Attempt.WorkItemID] = activeExecution{attempt: input.Attempt, environment: owned}
	workers.wait.Add(1)
	go func() {
		defer workers.wait.Done()
		result, err := workers.hierarchy.RunEnvironment(workers.ctx, input, workers.exchange)
		err = errors.Join(err, execution.Close())
		select {
		case workers.outputs <- executionOutput{attempt: input.Attempt, environment: result.Progress, environmentFailure: result.Failure, err: err}:
		case <-workers.ctx.Done():
		}
	}()
	return nil
}
