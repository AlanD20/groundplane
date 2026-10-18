package agent

import (
	"errors"

	"github.com/AlanD20/groundplane/internal/common/resolverexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (workers *executionWorkers) submitResolver(delivery *agentpb.ResolverWorkAssignment, available int) error {
	input, err := resolverexecution.DecodeAssignment(delivery, workers.identity)
	if err != nil {
		return err
	}
	attempt := input.Attempt
	workers.mu.Lock()
	defer workers.mu.Unlock()
	if workers.closed || workers.ctx.Err() != nil {
		return errs.New(errs.KindStateConflict, "DNS resolver worker session is closed")
	}
	if active, exists := workers.active[attempt.WorkItemID]; exists {
		if active.resolver == nil || !proto.Equal(active.resolver, delivery) {
			return errs.New(errs.KindStateConflict, "DNS resolver already has a different active assignment")
		}
		return nil
	}
	if available <= 0 || len(workers.active) >= workers.limit {
		return errs.New(errs.KindStateConflict, "Agent execution capacity is exhausted")
	}
	execution, err := workers.ownership.Begin(attempt)
	if err != nil {
		return err
	}
	workers.active[attempt.WorkItemID] = activeExecution{attempt: attempt, resolver: proto.CloneOf(delivery)}
	workers.wait.Add(1)
	go func() {
		defer workers.wait.Done()
		terminal, err := workers.resolvers.Run(workers.executionContext(attempt), input, workers.exchange)
		err = errors.Join(err, execution.Close())
		select {
		case workers.outputs <- executionOutput{attempt: attempt, resolver: &terminal, err: err}:
		case <-workers.ctx.Done():
		}
	}()
	return nil
}

func (workers *executionWorkers) finishResolver(output executionOutput, active activeExecution) (bool, error) {
	if active.resolver == nil {
		if output.resolver != nil {
			return true, errs.New(errs.KindStateConflict, "DNS resolver result belongs to another active operation")
		}
		return false, nil
	}
	if output.err == nil {
		if output.resolver == nil || output.resolver.Validate() != nil || output.resolver.FinishedAt == nil ||
			output.resolver.Result == nil || !workexecution.SameAssignment(*output.resolver, output.attempt) {
			return true, errs.New(errs.KindStateConflict, "DNS resolver requires its saved terminal acknowledgement")
		}
		if err := workers.resolvers.Retire(workers.ctx, *output.resolver); err != nil {
			return true, err
		}
	}
	workers.retireLocalExecution(output.attempt.WorkItemID)
	return true, nil
}
