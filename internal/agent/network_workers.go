package agent

import (
	"errors"

	"github.com/AlanD20/groundplane/internal/agent/networkruntime"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (workers *executionWorkers) submitNetwork(delivery *agentpb.NetworkWorkAssignment, available int) error {
	input, err := networkruntime.DecodeAssignment(delivery, workers.identity)
	if err != nil {
		return err
	}
	attempt, err := workexecution.DecodeAgentAttempt(delivery.Attempt, workers.identity)
	if err != nil {
		return err
	}
	workers.mu.Lock()
	defer workers.mu.Unlock()
	if workers.closed || workers.ctx.Err() != nil {
		return errs.New(errs.KindStateConflict, "Network worker session is closed")
	}
	if active, exists := workers.active[attempt.WorkItemID]; exists {
		if active.network == nil || !proto.Equal(active.network, delivery) {
			return errs.New(errs.KindStateConflict, "Network already has a different active assignment")
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
	workers.active[attempt.WorkItemID] = activeExecution{attempt: attempt, network: proto.CloneOf(delivery)}
	workers.wait.Add(1)
	go func() {
		defer workers.wait.Done()
		terminal, err := workers.networks.Run(workers.executionContext(attempt), input, workers.exchange)
		err = errors.Join(err, execution.Close())
		select {
		case workers.outputs <- executionOutput{attempt: attempt, network: &terminal, err: err}:
		case <-workers.ctx.Done():
		}
	}()
	return nil
}
