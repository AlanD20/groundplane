package agent

import (
	"errors"

	"github.com/AlanD20/groundplane/internal/agent/volumeruntime"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (workers *executionWorkers) submitVolume(delivery *agentpb.VolumeWorkAssignment, available int) error {
	input, err := volumeruntime.DecodeAssignment(delivery, workers.identity)
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
		return errs.New(errs.KindStateConflict, "Volume worker session is closed")
	}
	if active, exists := workers.active[attempt.WorkItemID]; exists {
		if active.volume == nil || !proto.Equal(active.volume, delivery) {
			return errs.New(errs.KindStateConflict, "Volume already has a different active assignment")
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
	workers.active[attempt.WorkItemID] = activeExecution{attempt: attempt, volume: proto.CloneOf(delivery)}
	workers.wait.Add(1)
	go func() {
		defer workers.wait.Done()
		terminal, err := workers.volumes.Run(workers.executionContext(attempt), input, workers.exchange)
		err = errors.Join(err, execution.Close())
		select {
		case workers.outputs <- executionOutput{attempt: attempt, volume: &terminal, err: err}:
		case <-workers.ctx.Done():
		}
	}()
	return nil
}
