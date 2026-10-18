package agent

import (
	"errors"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (workers *executionWorkers) submitAttachProvision(delivery *agentpb.AttachProvisionAssignment, available int) error {
	input, err := attachexecution.DecodeProvisionAssignment(delivery, workers.identity)
	if err != nil {
		return err
	}
	transferred := false
	defer func() {
		if !transferred {
			attachexecution.ClearProvision(input.Provision)
		}
	}()
	workers.mu.Lock()
	defer workers.mu.Unlock()
	if workers.closed || workers.ctx.Err() != nil {
		return errs.New(errs.KindStateConflict, "Attach provisioning worker session is closed")
	}
	if active, exists := workers.active[input.Attempt.WorkItemID]; exists {
		if active.provision == nil || !proto.Equal(active.provision, delivery) {
			return errs.New(errs.KindStateConflict, "Attach provisioning already has a different active assignment")
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
	workers.active[input.Attempt.WorkItemID] = activeExecution{attempt: input.Attempt, provision: proto.CloneOf(delivery)}
	workers.wait.Add(1)
	transferred = true
	go func() {
		defer workers.wait.Done()
		defer attachexecution.ClearProvision(input.Provision)
		result, err := workers.attachProvision.Run(workers.ctx, input, workers.exchange)
		err = errors.Join(err, execution.Close())
		select {
		case workers.outputs <- executionOutput{attempt: input.Attempt, provision: &result, err: err}:
		case <-workers.ctx.Done():
		}
	}()
	return nil
}

func (workers *executionWorkers) consumeAttachProvision(active activeExecution, output executionOutput) error {
	defer attachexecution.ClearProvision(active.provision.GetProvision())
	if output.err != nil {
		return nil
	}
	result := output.provision
	if result == nil || (result.Progress == nil) == (result.Failure == nil) {
		return errs.New(errs.KindStateConflict, "Attach provisioning requires exactly one acknowledged completion or failure")
	}
	input, err := attachexecution.DecodeProvisionAssignment(active.provision, workers.identity)
	if err != nil {
		return err
	}
	defer attachexecution.ClearProvision(input.Provision)
	if result.Progress != nil {
		authority, err := input.Authority()
		if err != nil {
			return err
		}
		if result.Progress.InFlight || result.Progress.Completed != authority.Commands || result.Progress.AttemptID != input.Attempt.ID {
			return errs.New(errs.KindStateConflict, "Attach provisioning result is missing captured commands")
		}
	}
	if failure := result.Failure; failure != nil && (failure.Validate() != nil || failure.FinishedAt == nil ||
		failure.Result == nil || failure.Outcome == workexecution.Succeeded || !workexecution.SameAssignment(*failure, input.Attempt)) {
		return errs.New(errs.KindStateConflict, "Attach provisioning failure lacks a saved terminal acknowledgement")
	}
	return workers.attachProvision.Retire(workers.ctx, input)
}
