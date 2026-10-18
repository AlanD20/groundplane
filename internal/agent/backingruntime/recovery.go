package backingruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backingprovision"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (executor *Executor) RunRecovery(ctx context.Context, assignment backingprovision.Assignment, exchange Exchange) (workexecution.Attempt, error) {
	if ctx == nil || exchange == nil || assignment.Validate() != nil || assignment.Recovery == nil ||
		assignment.Procedure.VolumeRoot != executor.volumeRoot {
		return workexecution.Attempt{}, invalidInput()
	}
	if err := executor.ownership.Check(); err != nil {
		return workexecution.Attempt{}, err
	}
	ack, err := exchange.StartAttempt(ctx, assignment.Attempt)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	started, _, err := workexecution.DecodeAgentStartAck(ack, assignment.Attempt)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	assignment.Attempt.StartedAt = &started
	saved, err := executor.journal.PrepareBackingRecovery(ctx, assignment)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if saved.Receipt != nil {
		return *saved.Receipt, nil
	}
	if saved.Report == nil {
		bounded, cancel := context.WithDeadline(ctx, assignment.Attempt.Deadline)
		report, executionErr := executor.cleanup(bounded, assignment)
		cancel()
		if executionErr != nil {
			status := backingprovision.RecoveryFailed
			if !time.Now().Before(assignment.Attempt.Deadline) {
				status = backingprovision.RecoveryTimedOut
			} else if ctx.Err() != nil {
				status = backingprovision.RecoveryInterrupted
			}
			report = backingprovision.RecoveryReport{AttemptID: assignment.Attempt.ID, ForwardID: assignment.Recovery.Forward.ID,
				Failure: assignment.Recovery.Evidence, Input: assignment.Attempt.Input, ServiceID: saved.Layout.ServiceID,
				Status: status, ObservedAt: time.Now().UTC()}
		}
		delivery, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
		defer cancel()
		if err := executor.journal.SaveBackingRecoveryReport(delivery, assignment, report); err != nil {
			return workexecution.Attempt{}, err
		}
		saved.Report = &report
		ctx = delivery
	}
	raw, _, err := backingprovision.EncodeRecoveryReport(assignment.Attempt, saved.Source, saved.Layout, *saved.Report)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	identity, err := workexecution.EncodeAgentAttempt(assignment.Attempt)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	completed, err := exchange.RecoverBackingProvision(ctx, &agentpb.BackingProvisionRecoveryRequest{Attempt: identity, Report: raw})
	if err != nil {
		return workexecution.Attempt{}, err
	}
	return executor.journal.AcknowledgeBackingRecovery(ctx, assignment, completed)
}

func (executor *Executor) cleanup(ctx context.Context, assignment backingprovision.Assignment) (backingprovision.RecoveryReport, error) {
	if err := executor.ownership.Check(); err != nil {
		return backingprovision.RecoveryReport{}, err
	}
	message, err := backingprovision.EncodeAssignment(assignment)
	if err != nil {
		return backingprovision.RecoveryReport{}, err
	}
	identity, err := workexecution.EncodeAgentAttempt(assignment.Attempt)
	if err != nil {
		return backingprovision.RecoveryReport{}, err
	}
	request := &agentpb.BackingProvisionHelperRequest{Assignment: message,
		Start: &agentpb.WorkStartAck{Attempt: identity, StartedAt: timestamppb.New(*assignment.Attempt.StartedAt)}}
	input, err := backingprovision.OpenHelperRequest(request)
	if err != nil {
		return backingprovision.RecoveryReport{}, err
	}
	if err := executor.helper.StopBackingProvision(ctx, request); err != nil {
		return backingprovision.RecoveryReport{}, err
	}
	if err := executor.materializer.StopBackingRecoveryFiles(ctx, input); err != nil {
		return backingprovision.RecoveryReport{}, err
	}
	if err := executor.ownership.Check(); err != nil {
		return backingprovision.RecoveryReport{}, err
	}
	response, err := executor.helper.ExecuteBackingProvision(ctx, executor.volumeRoot, request)
	if err != nil {
		return backingprovision.RecoveryReport{}, err
	}
	observed, err := backingprovision.ReadHelperRecovery(input, response)
	if err != nil {
		return backingprovision.RecoveryReport{}, err
	}
	if err := executor.ownership.Check(); err != nil {
		return backingprovision.RecoveryReport{}, err
	}
	report := backingprovision.RecoveryReport{AttemptID: assignment.Attempt.ID, ForwardID: assignment.Recovery.Forward.ID,
		Failure: assignment.Recovery.Evidence, Input: assignment.Attempt.Input, ServiceID: input.Layout.ServiceID,
		Status: backingprovision.RecoveryComplete, HelperStopped: true, FilesStopped: true, RuntimeAbsent: observed.RuntimeAbsent, ObservedAt: observed.ObservedAt}
	return report, report.Validate(assignment.Attempt, *assignment.Recovery, input.Layout)
}
