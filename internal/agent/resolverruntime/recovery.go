package resolverruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/resolverexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (executor *Executor) runRecovery(ctx context.Context, assignment resolverexecution.Assignment, exchange Exchange,
	start *agentpb.WorkStartAck,
) (workexecution.Attempt, error) {
	saved, err := executor.journal.PrepareResolverRecovery(
		ctx,
		assignment.Attempt,
		assignment.Input,
		*assignment.Recovery,
	)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if saved.Receipt != nil {
		return *saved.Receipt, nil
	}
	if saved.Recovery != nil {
		return executor.deliverRecovery(ctx, assignment, exchange, *saved.Recovery)
	}
	request, captured, err := executor.helperRequest(ctx, assignment, start, false)
	if err != nil {
		return executor.recoveryFailure(ctx, assignment, exchange)
	}
	bounded, cancel := context.WithDeadline(ctx, assignment.Attempt.Deadline)
	defer cancel()
	// Join exact earlier helpers before allowing the fixed captured restoration.
	// The launcher must also join its own process before returning its proof.
	if err := executor.helper.StopResolver(bounded, request); err != nil {
		return executor.recoveryFailure(ctx, assignment, exchange)
	}
	if err := executor.ownership.Check(); err != nil {
		return executor.recoveryFailure(ctx, assignment, exchange)
	}
	response, err := executor.helper.ExecuteResolver(bounded, request)
	if err != nil {
		return executor.recoveryFailure(ctx, assignment, exchange)
	}
	if err := executor.ownership.Check(); err != nil {
		return executor.recoveryFailure(ctx, assignment, exchange)
	}
	observation, err := resolverexecution.ReadHelperRestoration(captured, response)
	if err != nil {
		return executor.recoveryFailure(ctx, assignment, exchange)
	}
	report := resolverexecution.RecoveryReport{
		AttemptID:             assignment.Attempt.ID,
		ForwardID:             assignment.Recovery.Forward.ID,
		Input:                 assignment.Attempt.Input,
		Failure:               assignment.Recovery.Evidence,
		Status:                resolverexecution.RecoveryComplete,
		HelpersStopped:        true,
		ConfigurationRestored: observation.ConfigurationRestored,
		RuntimeRestored:       observation.RuntimeRestored,
		HostResolverRestored:  observation.HostResolverRestored,
		ServingProof:          observation.ServingProof,
		ObservedAt:            observation.ObservedAt,
	}
	if err := executor.journal.SaveResolverRecovery(ctx, assignment.Attempt, assignment.Input, report); err != nil {
		return workexecution.Attempt{}, err
	}
	return executor.deliverRecovery(ctx, assignment, exchange, report)
}

func (executor *Executor) recoveryFailure(
	ctx context.Context,
	assignment resolverexecution.Assignment,
	exchange Exchange,
) (workexecution.Attempt, error) {
	at := time.Now().UTC()
	status := resolverexecution.RecoveryFailed
	if !at.Before(assignment.Attempt.Deadline) {
		status = resolverexecution.RecoveryTimedOut
	} else if ctx.Err() != nil {
		status = resolverexecution.RecoveryInterrupted
	}
	report := resolverexecution.RecoveryReport{
		AttemptID:  assignment.Attempt.ID,
		ForwardID:  assignment.Recovery.Forward.ID,
		Input:      assignment.Attempt.Input,
		Failure:    assignment.Recovery.Evidence,
		Status:     status,
		ObservedAt: at,
	}
	bounded, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()
	if err := executor.journal.SaveResolverRecovery(bounded, assignment.Attempt, assignment.Input, report); err != nil {
		return workexecution.Attempt{}, err
	}
	return executor.deliverRecovery(bounded, assignment, exchange, report)
}

func (executor *Executor) deliverRecovery(
	ctx context.Context,
	assignment resolverexecution.Assignment,
	exchange Exchange,
	report resolverexecution.RecoveryReport,
) (workexecution.Attempt, error) {
	raw, _, err := resolverexecution.EncodeRecoveryReport(
		assignment.Attempt,
		assignment.Input,
		*assignment.Recovery,
		report,
	)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	identity, err := workexecution.EncodeAgentAttempt(assignment.Attempt)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	ack, err := exchange.RecoverResolver(ctx, &agentpb.ResolverWorkRecoveryRequest{Attempt: identity, Report: raw})
	if err != nil {
		return workexecution.Attempt{}, err
	}
	return executor.journal.AcknowledgeResolverRecovery(ctx, assignment.Attempt, assignment.Input, ack)
}
