package resolverruntime

import (
	"context"
	"errors"
	"time"

	"github.com/AlanD20/groundplane/internal/common/resolverexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (executor *Executor) acknowledge(
	ctx context.Context,
	assignment resolverexecution.Assignment,
	exchange Exchange,
	progress resolverexecution.Progress,
) error {
	layout, err := resolverexecution.CaptureLayout(assignment.Attempt, assignment.Input)
	if err != nil {
		return err
	}
	raw, err := resolverexecution.EncodeProgress(assignment.Attempt, layout, progress)
	if err != nil {
		return err
	}
	identity, err := workexecution.EncodeAgentAttempt(assignment.Attempt)
	if err != nil {
		return err
	}
	ack, err := exchange.ResolverCheckpoint(
		ctx,
		&agentpb.ResolverWorkCheckpointRequest{Attempt: identity, Progress: raw},
	)
	if err != nil {
		return err
	}
	return executor.journal.AcknowledgeResolver(ctx, assignment.Attempt, assignment.Input, ack)
}

func (executor *Executor) save(
	ctx context.Context,
	assignment resolverexecution.Assignment,
	exchange Exchange,
	progress resolverexecution.Progress,
) error {
	if _, err := executor.journal.PrepareResolver(ctx, assignment.Attempt, assignment.Input, progress); err != nil {
		return err
	}
	return executor.acknowledge(ctx, assignment, exchange, progress)
}

func (executor *Executor) complete(
	ctx context.Context,
	assignment resolverexecution.Assignment,
	exchange Exchange,
) (workexecution.Attempt, error) {
	saved, err := executor.journal.Resolver(ctx, assignment.Attempt, assignment.Input)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if saved == nil || saved.Progress == nil || !saved.Acknowledged {
		return workexecution.Attempt{}, invalidInput()
	}
	evidence, err := resolverexecution.CompletionEvidence(
		assignment.Attempt,
		assignment.Input,
		*saved.Progress,
		saved.Verification,
		saved.Proof,
	)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	encoded, err := resolverexecution.EncodeReference(evidence)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	identity, err := workexecution.EncodeAgentAttempt(assignment.Attempt)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	ack, err := exchange.CompleteResolver(
		ctx,
		&agentpb.ResolverWorkCompletionRequest{
			Attempt:      identity,
			Evidence:     encoded,
			ServingProof: proto.CloneOf(saved.Proof),
		},
	)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	return executor.journal.CompleteResolver(ctx, assignment.Attempt, assignment.Input, ack)
}

func (executor *Executor) fail(
	ctx context.Context,
	assignment resolverexecution.Assignment,
	exchange Exchange,
	cause error,
) (workexecution.Attempt, error) {
	reason := resolverexecution.ExecutionFailed
	at := time.Now().UTC()
	if !at.Before(assignment.Attempt.Deadline) {
		reason = resolverexecution.DeadlineExpired
	} else if ctx.Err() != nil || errors.Is(cause, context.Canceled) {
		reason = resolverexecution.Interrupted
	}
	bounded, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()
	saved, err := executor.journal.PrepareResolverFailure(bounded, assignment.Attempt, assignment.Input, reason, at)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	return executor.deliverFailure(bounded, assignment, exchange, saved.FailureReason)
}

func (executor *Executor) deliverFailure(
	ctx context.Context,
	assignment resolverexecution.Assignment,
	exchange Exchange,
	reason resolverexecution.FailureReason,
) (workexecution.Attempt, error) {
	identity, err := workexecution.EncodeAgentAttempt(assignment.Attempt)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	ack, err := exchange.FailResolver(
		ctx,
		&agentpb.ResolverWorkFailureRequest{Attempt: identity, Reason: string(reason)},
	)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	return executor.journal.AcknowledgeResolverFailure(ctx, assignment.Attempt, assignment.Input, ack)
}

func (executor *Executor) helperRequest(
	ctx context.Context,
	assignment resolverexecution.Assignment,
	start *agentpb.WorkStartAck,
	inspect bool,
) (*agentpb.ResolverWorkHelperRequest, resolverexecution.HelperInput, error) {
	if err := executor.ownership.Check(); err != nil {
		return nil, resolverexecution.HelperInput{}, err
	}
	identity, err := workexecution.EncodeAgentAttempt(assignment.Attempt)
	if err != nil {
		return nil, resolverexecution.HelperInput{}, err
	}
	raw, _, err := resolverexecution.Capture(assignment.Attempt.Input.ID, assignment.Input)
	if err != nil {
		return nil, resolverexecution.HelperInput{}, err
	}
	request := &agentpb.ResolverWorkHelperRequest{
		Assignment:  &agentpb.ResolverWorkAssignment{Attempt: identity, Input: raw},
		Start:       proto.CloneOf(start),
		InspectOnly: inspect,
	}
	if assignment.Recovery != nil {
		request.Assignment.Recovery, err = resolverexecution.EncodeRecovery(*assignment.Recovery)
		if err != nil {
			return nil, resolverexecution.HelperInput{}, err
		}
	} else {
		saved, err := executor.journal.Resolver(ctx, assignment.Attempt, assignment.Input)
		if err != nil {
			return nil, resolverexecution.HelperInput{}, err
		}
		if saved == nil || saved.Progress == nil || !saved.Acknowledged {
			return nil, resolverexecution.HelperInput{}, invalidInput()
		}
		// Reconstruct only an ACK already validated and durably acknowledged;
		// an unacknowledged local cursor can never authorize a helper.
		progress, err := resolverexecution.EncodeProgress(assignment.Attempt, saved.Layout, *saved.Progress)
		if err != nil {
			return nil, resolverexecution.HelperInput{}, err
		}
		cursor, err := saved.Progress.Checkpoint(assignment.Attempt, saved.Layout)
		if err != nil {
			return nil, resolverexecution.HelperInput{}, err
		}
		evidence, err := resolverexecution.EncodeReference(cursor.Evidence)
		if err != nil {
			return nil, resolverexecution.HelperInput{}, err
		}
		request.Checkpoint = &agentpb.ResolverWorkCheckpointAck{Attempt: proto.CloneOf(identity), Progress: progress, Evidence: evidence}
	}
	captured, err := resolverexecution.OpenHelperRequest(request)
	return request, captured, err
}

func (executor *Executor) Retire(ctx context.Context, terminal workexecution.Attempt) error {
	return executor.journal.RetireResolver(ctx, terminal)
}
