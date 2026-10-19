// Package resolverruntime executes the fixed captured DNS resolver procedure.
package resolverruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/resolverexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentownership"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type Exchange interface {
	StartAttempt(context.Context, workexecution.Attempt) (*agentpb.WorkStartAck, error)
	ResolverCheckpoint(
		context.Context,
		*agentpb.ResolverWorkCheckpointRequest,
	) (*agentpb.ResolverWorkCheckpointAck, error)
	CompleteResolver(context.Context, *agentpb.ResolverWorkCompletionRequest) (*agentpb.ResolverWorkResultAck, error)
	FailResolver(context.Context, *agentpb.ResolverWorkFailureRequest) (*agentpb.ResolverWorkResultAck, error)
	RecoverResolver(context.Context, *agentpb.ResolverWorkRecoveryRequest) (*agentpb.ResolverWorkResultAck, error)
}

// Helper must join the physical process before returning. StopResolver joins
// the exact forward/cleanup helpers, not arbitrary containers on the host.
type Helper interface {
	ExecuteResolver(context.Context, *agentpb.ResolverWorkHelperRequest) (*agentpb.ResolverWorkHelperResponse, error)
	StopResolver(context.Context, *agentpb.ResolverWorkHelperRequest) error
}

type Executor struct {
	journal   *agentworkjournal.Journal
	helper    Helper
	ownership *agentownership.Lease
}

func New(journal *agentworkjournal.Journal, helper Helper, ownership *agentownership.Lease) (*Executor, error) {
	if journal == nil || helper == nil {
		return nil, errs.New(errs.KindInternal, "DNS resolver execution requires its journal and physical helper")
	}
	if err := ownership.Check(); err != nil {
		return nil, err
	}
	return &Executor{journal: journal, helper: helper, ownership: ownership}, nil
}

func (executor *Executor) Run(
	ctx context.Context,
	assignment resolverexecution.Assignment,
	exchange Exchange,
) (workexecution.Attempt, error) {
	if ctx == nil || exchange == nil || assignment.Attempt.Validate() != nil {
		return workexecution.Attempt{}, invalidInput()
	}
	if err := executor.ownership.Check(); err != nil {
		return workexecution.Attempt{}, err
	}
	start, err := exchange.StartAttempt(ctx, assignment.Attempt)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	started, reconcile, err := workexecution.DecodeAgentStartAck(start, assignment.Attempt)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	assignment.Attempt.StartedAt = &started
	if assignment.Recovery != nil {
		return executor.runRecovery(ctx, assignment, exchange, start)
	}
	layout, err := resolverexecution.CaptureLayout(assignment.Attempt, assignment.Input)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	saved, err := executor.journal.Resolver(ctx, assignment.Attempt, assignment.Input)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if saved != nil && saved.Receipt != nil {
		return *saved.Receipt, nil
	}
	if saved != nil && saved.FailureReason != "" {
		return executor.deliverFailure(ctx, assignment, exchange, saved.FailureReason)
	}
	if saved == nil && reconcile {
		return executor.fail(
			ctx,
			assignment,
			exchange,
			errs.New(
				errs.KindStateConflict,
				"CoreDNS started previously but its local journal is unavailable; captured restoration is required",
			),
		)
	}
	var progress resolverexecution.Progress
	if saved != nil && saved.Progress != nil {
		progress = *saved.Progress
		if !saved.Acknowledged {
			if err := executor.acknowledge(ctx, assignment, exchange, progress); err != nil {
				if progress.InFlight {
					return executor.fail(ctx, assignment, exchange, err)
				}
				return workexecution.Attempt{}, err
			}
		}
	} else {
		progress, err = resolverexecution.NewProgress(assignment.Attempt, layout, time.Now().UTC())
		if err != nil {
			return executor.fail(ctx, assignment, exchange, err)
		}
	}
	for progress.Stage != resolverexecution.Complete {
		if err := ctx.Err(); err != nil {
			return executor.fail(ctx, assignment, exchange, err)
		}
		fresh := !progress.InFlight
		if fresh {
			progress, err = progress.Begin(assignment.Attempt, layout, time.Now().UTC())
			if err != nil {
				return executor.fail(ctx, assignment, exchange, err)
			}
			if err := executor.save(ctx, assignment, exchange, progress); err != nil {
				return executor.fail(ctx, assignment, exchange, err)
			}
		}
		observation, proof, err := executor.observe(ctx, assignment, start, progress, fresh)
		if err != nil {
			return executor.fail(ctx, assignment, exchange, err)
		}
		if proof != nil {
			if err := executor.journal.SaveResolverProof(ctx, assignment.Attempt, assignment.Input, proof); err != nil {
				return executor.fail(ctx, assignment, exchange, err)
			}
		}
		progress, err = progress.Confirm(assignment.Attempt, layout, observation, time.Now().UTC())
		if err != nil {
			return executor.fail(ctx, assignment, exchange, err)
		}
		if err := executor.save(ctx, assignment, exchange, progress); err != nil {
			return workexecution.Attempt{}, err
		}
	}
	return executor.complete(ctx, assignment, exchange)
}

func (executor *Executor) observe(
	ctx context.Context,
	assignment resolverexecution.Assignment,
	start *agentpb.WorkStartAck,
	progress resolverexecution.Progress,
	fresh bool,
) (resolverexecution.Observation, *agentpb.DNSResolverObservationEvidence, error) {
	if progress.Stage == resolverexecution.VerifyDNS {
		saved, err := executor.journal.Resolver(ctx, assignment.Attempt, assignment.Input)
		if err != nil {
			return resolverexecution.Observation{}, nil, err
		}
		if saved != nil && saved.Proof != nil {
			observation, err := resolverexecution.ServingObservation(assignment.Attempt, assignment.Input, saved.Proof)
			return observation, saved.Proof, err
		}
	}
	request, captured, err := executor.helperRequest(ctx, assignment, start, true)
	if err != nil {
		return resolverexecution.Observation{}, nil, err
	}
	inspection, cancel := context.WithTimeout(ctx, 60*time.Second)
	response, err := executor.helper.ExecuteResolver(inspection, request)
	cancel()
	if err != nil {
		return resolverexecution.Observation{}, nil, err
	}
	if err := executor.ownership.Check(); err != nil {
		return resolverexecution.Observation{}, nil, err
	}
	observation, proof, err := resolverexecution.ReadHelperObservation(captured, response)
	if err != nil || observation.Satisfies(progress.Stage, captured.Layout) || !fresh {
		return observation, proof, err
	}
	request.InspectOnly = false
	captured.InspectOnly = false
	operation, cancel := context.WithDeadline(ctx, assignment.Attempt.Deadline)
	response, err = executor.helper.ExecuteResolver(operation, request)
	cancel()
	if err != nil {
		return resolverexecution.Observation{}, nil, err
	}
	if err := executor.ownership.Check(); err != nil {
		return resolverexecution.Observation{}, nil, err
	}
	return resolverexecution.ReadHelperObservation(captured, response)
}

func invalidInput() error {
	return errs.New(errs.KindValidationFailed, "DNS resolver execution does not match its captured assignment")
}
