package entryruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/entryexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentownership"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Consumers is the host executor for the exact captured workload instances.
// Reconcile joins its helper before returning. Inspect must account for an
// existing helper and may not infer termination from a timed-out Controller RPC.
type Consumers interface {
	Stop(context.Context, entryexecution.FileInput) error
	Restore(context.Context, entryexecution.FileInput, bool) (bool, error)
	Reconcile(context.Context, entryexecution.FileInput) error
	Inspect(context.Context, entryexecution.FileInput) (bool, error)
}

type RuntimeHost struct {
	journal       *agentworkjournal.Journal
	ownership     *agentownership.Lease
	configuration *Configuration
	consumers     Consumers
}

func NewHost(journal *agentworkjournal.Journal, ownership *agentownership.Lease,
	configuration *Configuration, consumers Consumers,
) (*RuntimeHost, error) {
	if journal == nil || configuration == nil || consumers == nil {
		return nil, errs.New(
			errs.KindInternal,
			"Entry host requires its journal, file materializer and workload executor",
		)
	}
	if err := ownership.Check(); err != nil {
		return nil, err
	}
	return &RuntimeHost{journal: journal, ownership: ownership, configuration: configuration, consumers: consumers}, nil
}

func (host *RuntimeHost) Apply(ctx context.Context, input Input, stage entryexecution.Stage) error {
	_, err := host.execute(ctx, input, stage, false)
	return err
}

func (host *RuntimeHost) Inspect(
	ctx context.Context,
	input Input,
	stage entryexecution.Stage,
) (entryexecution.Observation, error) {
	return host.execute(ctx, input, stage, true)
}

func (host *RuntimeHost) execute(
	ctx context.Context,
	input Input,
	stage entryexecution.Stage,
	inspectOnly bool,
) (entryexecution.Observation, error) {
	if ctx == nil || input.procedure == nil || input.attempt.StartedAt == nil {
		return entryexecution.Observation{}, invalidInput()
	}
	if err := host.ownership.Check(); err != nil {
		return entryexecution.Observation{}, err
	}
	saved, err := host.journal.EntryConfiguration(ctx, input.attempt, input.procedure)
	if err != nil {
		return entryexecution.Observation{}, err
	}
	if saved == nil || !saved.Acknowledged || saved.Completion != nil || saved.Failure != nil ||
		!saved.Progress.InFlight || saved.Progress.Stage != stage {
		return entryexecution.Observation{}, errs.New(
			errs.KindStateConflict,
			"Entry host action has no acknowledged execution stage",
		)
	}
	captured := entryexecution.FileInput{Attempt: input.attempt, Procedure: input.procedure, Progress: saved.Progress}
	observed := entryexecution.Observation{
		EnvironmentID: input.procedure.EnvironmentId,
		Input:         input.attempt.Input,
		Stage:         stage,
	}
	switch stage {
	case entryexecution.WriteConfiguration, entryexecution.VerifyConfiguration:
		ready, err := host.configuration.Run(ctx, captured, inspectOnly || stage == entryexecution.VerifyConfiguration)
		if err != nil {
			return entryexecution.Observation{}, err
		}
		if stage == entryexecution.WriteConfiguration {
			observed.FilesWritten = ready
		} else {
			observed.FilesVerified = ready
		}
	case entryexecution.ReconcileConsumers, entryexecution.VerifyConsumers:
		if input.procedure.Removal || len(input.procedure.RunningConsumerIds) == 0 {
			return entryexecution.Observation{}, invalidInput()
		}
		if !inspectOnly && stage == entryexecution.ReconcileConsumers {
			if err := host.consumers.Reconcile(ctx, captured); err != nil {
				return entryexecution.Observation{}, err
			}
		}
		ready, err := host.consumers.Inspect(ctx, captured)
		if err != nil {
			return entryexecution.Observation{}, err
		}
		if stage == entryexecution.ReconcileConsumers {
			observed.ConsumersReconciled = ready
		} else {
			observed.ConsumersVerified = ready
		}
	default:
		return entryexecution.Observation{}, invalidInput()
	}
	if err := host.ownership.Check(); err != nil {
		return entryexecution.Observation{}, err
	}
	observed.At = time.Now().UTC()
	if !inspectOnly && !observed.Satisfies(stage) {
		return entryexecution.Observation{}, errs.New(
			errs.KindStateConflict,
			"Entry action finished without the required file or workload observation",
		)
	}
	return observed, nil
}
