// Package backingruntime executes the captured Backing creation procedure.
package backingruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/agent/materialization"
	"github.com/AlanD20/groundplane/internal/common/backingprovision"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentownership"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/internal/infra/docker/backingcommand"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type Exchange interface {
	StartAttempt(context.Context, workexecution.Attempt) (*agentpb.WorkStartAck, error)
	BackingProvisionCheckpoint(context.Context, *agentpb.BackingProvisionCheckpointRequest) (*agentpb.BackingProvisionCheckpointAck, error)
	CompleteBackingProvision(context.Context, *agentpb.BackingProvisionCompletionRequest) (*agentpb.BackingProvisionCompletionAck, error)
	FailBackingProvision(context.Context, *agentpb.BackingProvisionFailureRequest) (*agentpb.BackingProvisionFailureAck, error)
	BackingProvisionConfiguration(context.Context, *agentpb.BackingProvisionConfigurationRequest) (*agentpb.BackingProvisionConfigurationManifest, error)
	BackingProvisionFile(context.Context, *agentpb.BackingProvisionFileRequest) (*agentpb.BackingProvisionFileDelivery, error)
	BackingProvisionHook(context.Context, *agentpb.BackingProvisionHookRequest) (*agentpb.BackingProvisionHookDelivery, error)
	RecoverBackingProvision(context.Context, *agentpb.BackingProvisionRecoveryRequest) (*agentpb.BackingProvisionRecoveryAck, error)
}

func (executor *Executor) Run(ctx context.Context, assignment backingprovision.Assignment, exchange Exchange) (workexecution.Attempt, error) {
	if assignment.Attempt.Direction == workexecution.Recovery {
		return executor.RunRecovery(ctx, assignment, exchange)
	}
	return executor.RunForward(ctx, assignment, exchange)
}

type Helper interface {
	ExecuteBackingProvision(context.Context, string, *agentpb.BackingProvisionHelperRequest) (*agentpb.BackingProvisionHelperResponse, error)
	StopBackingProvision(context.Context, *agentpb.BackingProvisionHelperRequest) error
}

type Executor struct {
	volumeRoot   string
	journal      *agentworkjournal.Journal
	materializer *materialization.Runtime
	helper       Helper
	commands     *backingcommand.Executor
	ownership    *agentownership.Lease
}

func NewExecutor(volumeRoot string, journal *agentworkjournal.Journal, materializer *materialization.Runtime,
	helper Helper, commands *backingcommand.Executor, ownership *agentownership.Lease,
) (*Executor, error) {
	if volumeRoot == "" || journal == nil || materializer == nil || helper == nil || commands == nil {
		return nil, errs.New(errs.KindInternal, "backing creation requires its journal, file/runtime helpers and command executor")
	}
	if err := ownership.Check(); err != nil {
		return nil, err
	}
	return &Executor{volumeRoot: volumeRoot, journal: journal, materializer: materializer,
		helper: helper, commands: commands, ownership: ownership}, nil
}

func (executor *Executor) RunForward(ctx context.Context, assignment backingprovision.Assignment, exchange Exchange) (workexecution.Attempt, error) {
	if ctx == nil || exchange == nil || assignment.Validate() != nil || assignment.Recovery != nil ||
		assignment.Attempt.Direction != workexecution.Forward || assignment.Procedure.VolumeRoot != executor.volumeRoot {
		return workexecution.Attempt{}, invalidInput()
	}
	if err := executor.ownership.Check(); err != nil {
		return workexecution.Attempt{}, err
	}
	ack, err := exchange.StartAttempt(ctx, assignment.Attempt)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	started, reconcile, err := workexecution.DecodeAgentStartAck(ack, assignment.Attempt)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	assignment.Attempt.StartedAt = &started
	layout, err := backingprovision.CaptureLayout(assignment.Attempt, assignment.Procedure)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	saved, err := executor.journal.Backing(ctx, assignment.Attempt, assignment.Procedure)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if saved != nil && saved.Completion != nil {
		return *saved.Completion, nil
	}
	if saved != nil && saved.Failure != nil {
		return executor.deliverFailure(ctx, assignment, exchange, saved.Failure)
	}
	if saved == nil && reconcile {
		return executor.fail(ctx, assignment, exchange, errs.New(errs.KindStateConflict,
			"backing creation started previously but its local checkpoint is unavailable; cleanup inspection is required"))
	}
	var progress backingprovision.Progress
	if saved != nil {
		if saved.Progress == nil {
			return workexecution.Attempt{}, invalidInput()
		}
		progress = *saved.Progress
		if !saved.Acknowledged {
			if err := executor.acknowledge(ctx, assignment, exchange, progress); err != nil {
				// A confirmed effect report must be replayed, not converted to
				// failure merely because its acknowledgement was lost.
				if progress.InFlight {
					return executor.fail(ctx, assignment, exchange, err)
				}
				return workexecution.Attempt{}, err
			}
		}
	} else {
		progress, err = backingprovision.NewProgress(assignment.Attempt, layout, time.Now().UTC())
		if err != nil {
			return executor.fail(ctx, assignment, exchange, err)
		}
	}
	for progress.Stage != backingprovision.Complete {
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
		bounded, cancel := context.WithDeadline(ctx, assignment.Attempt.Deadline)
		if !fresh {
			cancel()
			bounded, cancel = context.WithTimeout(ctx, 30*time.Second)
		}
		observation, executionErr := executor.execute(bounded, assignment, exchange, progress.Stage, !fresh)
		cancel()
		if executionErr != nil {
			return executor.fail(ctx, assignment, exchange, executionErr)
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

func (executor *Executor) Retire(ctx context.Context, terminal workexecution.Attempt) error {
	return executor.journal.Retire(ctx, terminal)
}

func invalidInput() error {
	return errs.New(errs.KindStateConflict, "backing creation execution does not match its captured assignment and checkpoint")
}
