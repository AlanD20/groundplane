package hierarchyruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/agent/backingadapter"
	"github.com/AlanD20/groundplane/internal/agent/backinghookruntime"
	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/agentownership"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/internal/infra/docker/backingcommand"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type BackingHost struct {
	journal   *agentworkjournal.Journal
	adapter   *backingadapter.Runtime
	ownership *agentownership.Lease
	commands  *backingcommand.Executor
}

func NewBackingHost(journal *agentworkjournal.Journal, adapter *backingadapter.Runtime,
	ownership *agentownership.Lease, commands *backingcommand.Executor,
) (*BackingHost, error) {
	if journal == nil || adapter == nil || commands == nil {
		return nil, errs.New(
			errs.KindInternal,
			"Attach removal host requires its journal, adapter catalog and durable command executor",
		)
	}
	if err := ownership.Check(); err != nil {
		return nil, err
	}
	return &BackingHost{journal: journal, adapter: adapter, ownership: ownership, commands: commands}, nil
}

func (host *BackingHost) ExecuteRemovalCommand(ctx context.Context, input attachexecution.RemovalInput,
	progress attachexecution.RemovalProgress, inspectOnly bool,
) (attachexecution.RemovalCommandResult, error) {
	if ctx == nil || progress.Validate(input) != nil || !progress.InFlight {
		return attachexecution.RemovalCommandResult{}, errs.New(
			errs.KindStateConflict,
			"Attach command requires an acknowledged in-flight boundary",
		)
	}
	saved, err := host.journal.AttachRemoval(ctx, input)
	if err != nil {
		return attachexecution.RemovalCommandResult{}, err
	}
	if saved == nil || !saved.Acknowledged {
		return attachexecution.RemovalCommandResult{}, errs.New(
			errs.KindStateConflict,
			"Attach command has no acknowledged Agent journal boundary",
		)
	}
	want, err := (attachexecution.RemovalReport{Progress: progress}).Checkpoint(input)
	if err != nil {
		return attachexecution.RemovalCommandResult{}, err
	}
	got, err := saved.Report.Checkpoint(input)
	if err != nil || got.Sequence != want.Sequence || got.Evidence != want.Evidence {
		return attachexecution.RemovalCommandResult{}, errs.New(
			errs.KindStateConflict,
			"Attach command differs from its saved authorization",
		)
	}
	if result := saved.CommandResult; result != nil {
		return *result, nil
	}
	if inspectOnly || len(saved.Executions) != 0 {
		if err := host.observeRemovalExecs(ctx, input, saved); err != nil {
			return attachexecution.RemovalCommandResult{}, err
		}
		return attachexecution.RemovalCommandResult{}, errs.New(
			errs.KindStateConflict,
			"Attach command was interrupted without a saved result; automatic replay is disabled",
		)
	}
	if err := host.ownership.Check(); err != nil {
		return attachexecution.RemovalCommandResult{}, err
	}
	ordinal, err := input.CredentialCommandOrdinal(progress.Completed)
	if err != nil {
		return attachexecution.RemovalCommandResult{}, err
	}
	containerID, err := host.adapter.BackingContainer(ctx, input.Removal.BackingServiceId)
	if err != nil {
		return attachexecution.RemovalCommandResult{}, err
	}
	if hook := input.Removal.GetCustomDetach(); hook != nil {
		if err := host.adapter.VerifyBackingContainer(ctx, containerID, input.Removal.BackingServiceId); err != nil {
			return attachexecution.RemovalCommandResult{}, err
		}
		definition, values, _, err := executionplan.DecodeBackingHookProcedure(hook)
		if err != nil {
			return attachexecution.RemovalCommandResult{}, err
		}
		recorder := attachCommandRecorder{journal: host.journal, input: input, ownership: host.ownership}
		if err := backinghookruntime.ExecuteDetach(ctx, host.commands, recorder, containerID, definition, values); err != nil {
			return attachexecution.RemovalCommandResult{}, err
		}
	} else {
		procedures, err := attachexecution.CredentialProcedures(input.Removal)
		if err != nil {
			return attachexecution.RemovalCommandResult{}, err
		}
		recorder := attachCommandRecorder{journal: host.journal, input: input, ownership: host.ownership}
		if err := host.adapter.ExecuteCapturedProcedure(ctx, procedures[ordinal], containerID, host.commands, recorder); err != nil {
			return attachexecution.RemovalCommandResult{}, err
		}
	}
	result := attachexecution.RemovalCommandResult{
		Ordinal:    progress.Completed,
		StartedAt:  *progress.BeganAt,
		FinishedAt: time.Now().UTC(),
	}
	// A cancelled delivery still must durably save its observed successful
	// return; this bounded write does not authorize another external effect.
	storage, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if err := host.journal.SaveAttachCommandResult(storage, input, result); err != nil {
		return attachexecution.RemovalCommandResult{}, err
	}
	return result, nil
}
