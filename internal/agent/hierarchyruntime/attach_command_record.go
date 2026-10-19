package hierarchyruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentownership"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type attachCommandRecorder struct {
	journal   *agentworkjournal.Journal
	input     attachexecution.RemovalInput
	ownership *agentownership.Lease
}

func (recorder attachCommandRecorder) Created(ctx context.Context, containerID, execID string, at time.Time) error {
	if err := recorder.ownership.Check(); err != nil {
		return err
	}
	return recorder.journal.PrepareAttachExec(ctx, recorder.input, agentworkjournal.AttachExec{
		ContainerID: containerID, ExecID: execID, CreatedAt: at,
	})
}

func (recorder attachCommandRecorder) Ready(ctx context.Context, execID string, at time.Time) error {
	if err := recorder.ownership.Check(); err != nil {
		return err
	}
	return recorder.journal.AuthorizeAttachExec(ctx, recorder.input, execID, at)
}

func (recorder attachCommandRecorder) Exited(ctx context.Context, execID string, exitCode int, at time.Time) error {
	return recorder.journal.ObserveAttachExecExit(ctx, recorder.input, execID, exitCode, at)
}

func (host *BackingHost) observeRemovalExecs(
	ctx context.Context,
	input attachexecution.RemovalInput,
	saved *agentworkjournal.AttachRemovalRecord,
) error {
	if err := host.ownership.Check(); err != nil {
		return err
	}
	if len(saved.Executions) == 0 {
		return nil // No recorded exec can receive command permission.
	}
	execution := saved.Executions[len(saved.Executions)-1]
	if execution.ReadyAt == nil || execution.ExitedAt != nil {
		return nil // No permission was sent, or exit was already persisted.
	}
	observed, err := host.commands.Inspect(ctx, execution.ContainerID, execution.ExecID)
	if err != nil {
		return err
	}
	if observed.Running {
		return errs.New(
			errs.KindStateConflict,
			"previous Attach command is still running; retry cannot start another command",
		)
	}
	// This does not report success or release resource claims. Controller-owned
	// explicit Retry still needs the captured interrupted-operation proof.
	return host.journal.ObserveAttachExecExit(ctx, input, execution.ExecID, observed.ExitCode, time.Now().UTC())
}
