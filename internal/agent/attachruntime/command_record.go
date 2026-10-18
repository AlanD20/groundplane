package attachruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentownership"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type commandRecorder struct {
	journal   *agentworkjournal.Journal
	input     attachexecution.ProvisionInput
	ownership *agentownership.Lease
}

func (recorder commandRecorder) Created(ctx context.Context, containerID, execID string, at time.Time) error {
	if err := recorder.ownership.Check(); err != nil {
		return err
	}
	return recorder.journal.PrepareAttachProvisionExec(ctx, recorder.input, agentworkjournal.AttachExec{
		ContainerID: containerID, ExecID: execID, CreatedAt: at,
	})
}

func (recorder commandRecorder) Ready(ctx context.Context, execID string, at time.Time) error {
	if err := recorder.ownership.Check(); err != nil {
		return err
	}
	return recorder.journal.AuthorizeAttachProvisionExec(ctx, recorder.input, execID, at)
}

func (recorder commandRecorder) Exited(ctx context.Context, execID string, exitCode int, at time.Time) error {
	return recorder.journal.ObserveAttachProvisionExecExit(ctx, recorder.input, execID, exitCode, at)
}

func (host *BackingHost) observeProvisionExecs(ctx context.Context, input attachexecution.ProvisionInput, saved *agentworkjournal.AttachProvisionRecord) error {
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
		return errs.New(errs.KindStateConflict, "previous Attach command is still running; retry cannot start another command")
	}
	// This does not report success or release resource claims. Controller-owned
	// explicit Retry still needs the captured interrupted-operation proof.
	return host.journal.ObserveAttachProvisionExecExit(ctx, input, execution.ExecID, observed.ExitCode, time.Now().UTC())
}
