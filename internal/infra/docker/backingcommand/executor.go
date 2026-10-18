// Package backingcommand executes captured backing commands behind a durable
// stdin gate. Disconnects never authorize replay of a Docker exec.
package backingcommand

import (
	"context"
	"io"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/moby/moby/client"
)

type Engine interface {
	ExecCreate(context.Context, string, client.ExecCreateOptions) (client.ExecCreateResult, error)
	ExecAttach(context.Context, string, client.ExecAttachOptions) (client.ExecAttachResult, error)
	ExecInspect(context.Context, string, client.ExecInspectOptions) (client.ExecInspectResult, error)
}

// Recorder persists each boundary before Execute advances. Ready must refuse
// repeated permission grants; Created must refuse an existing exec identity.
type Recorder interface {
	Created(context.Context, string, string, time.Time) error
	Ready(context.Context, string, time.Time) error
	Exited(context.Context, string, int, time.Time) error
}

type Executor struct{ engine Engine }

func New(engine Engine) (*Executor, error) {
	if engine == nil {
		return nil, unavailable("backing command Docker client is unavailable")
	}
	return &Executor{engine: engine}, nil
}

const readyMarker = "groundplane-command-ready\n"

// read is a shell builtin and consumes only the permission line. The remaining
// stdin belongs to the command. EOF without permission exits without effects.
const gateScript = `printf 'groundplane-command-ready\n'
IFS= read -r permission || exit 125
[ "$permission" = groundplane-command-run ] || exit 125
exec "$@"`

func (executor *Executor) Execute(ctx context.Context, recorder Recorder, containerID string, command, environment []string, input []byte, output io.Writer) (int, error) {
	if ctx == nil || recorder == nil || output == nil || len(containerID) != 64 || !ids.ValidContainerID(containerID) || len(command) == 0 {
		return 0, unavailable("backing command requires its exact container and durable recorder")
	}
	if _, bounded := ctx.Deadline(); !bounded {
		return 0, unavailable("backing command requires an execution deadline")
	}
	args := append([]string{"/bin/sh", "-c", gateScript, "groundplane-command"}, command...)
	created, err := executor.engine.ExecCreate(ctx, containerID, client.ExecCreateOptions{
		AttachStdin: true, AttachStdout: true, AttachStderr: true,
		Cmd: args, Env: environment, TTY: false, Privileged: false,
	})
	if err != nil || len(created.ID) != 64 || !ids.ValidContainerID(created.ID) {
		return 0, unavailable("backing command could not create a Docker exec")
	}
	if err := recorder.Created(ctx, containerID, created.ID, time.Now().UTC()); err != nil {
		return 0, err // ExecCreate alone cannot run the command.
	}
	attached, err := executor.engine.ExecAttach(ctx, created.ID, client.ExecAttachOptions{TTY: false})
	if err != nil || attached.Conn == nil || attached.Reader == nil {
		if err == nil {
			attached.Close()
		}
		return 0, unavailable("backing command did not establish its input gate")
	}
	defer attached.Close()
	stopClose := context.AfterFunc(ctx, attached.Close)
	defer stopClose()
	ready, done := make(chan struct{}), make(chan struct{})
	var streamErr error
	go func() {
		streamErr = drainOutput(attached.Reader, ready, output)
		close(done)
	}()
	defer func() { attached.Close(); <-done }()
	select {
	case <-ready:
	case <-done:
		return 0, unavailable("backing command exited before its readiness handshake")
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	if err := recorder.Ready(ctx, created.ID, time.Now().UTC()); err != nil {
		return 0, err
	}
	const permission = "groundplane-command-run\n"
	if written, err := io.WriteString(attached.Conn, permission); err != nil || written != len(permission) {
		return 0, unavailable("backing command permission delivery is uncertain")
	}
	if err := writeInput(attached.Conn, input); err != nil {
		return 0, err
	}
	select {
	case <-done:
		if streamErr != nil {
			return 0, unavailable("backing command output ended unexpectedly")
		}
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	for {
		result, err := executor.engine.ExecInspect(ctx, created.ID, client.ExecInspectOptions{})
		if err != nil || result.ID != created.ID || result.ContainerID != containerID {
			return 0, unavailable("backing command exit could not be inspected")
		}
		if !result.Running {
			if err := recorder.Exited(ctx, created.ID, result.ExitCode, time.Now().UTC()); err != nil {
				return 0, err
			}
			return result.ExitCode, nil
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return 0, ctx.Err()
		}
	}
}

func unavailable(detail string) error { return errs.New(errs.KindStateConflict, detail) }
