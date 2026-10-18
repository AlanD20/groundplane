package backingcommand

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/moby/moby/client"
)

type Observation struct {
	Running  bool
	ExitCode int
}

// Inspect never starts an exec. Running=false proves exit only if the caller's
// journal has the readiness handshake; an exec merely created may start later.
// Missing Docker state is an error, not evidence that its effects are settled.
func (executor *Executor) Inspect(ctx context.Context, containerID, execID string) (Observation, error) {
	if len(containerID) != 64 || len(execID) != 64 || !ids.ValidContainerID(containerID) || !ids.ValidContainerID(execID) {
		return Observation{}, unavailable("backing command inspection requires exact process identities")
	}
	result, err := executor.engine.ExecInspect(ctx, execID, client.ExecInspectOptions{})
	if err != nil || result.ID != execID || result.ContainerID != containerID {
		return Observation{}, unavailable("captured backing command is unavailable for inspection")
	}
	return Observation{Running: result.Running, ExitCode: result.ExitCode}, nil
}
