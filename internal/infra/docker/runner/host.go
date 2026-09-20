package runner

import (
	"context"
	"fmt"
	commandrunner "github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

const (
	hostDockerSocket = "/var/run/docker.sock"
	commandTimeout   = 30 * time.Second
	startupTimeout   = 90 * time.Second
	maximumOutput    = 64 * 1024
)

type localOperations struct{ commands commandrunner.Runner }

// NewLocal fixes the production Linux host boundary behind the validated
// HostControl facade. Docker API traffic uses the Runner's private socket;
// host administration commands retain the repository-wide subprocess seam.
func NewLocal(commands commandrunner.Runner) (*HostControl, error) {
	if commands == nil {
		return nil, errs.New(errs.KindInternal, "runner host command runner is required")
	}
	return New(&localOperations{commands: commands})
}

func (operations *localOperations) run(ctx context.Context, name string, args ...string) (commandrunner.Result, error) {
	return operations.runInput(ctx, nil, name, args...)
}

func (operations *localOperations) runInput(
	ctx context.Context,
	input []byte,
	name string,
	args ...string,
) (commandrunner.Result, error) {
	result, err := operations.commands.Run(ctx, commandrunner.RunCmdOpts{
		Name: name, Args: args, Timeout: commandTimeout, Stdin: input, CaptureLimitBytes: maximumOutput,
	})
	if err != nil {
		return result, errs.Wrap(errs.KindInternal, fmt.Errorf("runner host %s failed: %w", name, err))
	}
	if result.ExitCode != 0 {
		return result, errs.Newf(errs.KindInternal, "runner host %s exited with status %d", name, result.ExitCode)
	}
	return result, nil
}
