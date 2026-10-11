package composehelper

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/envoyrouter"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// The caller must select and authenticate the exact compiled Component container
// and pass the Task's immutable sources. No Docker name, current desired state,
// or successful command exit substitutes for that authority. The original step
// context bounds publication and all asynchronous acknowledgement waits.
func activateHTTPDiscoveryConfiguration(
	ctx context.Context,
	taskRunner runner.Runner,
	containerID string,
	previousSource, desiredSource []byte,
) error {
	activation, err := envoyrouter.PrepareActivation(previousSource, desiredSource)
	if err != nil {
		return err
	}
	return executeHTTPDiscovery(ctx, taskRunner, containerID, activation.Advance)
}

func restoreHTTPDiscoveryConfiguration(
	ctx context.Context, taskRunner runner.Runner, containerID string, priorSource, failedSource []byte,
) error {
	restoration, err := envoyrouter.PrepareRestoration(priorSource, failedSource)
	if err != nil {
		return err
	}
	return executeHTTPDiscovery(ctx, taskRunner, containerID, restoration.Advance)
}

func executeHTTPDiscovery(
	ctx context.Context, taskRunner runner.Runner, containerID string,
	advance func([]byte) (envoyrouter.Decision, error),
) error {
	if ctx == nil || taskRunner == nil || !validComponentContainerID(containerID) || advance == nil {
		return errs.New(errs.KindInternal, "HTTP router execution dependencies or owned container are missing")
	}
	run := func(command []string, input []byte) (runner.Result, error) {
		result, runErr := taskRunner.Run(ctx, runner.RunCmdOpts{
			Name: DockerExecutable,
			Args: append([]string{"container", "exec", "--interactive", "--user", "envoy", containerID}, command...),
			Dir:  WorkDirectory, Env: append([]string(nil), fixedEnvironment...), ReplaceEnv: true,
			Stdin: input, CaptureLimitBytes: envoyrouter.MaximumObservationBytes,
		})
		if ctx.Err() != nil {
			return runner.Result{}, ctx.Err()
		}
		if runErr != nil {
			return runner.Result{}, errs.Wrap(errs.KindRequestFailed, runErr)
		}
		if result.ExitCode != 0 {
			return runner.Result{}, errs.New(errs.KindRequestFailed,
				"HTTP router configuration publication or active-state observation failed")
		}
		return result, nil
	}
	for {
		observed, err := run([]string{"sh", "-ec", envoyrouter.ObservationCommand}, nil)
		if err != nil {
			return err
		}
		decision, err := advance(observed.Stdout)
		if err != nil {
			return err
		}
		if decision.Complete() {
			return nil
		}
		if publication, ready := decision.Publication(); ready {
			if _, err := run(publication.Command(), publication.Input()); err != nil {
				// An unknown publication outcome is resolved from the same captured
				// sources on a later invocation, never by blindly replaying here.
				return err
			}
			continue
		}
		timer := time.NewTimer(250 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
