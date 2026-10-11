package composehelper

import (
	"context"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/common/serviceproxy"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func activateCapturedProxy(ctx context.Context, taskRunner runner.Runner, containerID string,
	implementation agentpb.ServiceProxyImplementation, manifest []byte,
) (*agentpb.ComposeHelperResponse, error) {
	if !validComponentContainerID(containerID) {
		return nil, errs.New(errs.KindValidationFailed, "proxy activation requires the exact owned container")
	}
	selected, err := executionplan.OpenProxyImplementation(implementation)
	if err != nil {
		return nil, err
	}
	activation, err := serviceproxy.PrepareActivation(selected, manifest)
	if err != nil {
		return nil, err
	}
	run := func(command []string, input []byte) (*agentpb.ComposeHelperResponse, error) {
		args := []string{"exec", "--interactive"}
		if activation.User != "" {
			args = append(args, "--user", activation.User)
		}
		args = append(args, containerID)
		result, err := taskRunner.Run(ctx, runner.RunCmdOpts{
			Name: DockerExecutable, Args: append(args, command...), Dir: WorkDirectory,
			Env: slices.Clone(fixedEnvironment), ReplaceEnv: true, Stdin: slices.Clone(input),
			CaptureLimitBytes: maximumComponentConfig,
		})
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if err != nil {
			return nil, errs.Wrap(errs.KindRequestFailed, err)
		}
		if result.ExitCode != 0 {
			return failedProxyResponse(), nil
		}
		return nil, nil
	}
	if selected == serviceproxy.Envoy {
		// Validate the captured full routing manifest without serving traffic or
		// disturbing PID1. Configuration is stdin data, not executable shell text.
		validation := []string{
			"sh",
			"-ec",
			"umask 077; gp_validation=$(mktemp /tmp/gp-envoy-validate.XXXXXX); " +
				"trap 'rm -f -- \"$gp_validation\"' EXIT; cat > \"$gp_validation\"; test -s \"$gp_validation\"; " +
				"envoy --mode validate --disable-hot-restart --config-path \"$gp_validation\"",
		}
		if failure, err := run(validation, manifest); err != nil || failure != nil {
			return failure, err
		}
	}
	return run(activation.Command, activation.Input)
}

// Filesystem CDS applies asynchronously. Read-only confirmation can wait within
// the original step deadline; it never repeats publication or renews its budget.
func awaitRestorationProxy(ctx context.Context, taskRunner runner.Runner, containerID string,
	implementation agentpb.ServiceProxyImplementation, digest []byte,
) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for {
		proven, err := observeRestorationProxy(ctx, taskRunner, containerID, implementation, digest)
		if proven || err != nil {
			return proven, err
		}
		timer := time.NewTimer(250 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false, ctx.Err()
		case <-timer.C:
		}
	}
}
