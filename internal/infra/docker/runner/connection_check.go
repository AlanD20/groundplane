package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"time"

	commandrunner "github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	"github.com/AlanD20/groundplane/internal/common/runnerconnectioncheck"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ConnectionChecker runs the image's fixed diagnostic helper in an existing
// ownership-proved Runner. It cannot create or resume a Runner or its daemon.
type ConnectionChecker struct{ operations *localOperations }

func NewConnectionChecker(commands commandrunner.Runner) (*ConnectionChecker, error) {
	if commands == nil {
		return nil, errs.New(errs.KindInternal, "Runner connection checker requires its command runner")
	}
	return &ConnectionChecker{operations: &localOperations{commands: commands}}, nil
}

func (checker *ConnectionChecker) Check(
	ctx context.Context,
	plan runnerallocation.RuntimePlan,
	expected runnerallocation.RunnerRuntimeEvidence,
	request runnerconnectioncheck.Request,
) (runnerconnectioncheck.Result, error) {
	if checker == nil || checker.operations == nil || !expected.Valid() {
		return runnerconnectioncheck.Result{}, errs.New(
			errs.KindValidationFailed,
			"Runner connection-check ownership is required",
		)
	}
	if err := validateCall(ctx, plan); err != nil {
		return runnerconnectioncheck.Result{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, runnerconnectioncheck.Timeout+10*time.Second)
	defer cancel()
	engine, err := newEngine(hostDockerSocket)
	if err != nil {
		return runnerconnectioncheck.Result{}, err
	}
	defer engine.Close()
	observed, found, err := checker.operations.inspectRunner(ctx, engine, plan)
	if err != nil {
		return runnerconnectioncheck.Result{}, err
	}
	if !found || observed != expected {
		return runnerconnectioncheck.Result{}, errs.New(
			errs.KindStateConflict,
			"Runner container or rootless daemon changed; refresh before checking connections",
		)
	}
	if request.Validate() != nil || request.RunnerID != plan.RunnerID || request.RuntimeEpoch != plan.RuntimeEpoch ||
		request.PlanSHA256 != plan.Digest() ||
		request.ControllerEndpoint != plan.Egress.ControllerEndpoint.String() ||
		request.RunnerHome != plan.Paths.RunnerHome {
		return runnerconnectioncheck.Result{}, errs.New(
			errs.KindValidationFailed,
			"Runner diagnostic request differs from its captured plan",
		)
	}
	input, err := json.Marshal(request)
	if err != nil {
		return runnerconnectioncheck.Result{}, errs.Wrap(errs.KindInternal, err)
	}
	output, runErr := checker.operations.commands.Run(ctx, commandrunner.RunCmdOpts{
		Name: "docker", Args: []string{"--host=unix://" + hostDockerSocket, "exec", "--interactive", "--user", plan.Container.User,
			expected.ContainerID, "/usr/local/bin/groundplane", runnerconnectioncheck.HelperArgument},
		Stdin: input, Timeout: runnerconnectioncheck.Timeout + 5*time.Second, CaptureLimitBytes: runnerconnectioncheck.MaximumOutputBytes,
	})
	defer clear(output.Stdout)
	defer clear(output.Stderr)
	if runErr != nil || output.ExitCode != 0 {
		if ctx.Err() != nil {
			return runnerconnectioncheck.Result{}, ctx.Err()
		}
		return runnerconnectioncheck.Result{}, errs.New(
			errs.KindStorageUnavailable,
			"Runner diagnostic process did not complete; check its runtime and matching installed Runner image",
		)
	}
	result, err := runnerconnectioncheck.ReadResult(bytes.NewReader(output.Stdout), request)
	if err != nil {
		return runnerconnectioncheck.Result{}, err
	}
	// Do not record a result against a runtime replaced during the check.
	observed, found, err = checker.operations.inspectRunner(ctx, engine, plan)
	if err != nil {
		return runnerconnectioncheck.Result{}, err
	}
	if !found || observed != expected {
		return runnerconnectioncheck.Result{}, errs.New(
			errs.KindStateConflict,
			"Runner runtime changed during its connection check; start a new check",
		)
	}
	return result, nil
}
