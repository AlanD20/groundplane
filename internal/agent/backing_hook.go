package agent

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	backingHookHelperTimeout = 10 * time.Second
	backingHookKillAfter     = 5 * time.Second
	backingHookRunGrace      = 10 * time.Second
	backingHookCaptureBytes  = 64 * 1024
	backingHookResultPrefix  = "/tmp/groundplane-hook."
	backingHookResultSuffix  = "/result"
	backingHookComplete      = "groundplane-hook-complete\n"

	// Custom backing runtime images require /bin/sh, a timeout implementation
	// supporting -k, and the baseline mktemp, chmod, cat, and rm utilities.
	// The operator hook itself executes its explicit argv without shell
	// expansion; timeout owns its TERM-then-KILL deadline inside the container.
	backingHookSetupScript = `set -eu
umask 077
timeout -k 1s 1s /bin/sh -c ':'
directory=$(mktemp -d /tmp/groundplane-hook.XXXXXX)
trap 'rm -rf "$directory"' 0
chmod 700 "$directory"
result="$directory/result"
: > "$result"
chmod 600 "$result"
printf '%s\n' "$result"
trap - 0`
	backingHookRunScript = `set -eu
kill_after=$1
duration=$2
shift 2
set +e
timeout -k "$kill_after" "$duration" "$@" >/dev/null 2>&1
status=$?
set -e
printf '%s\n' groundplane-hook-complete
exit "$status"`
	backingHookReadScript = `set -eu
case "$GP_RESULT_FILE" in /tmp/groundplane-hook.??????/result) ;; *) exit 64 ;; esac
[ -f "$GP_RESULT_FILE" ] && [ ! -L "$GP_RESULT_FILE" ]
cat "$GP_RESULT_FILE"`
	backingHookCleanupScript = `set -eu
case "$GP_RESULT_FILE" in /tmp/groundplane-hook.??????/result) ;; *) exit 64 ;; esac
directory=${GP_RESULT_FILE%/result}
[ -d "$directory" ] && [ ! -L "$directory" ]
rm -rf "$directory"`
)

type backingHookEnvironment struct {
	name  string
	value string
}

// ExecuteBackingHook executes one sealed custom-backing hook inside its
// already-selected backing container. Resolved values cross Docker only by
// environment, never by command text or argv.
func ExecuteBackingHook(
	ctx context.Context,
	taskRunner runner.Runner,
	containerID string,
	definition backinghook.Definition,
	input backinghook.Input,
	schema []backinghook.FactDefinition,
) (output backinghook.Output, resultErr error) {
	if ctx == nil || taskRunner == nil || !validContainerID(containerID) {
		return output, errs.New(errs.KindValidationFailed, "agent: backing hook runtime is invalid")
	}
	if err := backinghook.Validate(definition, input, schema); err != nil {
		return output, err
	}
	resultFile, err := prepareBackingHookResult(ctx, taskRunner, containerID)
	if err != nil {
		return output, err
	}
	defer func() {
		cleanupCtx, cancel := backingHookCleanupContext(ctx)
		defer cancel()
		cleanupErr := cleanupBackingHookResult(cleanupCtx, taskRunner, containerID, resultFile)
		if cleanupErr == nil {
			return
		}
		if resultErr == nil {
			output.Clear()
			resultErr = cleanupErr
			return
		}
		resultErr = errors.Join(resultErr, cleanupErr)
	}()

	environment := append(
		backingHookEnvironmentFor(input),
		backingHookEnvironment{name: "GP_RESULT_FILE", value: resultFile},
	)
	defer func() { clearBackingHookEnvironment(environment) }()
	hookTimeout := time.Duration(definition.TimeoutSeconds) * time.Second
	hookTimeout, err = boundedBackingHookTimeout(ctx, hookTimeout)
	if err != nil {
		return output, err
	}
	if err := ctx.Err(); err != nil {
		return output, err
	}
	executionCtx, cancelExecution, executionTimeout, err := backingHookExecutionContext(ctx, hookTimeout)
	if err != nil {
		return output, err
	}
	defer cancelExecution()
	runResult, runErr := taskRunner.Run(executionCtx, runner.RunCmdOpts{
		Name: "docker", Args: backingHookDockerExecArgs(
			containerID,
			environment,
			append(
				[]string{
					"/bin/sh", "-c", backingHookRunScript, "groundplane-hook",
					backingHookDurationArgument(backingHookKillAfter), backingHookDurationArgument(hookTimeout),
				},
				definition.Command...,
			),
		),
		Env:     backingHookProcessEnvironment(environment),
		Timeout: executionTimeout, CaptureLimitBytes: backingHookCaptureBytes,
	})
	completionObserved := string(runResult.Stdout) == backingHookComplete
	clear(runResult.Stdout)
	clear(runResult.Stderr)
	if !completionObserved {
		// A Docker client error does not establish what happened to the
		// process created by docker exec. Retain this hook's authority until
		// the in-container timeout, KILL window, and observation grace have
		// elapsed; only then may deferred cleanup remove its result file.
		if executionCtx.Err() == nil {
			<-executionCtx.Done()
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return output, ctxErr
		}
		return output, errs.New(errs.KindInternal, "agent: backing hook command completion was not observed")
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return output, ctxErr
	}
	if runResult.ExitCode != 0 {
		return output, errs.New(errs.KindInternal, "agent: backing hook command failed")
	}
	if runErr != nil {
		return output, errs.New(errs.KindInternal, "agent: backing hook command execution failed")
	}

	readCtx, cancelRead := backingHookReadContext(ctx)
	content, err := readBackingHookResult(readCtx, taskRunner, containerID, resultFile)
	cancelRead()
	if err != nil {
		return output, err
	}
	defer clear(content)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return output, ctxErr
	}
	if input.Context.Event != backinghook.Attach {
		return backinghook.ParseResult(nil, content)
	}
	return backinghook.ParseResult(schema, content)
}

func boundedBackingHookTimeout(ctx context.Context, requested time.Duration) (time.Duration, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return 0, errs.New(errs.KindInternal, "agent: backing hook Task deadline is missing")
	}
	available := time.Until(deadline) - backingHookKillAfter - backingHookRunGrace - 2*backingHookHelperTimeout
	if available <= 0 {
		return 0, errs.New(errs.KindStateConflict, "agent: backing hook Task budget is exhausted")
	}
	if requested < available {
		return requested, nil
	}
	return available, nil
}

// backingHookExecutionContext deliberately preserves an in-flight Docker exec
// across Task cancellation. Canceling the Docker client is not evidence that
// the process created by docker exec stopped; the in-container timeout owns
// termination, and the Agent remains joined until it observes completion. The
// absolute deadline retains one helper budget for reading and one for cleanup.
func backingHookExecutionContext(
	ctx context.Context,
	hookTimeout time.Duration,
) (context.Context, context.CancelFunc, time.Duration, error) {
	taskDeadline, ok := ctx.Deadline()
	if !ok {
		return nil, nil, 0, errs.New(errs.KindInternal, "agent: backing hook Task deadline is missing")
	}
	joinDeadline := time.Now().Add(hookTimeout + backingHookKillAfter + backingHookRunGrace)
	latestJoinDeadline := taskDeadline.Add(-2 * backingHookHelperTimeout)
	if latestJoinDeadline.Before(joinDeadline) {
		joinDeadline = latestJoinDeadline
	}
	joinTimeout := time.Until(joinDeadline)
	if joinTimeout <= 0 {
		return nil, nil, 0, errs.New(errs.KindStateConflict, "agent: backing hook Task budget is exhausted")
	}
	executionCtx, cancel := context.WithDeadline(context.WithoutCancel(ctx), joinDeadline)
	return executionCtx, cancel, joinTimeout, nil
}

func backingHookReadContext(ctx context.Context) (context.Context, context.CancelFunc) {
	deadline := time.Now().Add(backingHookHelperTimeout)
	if taskDeadline, ok := ctx.Deadline(); ok {
		reserved := taskDeadline.Add(-backingHookHelperTimeout)
		if reserved.Before(deadline) {
			deadline = reserved
		}
	}
	return context.WithDeadline(context.WithoutCancel(ctx), deadline)
}

func backingHookCleanupContext(ctx context.Context) (context.Context, context.CancelFunc) {
	deadline := time.Now().Add(backingHookHelperTimeout)
	if taskDeadline, ok := ctx.Deadline(); ok && taskDeadline.Before(deadline) {
		deadline = taskDeadline
	}
	return context.WithDeadline(context.WithoutCancel(ctx), deadline)
}

func backingHookEnvironmentFor(input backinghook.Input) []backingHookEnvironment {
	result := []backingHookEnvironment{
		{name: "GP_EVENT", value: string(input.Context.Event)},
		{name: "GP_BACKING_SERVICE_ID", value: input.Context.BackingServiceID},
	}
	for _, item := range []backingHookEnvironment{
		{name: "GP_ATTACH_ID", value: input.Context.AttachID},
		{name: "GP_TENANT_ID", value: input.Context.TenantID},
		{name: "GP_PROJECT_ID", value: input.Context.ProjectID},
		{name: "GP_ENVIRONMENT_ID", value: input.Context.EnvironmentID},
		{name: "GP_SERVICE_ID", value: input.Context.ServiceID},
	} {
		if item.value != "" {
			result = append(result, item)
		}
	}
	for _, value := range input.Values {
		result = append(result, backingHookEnvironment{name: "GP_INPUT_" + value.Key, value: string(value.Value)})
	}
	for _, value := range input.Facts {
		result = append(result, backingHookEnvironment{name: "GP_FACT_" + value.Key, value: string(value.Value)})
	}
	return result
}

func backingHookDockerExecArgs(
	containerID string,
	environment []backingHookEnvironment,
	command []string,
) []string {
	args := []string{"container", "exec", "-i"}
	for _, variable := range environment {
		args = append(args, "--env", variable.name)
	}
	args = append(args, containerID)
	return append(args, command...)
}

func backingHookDurationArgument(value time.Duration) string {
	return strconv.FormatInt(int64(value/time.Second), 10) + "s"
}

func backingHookProcessEnvironment(environment []backingHookEnvironment) []string {
	result := make([]string, 0, len(environment))
	for _, variable := range environment {
		result = append(result, variable.name+"="+variable.value)
	}
	return result
}

func prepareBackingHookResult(
	ctx context.Context,
	taskRunner runner.Runner,
	containerID string,
) (string, error) {
	result, err := taskRunner.Run(ctx, runner.RunCmdOpts{
		Name:    "docker",
		Args:    []string{"container", "exec", "-i", containerID, "/bin/sh", "-c", backingHookSetupScript},
		Timeout: backingHookHelperTimeout, CaptureLimitBytes: 512,
	})
	defer clear(result.Stdout)
	defer clear(result.Stderr)
	if err != nil || result.ExitCode != 0 {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return "", ctxErr
		}
		return "", errs.New(errs.KindInternal, "agent: backing hook result setup failed")
	}
	resultFile := strings.TrimSuffix(string(result.Stdout), "\n")
	if strings.Contains(resultFile, "\n") || !validBackingHookResultFile(resultFile) {
		return "", errs.New(errs.KindInternal, "agent: backing hook result path is invalid")
	}
	return resultFile, nil
}

func readBackingHookResult(
	ctx context.Context,
	taskRunner runner.Runner,
	containerID string,
	resultFile string,
) ([]byte, error) {
	environment := []backingHookEnvironment{{name: "GP_RESULT_FILE", value: resultFile}}
	result, err := taskRunner.Run(ctx, runner.RunCmdOpts{
		Name:    "docker",
		Args:    backingHookDockerExecArgs(containerID, environment, []string{"/bin/sh", "-c", backingHookReadScript}),
		Env:     backingHookProcessEnvironment(environment),
		Timeout: backingHookHelperTimeout, CaptureLimitBytes: backinghook.MaximumResultBytes,
	})
	defer clear(result.Stderr)
	if err != nil || result.ExitCode != 0 || len(result.Stdout) > backinghook.MaximumResultBytes {
		clear(result.Stdout)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, errs.New(errs.KindInternal, "agent: backing hook result read failed")
	}
	return result.Stdout, nil
}

func cleanupBackingHookResult(
	ctx context.Context,
	taskRunner runner.Runner,
	containerID string,
	resultFile string,
) error {
	if !validBackingHookResultFile(resultFile) {
		return errs.New(errs.KindInternal, "agent: backing hook cleanup path is invalid")
	}
	environment := []backingHookEnvironment{{name: "GP_RESULT_FILE", value: resultFile}}
	result, err := taskRunner.Run(ctx, runner.RunCmdOpts{
		Name: "docker",
		Args: backingHookDockerExecArgs(
			containerID,
			environment,
			[]string{"/bin/sh", "-c", backingHookCleanupScript},
		),
		Env:     backingHookProcessEnvironment(environment),
		Timeout: backingHookHelperTimeout, CaptureLimitBytes: 1024,
	})
	clear(result.Stdout)
	clear(result.Stderr)
	if err != nil || result.ExitCode != 0 {
		return errs.New(errs.KindInternal, "agent: backing hook result cleanup failed")
	}
	return nil
}

func validBackingHookResultFile(value string) bool {
	if !strings.HasPrefix(value, backingHookResultPrefix) || !strings.HasSuffix(value, backingHookResultSuffix) {
		return false
	}
	random := strings.TrimSuffix(strings.TrimPrefix(value, backingHookResultPrefix), backingHookResultSuffix)
	if len(random) != 6 {
		return false
	}
	for index := 0; index < len(random); index++ {
		character := random[index]
		if (character < 'a' || character > 'z') && (character < 'A' || character > 'Z') &&
			(character < '0' || character > '9') {
			return false
		}
	}
	return true
}

func clearBackingHookEnvironment(environment []backingHookEnvironment) {
	for index := range environment {
		environment[index].name = ""
		environment[index].value = ""
	}
}
