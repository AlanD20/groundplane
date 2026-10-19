package backinghookruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/infra/docker/backingcommand"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ExecuteDetach keeps setup, hook execution and result-file cleanup inside the
// same recorded Docker exec. Losing the Agent connection does not remove a file
// still used by the hook, nor grant another invocation of the operator command.
func ExecuteDetach(ctx context.Context, executor *backingcommand.Executor, recorder backingcommand.Recorder,
	containerID string, definition backinghook.Definition, input backinghook.Input,
) error {
	if input.Context.Event != backinghook.Detach || executor == nil {
		return errs.New(errs.KindValidationFailed, "captured Detach requires its Detach hook and executor")
	}
	return executeNoFacts(ctx, executor, recorder, containerID, definition, input)
}

// ExecuteLifecycle uses the same recorded input gate as Detach. Neither event
// may publish facts, and loss of the exec result never permits automatic replay.
func ExecuteLifecycle(ctx context.Context, executor *backingcommand.Executor, recorder backingcommand.Recorder,
	containerID string, definition backinghook.Definition, input backinghook.Input,
) error {
	if executor == nil ||
		(input.Context.Event != backinghook.BeforeStop && input.Context.Event != backinghook.AfterStart) {
		return errs.New(
			errs.KindValidationFailed,
			"Service lifecycle requires its captured BeforeStop or AfterStart hook",
		)
	}
	return executeNoFacts(ctx, executor, recorder, containerID, definition, input)
}

func executeNoFacts(ctx context.Context, executor *backingcommand.Executor, recorder backingcommand.Recorder,
	containerID string, definition backinghook.Definition, input backinghook.Input,
) error {
	if err := backinghook.Validate(definition, input, nil); err != nil {
		return err
	}
	duration, err := boundedBackingHookTimeout(ctx, time.Duration(definition.TimeoutSeconds)*time.Second)
	if err != nil {
		return err
	}
	environment := backingHookEnvironmentFor(input)
	defer clearBackingHookEnvironment(environment)
	variables := backingHookProcessEnvironment(environment)
	defer clear(variables)
	command := append([]string{"/bin/sh", "-c", capturedHookScript, "groundplane-hook",
		backingHookDurationArgument(
			backingHookKillAfter,
		), backingHookDurationArgument(duration)}, definition.Command...)
	result := &capturedHookResult{}
	defer func() { clear(result.content) }()
	exitCode, err := executor.Execute(ctx, recorder, containerID, command, variables, nil, result)
	if err != nil {
		return err
	}
	if exitCode != 0 {
		return errs.New(errs.KindStateConflict, "Custom backing hook failed")
	}
	output, err := backinghook.ParseResult(nil, result.content)
	output.Clear()
	return err
}

// The temporary directory belongs to this exec inside the backing container.
// A trap removes only that exact directory when the wrapper exits. The result
// remains private and uses the same no-facts Detach contract as other callers.
const capturedHookScript = `set -eu
umask 077
kill_after=$1
duration=$2
shift 2
timeout -k 1s 1s /bin/sh -c ':'
directory=$(mktemp -d /tmp/groundplane-hook.XXXXXX)
trap 'rm -rf "$directory"' 0
chmod 700 "$directory"
GP_RESULT_FILE="$directory/result"
export GP_RESULT_FILE
: > "$GP_RESULT_FILE"
chmod 600 "$GP_RESULT_FILE"
timeout -k "$kill_after" "$duration" "$@" >/dev/null 2>&1
[ -f "$GP_RESULT_FILE" ] && [ ! -L "$GP_RESULT_FILE" ]
cat "$GP_RESULT_FILE"`

type capturedHookResult struct{ content []byte }

func (result *capturedHookResult) Write(value []byte) (int, error) {
	if len(value) > backinghook.MaximumResultBytes-len(result.content) {
		return 0, errs.New(errs.KindValidationFailed, "Custom hook result exceeds its size limit")
	}
	result.content = append(result.content, value...)
	return len(value), nil
}
