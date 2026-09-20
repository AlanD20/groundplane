package backinghookruntime

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"strings"
	"time"
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
