package backinghookruntime

import (
	"context"
	"errors"
	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

// Execute executes one sealed custom-backing hook inside its
// already-selected backing container. Resolved values cross Docker only by
// environment, never by command text or argv.
func Execute(
	ctx context.Context,
	taskRunner runner.Runner,
	containerID string,
	definition backinghook.Definition,
	input backinghook.Input,
	schema []backinghook.FactDefinition,
) (output backinghook.Output, resultErr error) {
	if ctx == nil || taskRunner == nil || !ids.ValidContainerID(containerID) {
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
