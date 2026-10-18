package backinghookruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/infra/docker/backingcommand"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ExecuteAttach uses one durably identified exec for setup, the operator hook,
// and result cleanup. Facts are returned privately to the execution owner;
// command stdout/stderr never becomes Task diagnostics.
func ExecuteAttach(ctx context.Context, executor *backingcommand.Executor, recorder backingcommand.Recorder,
	containerID string, definition backinghook.Definition, input backinghook.Input, schema []backinghook.FactDefinition,
) (backinghook.Output, error) {
	if input.Context.Event != backinghook.Attach || executor == nil {
		return backinghook.Output{}, errs.New(errs.KindValidationFailed, "captured Attach requires its Attach hook and executor")
	}
	if err := backinghook.Validate(definition, input, schema); err != nil {
		return backinghook.Output{}, err
	}
	duration, err := boundedBackingHookTimeout(ctx, time.Duration(definition.TimeoutSeconds)*time.Second)
	if err != nil {
		return backinghook.Output{}, err
	}
	environment := backingHookEnvironmentFor(input)
	defer clearBackingHookEnvironment(environment)
	variables := backingHookProcessEnvironment(environment)
	defer clear(variables)
	command := append([]string{"/bin/sh", "-c", capturedHookScript, "groundplane-attach",
		backingHookDurationArgument(backingHookKillAfter), backingHookDurationArgument(duration)}, definition.Command...)
	result := &capturedHookResult{}
	defer func() { clear(result.content) }()
	exitCode, err := executor.Execute(ctx, recorder, containerID, command, variables, nil, result)
	if err != nil {
		return backinghook.Output{}, err
	}
	if exitCode != 0 {
		return backinghook.Output{}, errs.New(errs.KindStateConflict, "Custom Attach hook failed")
	}
	return backinghook.ParseResult(schema, result.content)
}
