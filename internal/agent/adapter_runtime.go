package agent

import (
	"context"
	"strings"

	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const maximumCompiledAdapterSteps = 16

type AdapterRuntime struct {
	runner runner.Runner
}

type adapterStepResult struct {
	ExitCode int32
}

func NewAdapterRuntime(taskRunner runner.Runner) *AdapterRuntime {
	return &AdapterRuntime{runner: taskRunner}
}

func (runtime *AdapterRuntime) executeStep(
	ctx context.Context,
	step *agentpb.ExecutionStep,
) (adapterStepResult, error) {
	if runtime == nil || runtime.runner == nil {
		return adapterStepResult{}, errs.New(errs.KindInternal, "agent: adapter runtime is not configured")
	}
	procedure := step.GetAdapterProcedure()
	if procedure == nil {
		return adapterStepResult{}, errs.New(errs.KindInternal, "agent: adapter procedure is required")
	}
	defer func() {
		clear(procedure.Password)
		procedure.Password = nil
	}()
	adapter, found := adapters.Get(procedure.AdapterKey)
	if !found || adapter.Manual() {
		return adapterStepResult{}, errs.New(errs.KindValidationFailed, "agent: adapter procedure is not registered")
	}
	params := adapters.ProvisionParams{
		Database: procedure.Database,
		Role:     procedure.Role,
		Password: append([]byte(nil), procedure.Password...),
		GrantOn:  procedure.GrantOn,
	}
	defer clear(params.Password)
	compiled := compileAdapterProcedure(adapter, procedure.Phase, params)
	defer adapters.ClearSteps(compiled)
	if len(compiled) == 0 || len(compiled) > maximumCompiledAdapterSteps {
		return adapterStepResult{}, errs.New(
			errs.KindValidationFailed,
			"agent: adapter procedure is empty or oversized",
		)
	}
	containerID, err := runtime.backingContainer(ctx, procedure.BackingServiceId)
	if err != nil {
		return adapterStepResult{}, err
	}
	result := adapterStepResult{}
	for _, operation := range compiled {
		command, args, input, err := adapterCommand(operation)
		if err != nil {
			return result, err
		}
		options := runner.RunCmdOpts{
			Name: "docker", Args: append([]string{"container", "exec", "-i", containerID, command}, args...),
			Stdin: input,
		}
		runResult, runErr := runtime.runner.Run(ctx, options)
		if runResult.ExitCode != 0 {
			result.ExitCode = int32(runResult.ExitCode)
		}
		if runErr != nil || runResult.ExitCode != 0 {
			return result, errs.New(errs.KindInternal, "agent: compiled adapter operation failed")
		}
	}
	return result, nil
}

func compileAdapterProcedure(
	adapter adapters.Adapter,
	phase agentpb.AdapterProcedurePhase,
	params adapters.ProvisionParams,
) []adapters.Step {
	switch phase {
	case agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_PROVISION:
		return adapter.ProvisionSteps(params)
	case agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_GRANT:
		return adapter.GrantSteps(params)
	case agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_REVOKE:
		return adapter.RevokeSteps(params)
	case agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_DETACH:
		return adapter.DetachSteps(params)
	default:
		return nil
	}
}

func (runtime *AdapterRuntime) backingContainer(ctx context.Context, runtimeServiceID string) (string, error) {
	if ids.Validate(ids.KindService, runtimeServiceID) != nil &&
		ids.Validate(ids.KindComponent, runtimeServiceID) != nil {
		return "", errs.New(errs.KindValidationFailed, "agent: adapter runtime service id is invalid")
	}
	result, err := runtime.runner.Run(ctx, runner.RunCmdOpts{
		Name: "docker",
		Args: []string{
			"container", "ls",
			"--filter", "label=com.groundplane.managed=true",
			"--filter", "label=com.groundplane.service-id=" + runtimeServiceID,
			"--filter", "status=running",
			"--format", "{{.ID}}",
		},
	})
	if err != nil || result.ExitCode != 0 {
		return "", errs.New(errs.KindInternal, "agent: backing container lookup failed")
	}
	containers := strings.Fields(string(result.Stdout))
	if len(containers) != 1 || !validContainerID(containers[0]) {
		return "", errs.New(errs.KindStateConflict, "agent: backing runtime service is not uniquely running")
	}
	return containers[0], nil
}

func adapterCommand(step adapters.Step) (string, []string, []byte, error) {
	if len(step.Stdin) == 0 {
		return "", nil, nil, errs.New(errs.KindValidationFailed, "agent: compiled adapter input is empty")
	}
	switch step.Op {
	case adapters.StepSQL:
		if step.Database == "" || step.Program != "" || len(step.Args) != 0 {
			return "", nil, nil, errs.New(errs.KindValidationFailed, "agent: compiled SQL operation is invalid")
		}
		return "psql", []string{
			"--no-psqlrc", "--username", "postgres", "--dbname", step.Database,
			"--set", "ON_ERROR_STOP=1",
		}, step.Stdin, nil
	case adapters.StepExec:
		if step.Database != "" || !validCompiledProgram(step.Program) {
			return "", nil, nil, errs.New(errs.KindValidationFailed, "agent: compiled exec operation is invalid")
		}
		for _, argument := range step.Args {
			if argument == "" || strings.IndexByte(argument, 0) >= 0 {
				return "", nil, nil, errs.New(errs.KindValidationFailed, "agent: compiled exec argument is invalid")
			}
		}
		return step.Program, append([]string(nil), step.Args...), step.Stdin, nil
	default:
		return "", nil, nil, errs.New(errs.KindValidationFailed, "agent: compiled adapter operation is unsupported")
	}
}

func validCompiledProgram(value string) bool {
	if value == "" || len(value) > 64 {
		return false
	}
	for _, character := range []byte(value) {
		if (character >= 'a' && character <= 'z') || (character >= '0' && character <= '9') ||
			character == '-' || character == '_' || character == '.' {
			continue
		}
		return false
	}
	return true
}

func validContainerID(value string) bool {
	if len(value) < 12 || len(value) > 64 {
		return false
	}
	for _, character := range []byte(value) {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}
