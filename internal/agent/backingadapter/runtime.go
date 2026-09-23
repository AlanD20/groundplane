package backingadapter

import (
	"context"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const maximumCompiledAdapterSteps = 16

type Runtime struct {
	runner   runner.Runner
	compiler Compiler
}

// Step is one operation compiled by the application's trusted, closed adapter
// catalog. The Agent validates each operation again before executing it.
type Step struct {
	Op       string
	Database string
	Program  string
	Args     []string
	Stdin    []byte
}

type Compiler func(*agentpb.AdapterProcedure) ([]Step, error)

type StepResult struct {
	ExitCode int32
}

func New(taskRunner runner.Runner, compiler Compiler) *Runtime {
	return &Runtime{runner: taskRunner, compiler: compiler}
}

func (runtime *Runtime) ExecuteStep(
	ctx context.Context,
	step *agentpb.ExecutionStep,
) (StepResult, error) {
	if runtime == nil || runtime.runner == nil || runtime.compiler == nil {
		return StepResult{}, errs.New(errs.KindInternal, "agent: adapter runtime is not configured")
	}
	procedure := step.GetAdapterProcedure()
	if procedure == nil {
		return StepResult{}, errs.New(errs.KindInternal, "agent: adapter procedure is required")
	}
	compiled, err := runtime.compiler(procedure)
	defer clearSteps(compiled)
	if err != nil {
		return StepResult{}, err
	}
	if len(compiled) == 0 || len(compiled) > maximumCompiledAdapterSteps {
		return StepResult{}, errs.New(
			errs.KindValidationFailed,
			"agent: adapter procedure is empty or oversized",
		)
	}
	containerID, err := runtime.BackingContainer(ctx, procedure.BackingServiceId)
	if err != nil {
		return StepResult{}, err
	}
	result := StepResult{}
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

func (runtime *Runtime) BackingContainer(ctx context.Context, runtimeServiceID string) (string, error) {
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
	if len(containers) != 1 || !ids.ValidContainerID(containers[0]) {
		return "", errs.New(errs.KindStateConflict, "agent: backing runtime service is not uniquely running")
	}
	return containers[0], nil
}

func adapterCommand(step Step) (string, []string, []byte, error) {
	switch step.Op {
	case "sql":
		if step.Database == "" || step.Program != "" || len(step.Args) != 0 || len(step.Stdin) == 0 {
			return "", nil, nil, errs.New(errs.KindValidationFailed, "agent: compiled SQL operation is invalid")
		}
		return "psql", []string{
			"--no-psqlrc", "--username", "postgres", "--dbname", step.Database,
			"--set", "ON_ERROR_STOP=1",
		}, step.Stdin, nil
	case "exec":
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

func clearSteps(steps []Step) {
	for index := range steps {
		clear(steps[index].Stdin)
		steps[index].Stdin = nil
		steps[index].Args = nil
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
