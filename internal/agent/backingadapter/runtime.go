package backingadapter

import (
	"context"
	"encoding/json"
	"math"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/adapterprocedure"
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
	if err := adapterprocedure.Validate(procedure); err != nil {
		return StepResult{}, err
	}
	containerID, err := runtime.BackingContainer(ctx, procedure.BackingServiceId)
	if err != nil {
		return StepResult{}, err
	}
	return runtime.ExecuteProcedure(ctx, procedure, containerID)
}

// ExecuteProcedure executes a typed operation against the already selected
// backing container, without rediscovering it between credential operations.
func (runtime *Runtime) ExecuteProcedure(
	ctx context.Context,
	procedure *agentpb.AdapterProcedure,
	containerID string,
) (StepResult, error) {
	if ctx == nil || runtime == nil || runtime.runner == nil || runtime.compiler == nil ||
		!ids.ValidContainerID(containerID) {
		return StepResult{}, errs.New(errs.KindInternal, "adapter procedure runtime is not configured")
	}
	if err := adapterprocedure.Validate(procedure); err != nil {
		return StepResult{}, err
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
	if err := runtime.VerifyBackingContainer(ctx, containerID, procedure.BackingServiceId); err != nil {
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
			Stdin: input, CaptureLimitBytes: 32 * 1024,
		}
		runResult, runErr := runtime.runner.Run(ctx, options)
		clear(runResult.Stdout)
		clear(runResult.Stderr)
		if runResult.ExitCode < 0 || runResult.ExitCode > math.MaxInt32 {
			return result, errs.New(errs.KindInternal, "adapter command returned an invalid exit status")
		}
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
			"--no-trunc",
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

func (runtime *Runtime) VerifyBackingContainer(ctx context.Context, containerID, serviceID string) error {
	result, err := runtime.runner.Run(ctx, runner.RunCmdOpts{Name: "docker",
		Args: []string{"container", "inspect", "--format", "{{json .}}", containerID}, CaptureLimitBytes: 1 << 20})
	defer clear(result.Stdout)
	defer clear(result.Stderr)
	if err != nil || result.ExitCode != 0 {
		return errs.New(errs.KindStateConflict, "captured backing container is unavailable")
	}
	var observed struct {
		ID     string `json:"Id"`
		Config struct {
			Labels map[string]string `json:"Labels"`
		} `json:"Config"`
		State struct {
			Running bool `json:"Running"`
		} `json:"State"`
	}
	if json.Unmarshal(result.Stdout, &observed) != nil || observed.ID != containerID || !observed.State.Running ||
		observed.Config.Labels["com.groundplane.managed"] != "true" || observed.Config.Labels["com.groundplane.service-id"] != serviceID {
		return errs.New(errs.KindStateConflict, "captured backing container ownership changed")
	}
	return nil
}

func adapterCommand(step Step) (string, []string, []byte, error) {
	switch step.Op {
	case "mysql_sql":
		if step.Database != "mysql" || step.Program != "" || len(step.Args) != 0 || len(step.Stdin) == 0 {
			return "", nil, nil, errs.New(errs.KindValidationFailed, "agent: compiled MySQL operation is invalid")
		}
		// This is a closed client invocation, never operator-authored shell.
		// The image's root credential stays in the child environment, not argv.
		return "sh", []string{
			"-ec",
			`MYSQL_PWD="$MYSQL_ROOT_PASSWORD" exec mysql --no-defaults --protocol=socket --user=root --database=mysql --batch --skip-column-names`,
		}, step.Stdin, nil
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
