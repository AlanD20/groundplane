// Package runner is THE way any subprocess is executed in this codebase:
// docker exec, pg_dump, psql, systemctl, staged-binary swaps all flow
// through here, with uniform logging, timeouts, and testability
// (FakeRunner). Never use raw os/exec outside this package, except the
// three documented exceptions: the docker driver and systemd unit
// control inside internal/infra, and the Controller's staged-binary
// swap handoff (validate-before-exec, where the handoff itself can't go
// through a Runner it's in the process of replacing). See
// docs/standards.md, section 5.
package runner

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os/exec"
	"sync"
	"time"

	"github.com/AlanD20/groundplane/internal/common/postgresidentity"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RunCmdOpts is every Run/Stream call's input: one shape, no
// interface{} anywhere in the signature.
type RunCmdOpts struct {
	Name       string
	Args       []string
	Dir        string
	Env        []string // additional vars, or the complete environment when ReplaceEnv is true
	ReplaceEnv bool
	Timeout    time.Duration
	Stdin      []byte
	// CaptureLimitBytes bounds the combined stdout and stderr prefixes retained
	// by Run and Stream. Zero preserves unlimited capture for existing callers.
	CaptureLimitBytes int64
	// StderrRedactions are transient values that are replaced before bounded
	// stderr is returned. They are never included in argv, the environment, or
	// runner logs.
	StderrRedactions []string
}

// Result is every Run/Stream call's output.
type Result struct {
	Stdout   []byte
	Stderr   []byte
	ExitCode int
}

// BinarySink is a backpressured binary destination with an explicit
// cancellation seam. Interrupt must be safe to call while Write is in
// progress, return promptly, and cause that Write to return. The runner calls
// it when the context or another stream fails; it does not claim an arbitrary
// io.Writer can be made cancellation-responsive.
type BinarySink struct {
	Writer    io.Writer
	Interrupt func(cause error) error
}

// Runner is the interface adapters' Step execution (see
// internal/adapters and internal/agent/worker.go) and infra glue code
// depend on: swapping the real OS runner for a FakeRunner in tests
// never ripples past the call site.
type Runner interface {
	// Run executes opts and waits for completion.
	Run(ctx context.Context, opts RunCmdOpts) (Result, error)
	// Stream executes opts, calling onLine for each line of output as it
	// arrives (stderr=true distinguishes the stream), and returns the
	// final Result once the process exits.
	Stream(ctx context.Context, opts RunCmdOpts, onLine func(stderr bool, line string)) (Result, error)
}

// BinaryRunner is the narrow raw-output contract used by typed backup capture
// procedures. It is separate so adding binary capture does not widen Runner or
// break existing Run/Stream-only implementations.
type BinaryRunner interface {
	// StreamPostgresDump executes the one typed PostgreSQL capture procedure
	// while writing stdout directly to sink. Result.Stderr is bounded.
	StreamPostgresDump(ctx context.Context, command PostgresDumpCommand, sink BinarySink) (Result, error)
}

// PostgresDumpCommand is the complete typed input for PostgreSQL backup
// capture. The derived argv fixes pg_dump to the backing container's local
// Unix socket and database superuser. The role and database are distinct even
// though the MVP provisions them with the same generated identity.
type PostgresDumpCommand struct {
	ContainerID string
	Database    string
	Role        string
	Timeout     time.Duration
}

func (command PostgresDumpCommand) runCmdOpts() (RunCmdOpts, error) {
	if !validContainerID(command.ContainerID) ||
		!postgresidentity.ValidGenerated(command.Database) ||
		!postgresidentity.ValidGenerated(command.Role) {
		return RunCmdOpts{}, errs.New(
			errs.KindValidationFailed,
			"runner: postgres dump command identity is invalid",
		)
	}
	return RunCmdOpts{
		Name: "docker",
		Args: []string{
			"container", "exec", "-i", "--user", "postgres", command.ContainerID,
			"pg_dump", "--format=custom", "--compress=0", "--no-owner", "--no-acl",
			"--host=/var/run/postgresql", "--username=postgres", "--no-password",
			"--role=" + command.Role, "--dbname=" + command.Database,
		},
		Timeout: command.Timeout,
	}, nil
}

type binaryCommandPolicy struct {
	validate func(RunCmdOpts) bool
}

var productionBinaryCommands = map[string]binaryCommandPolicy{
	"docker": {validate: validDockerBinaryCommand},
}

// OSRunner is the production Runner: real os/exec, real timeouts.
type OSRunner struct {
	Logger         *slog.Logger
	binaryCommands map[string]binaryCommandPolicy
	signalGroup    func(int) error
}

func New(logger *slog.Logger) *OSRunner {
	return &OSRunner{Logger: logger, binaryCommands: productionBinaryCommands}
}

// NewBinary constructs the runner with the typed binary-output capability.
func NewBinary(logger *slog.Logger) *OSRunner {
	return &OSRunner{Logger: logger, binaryCommands: productionBinaryCommands}
}

func (r *OSRunner) Run(ctx context.Context, opts RunCmdOpts) (Result, error) {
	operationCtx, cancelOperation := withTimeout(ctx, opts.Timeout)
	defer cancelOperation()
	capture, err := newOutputCapture(opts.CaptureLimitBytes)
	if err != nil {
		return Result{ExitCode: -1}, err
	}
	process, err := r.startCapturedProcess(operationCtx, opts, capture, "exec")
	if err != nil {
		return Result{ExitCode: -1}, err
	}

	stdoutResult := make(chan error, 1)
	stderrResult := make(chan error, 1)
	go readCapturedOutput(process.stdout, false, capture, process.requestInterrupt, stdoutResult)
	go readCapturedOutput(process.stderr, true, capture, process.requestInterrupt, stderrResult)
	stdoutErr := <-stdoutResult
	stderrErr := <-stderrResult
	readErr := errors.Join(stdoutErr, stderrErr)
	processErr := process.finish(operationCtx.Err() != nil || capture.exceededLimit() || readErr != nil)
	result := capture.result(process.exitCode())
	return result, capture.resolveError(operationCtx.Err(), errors.Join(readErr, processErr))
}

func (r *OSRunner) Stream(ctx context.Context, opts RunCmdOpts, onLine func(stderr bool, line string)) (Result, error) {
	operationCtx, cancelOperation := withTimeout(ctx, opts.Timeout)
	defer cancelOperation()
	capture, err := newOutputCapture(opts.CaptureLimitBytes)
	if err != nil {
		return Result{ExitCode: -1}, err
	}
	process, err := r.startCapturedProcess(operationCtx, opts, capture, "stream")
	if err != nil {
		return Result{ExitCode: -1}, err
	}

	lines := make(chan streamedLine, 32)
	results := make(chan streamResult, 2)
	var readers sync.WaitGroup
	readers.Add(2)
	go readStream(process.stdout, false, capture, lines, results, process.requestInterrupt, &readers)
	go readStream(process.stderr, true, capture, lines, results, process.requestInterrupt, &readers)
	completion := make(chan error, 1)
	go func() {
		readers.Wait()
		first := <-results
		second := <-results
		readErr := errors.Join(first.err, second.err)
		close(lines)
		processErr := process.finish(operationCtx.Err() != nil || capture.exceededLimit() || readErr != nil)
		completion <- errors.Join(readErr, processErr)
	}()
	for line := range lines {
		onLine(line.stderr, line.value)
	}
	operationErr := <-completion
	result := capture.result(process.exitCode())
	return result, capture.resolveError(operationCtx.Err(), operationErr)
}

func withTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if d <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, d)
}

func commandEnvironment(cmd *exec.Cmd, opts RunCmdOpts) []string {
	if opts.ReplaceEnv {
		return append([]string(nil), opts.Env...)
	}
	return append(cmd.Environ(), opts.Env...)
}

func splitLines(b []byte) []string {
	if len(b) == 0 {
		return nil
	}
	var lines []string
	start := 0
	for i, c := range b {
		if c == '\n' {
			end := i
			if end > start && b[end-1] == '\r' {
				end--
			}
			lines = append(lines, string(b[start:end]))
			start = i + 1
		}
	}
	if start < len(b) {
		lines = append(lines, string(b[start:]))
	}
	return lines
}
