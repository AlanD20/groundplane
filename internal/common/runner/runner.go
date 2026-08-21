// Package runner is THE way any subprocess is executed in this codebase
// — docker exec, pg_dump, psql, systemctl, staged-binary swaps all flow
// through here, with uniform logging, timeouts, and testability
// (FakeRunner). Never use raw os/exec outside this package, except the
// three documented exceptions: the docker driver and systemd unit
// control inside internal/infra, and the Controller's staged-binary
// swap handoff (validate-before-exec, where the handoff itself can't go
// through a Runner it's in the process of replacing). See
// docs/standards.md, section 5.
package runner

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os/exec"
	"sync"
	"time"
)

// RunCmdOpts is every Run/Stream call's input — one shape, no
// interface{} anywhere in the signature.
type RunCmdOpts struct {
	Name       string
	Args       []string
	Dir        string
	Env        []string // additional vars, or the complete environment when ReplaceEnv is true
	ReplaceEnv bool
	Timeout    time.Duration
	Stdin      []byte
}

// Result is every Run/Stream call's output.
type Result struct {
	Stdout   []byte
	Stderr   []byte
	ExitCode int
}

// Runner is the interface adapters' Step execution (see
// internal/adapters and internal/agent/worker.go) and infra glue code
// depend on — swapping the real OS runner for a FakeRunner in tests
// never ripples past the call site.
type Runner interface {
	// Run executes opts and waits for completion.
	Run(ctx context.Context, opts RunCmdOpts) (Result, error)
	// Stream executes opts, calling onLine for each line of output as it
	// arrives (stderr=true distinguishes the stream), and returns the
	// final Result once the process exits.
	Stream(ctx context.Context, opts RunCmdOpts, onLine func(stderr bool, line string)) (Result, error)
}

// OSRunner is the production Runner: real os/exec, real timeouts.
type OSRunner struct {
	Logger *slog.Logger
}

func New(logger *slog.Logger) Runner {
	return &OSRunner{Logger: logger}
}

func (r *OSRunner) Run(ctx context.Context, opts RunCmdOpts) (Result, error) {
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, opts.Name, opts.Args...)
	cmd.Dir = opts.Dir
	cmd.Env = commandEnvironment(cmd, opts)
	if opts.Stdin != nil {
		cmd.Stdin = bytes.NewReader(opts.Stdin)
	}

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if r.Logger != nil {
		r.Logger.Debug("runner: exec", "name", opts.Name, "arg_count", len(opts.Args), "dir", opts.Dir)
	}

	err := cmd.Run()
	exitCode := -1
	if cmd.ProcessState != nil {
		exitCode = cmd.ProcessState.ExitCode()
	}
	res := Result{Stdout: stdout.Bytes(), Stderr: stderr.Bytes(), ExitCode: exitCode}
	if err != nil && ctx.Err() != nil {
		return res, ctx.Err()
	}
	return res, err
}

func (r *OSRunner) Stream(ctx context.Context, opts RunCmdOpts, onLine func(stderr bool, line string)) (Result, error) {
	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, opts.Name, opts.Args...)
	cmd.Dir = opts.Dir
	cmd.Env = commandEnvironment(cmd, opts)
	if opts.Stdin != nil {
		cmd.Stdin = bytes.NewReader(opts.Stdin)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return Result{ExitCode: -1}, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return Result{ExitCode: -1}, err
	}

	if r.Logger != nil {
		r.Logger.Debug("runner: stream", "name", opts.Name, "arg_count", len(opts.Args), "dir", opts.Dir)
	}
	if err := cmd.Start(); err != nil {
		if ctx.Err() != nil {
			return Result{ExitCode: -1}, ctx.Err()
		}
		return Result{ExitCode: -1}, err
	}

	lines := make(chan streamedLine, 32)
	results := make(chan streamResult, 2)
	var readers sync.WaitGroup
	readers.Add(2)
	go readStream(stdout, false, lines, results, &readers)
	go readStream(stderr, true, lines, results, &readers)
	go func() {
		readers.Wait()
		close(lines)
	}()
	for line := range lines {
		onLine(line.stderr, line.value)
	}

	first := <-results
	second := <-results
	var stdoutBytes, stderrBytes []byte
	var readErrors []error
	for _, result := range []streamResult{first, second} {
		if result.stderr {
			stderrBytes = result.output
		} else {
			stdoutBytes = result.output
		}
		if result.err != nil {
			readErrors = append(readErrors, result.err)
		}
	}
	waitErr := cmd.Wait()
	exitCode := -1
	if cmd.ProcessState != nil {
		exitCode = cmd.ProcessState.ExitCode()
	}
	result := Result{Stdout: stdoutBytes, Stderr: stderrBytes, ExitCode: exitCode}
	if ctx.Err() != nil {
		return result, ctx.Err()
	}
	return result, errors.Join(append(readErrors, waitErr)...)
}

type streamedLine struct {
	stderr bool
	value  string
}

type streamResult struct {
	stderr bool
	output []byte
	err    error
}

func readStream(
	reader io.Reader,
	stderr bool,
	lines chan<- streamedLine,
	results chan<- streamResult,
	readers *sync.WaitGroup,
) {
	defer readers.Done()
	var captured bytes.Buffer
	scanner := bufio.NewScanner(io.TeeReader(reader, &captured))
	scanner.Buffer(make([]byte, 4096), 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		line = string(bytes.TrimSuffix([]byte(line), []byte{'\r'}))
		lines <- streamedLine{stderr: stderr, value: line}
	}
	scanErr := scanner.Err()
	if scanErr != nil {
		if _, err := io.Copy(&captured, reader); err != nil {
			scanErr = errors.Join(scanErr, err)
		}
	}
	results <- streamResult{stderr: stderr, output: captured.Bytes(), err: scanErr}
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
