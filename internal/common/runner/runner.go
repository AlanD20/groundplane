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
	"bytes"
	"context"
	"log/slog"
	"os/exec"
	"time"
)

// RunCmdOpts is every Run/Stream call's input — one shape, no
// interface{} anywhere in the signature.
type RunCmdOpts struct {
	Name    string
	Args    []string
	Dir     string
	Env     []string // additional vars, appended to the process environment
	Timeout time.Duration
	Stdin   []byte
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
	cmd.Env = append(cmd.Environ(), opts.Env...)
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

// Stream is a minimal implementation for this scaffold: it runs to
// completion via Run and replays the captured output through onLine
// line-by-line, rather than streaming live. TODO: wire cmd.StdoutPipe /
// cmd.StderrPipe + bufio.Scanner for true live streaming (needed once
// `service logs --follow` and long-running steps want incremental
// TaskEvent output — see proto/agent.proto's TaskEvent.chunk).
func (r *OSRunner) Stream(ctx context.Context, opts RunCmdOpts, onLine func(stderr bool, line string)) (Result, error) {
	res, err := r.Run(ctx, opts)
	for _, line := range splitLines(res.Stdout) {
		onLine(false, line)
	}
	for _, line := range splitLines(res.Stderr) {
		onLine(true, line)
	}
	return res, err
}

func withTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if d <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, d)
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
