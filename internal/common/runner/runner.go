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
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os/exec"
	"sync"
	"syscall"
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

func New(logger *slog.Logger) Runner {
	return &OSRunner{Logger: logger, binaryCommands: productionBinaryCommands}
}

// NewBinary returns only the raw-output capability. New deliberately retains
// its established Runner return type for source compatibility.
func NewBinary(logger *slog.Logger) BinaryRunner {
	return &OSRunner{Logger: logger, binaryCommands: productionBinaryCommands}
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

const (
	maxBinaryStderrBytes   = 64 * 1024
	maxRedactionItems      = 32
	maxRedactionItemBytes  = 4 * 1024
	maxRedactionTotalBytes = 64 * 1024
	redactionWriteBytes    = 32 * 1024
	stderrTruncatedMark    = "\n[stderr truncated]"
	stderrRedactedMark     = "[REDACTED]"
)

// StreamPostgresDump derives the complete execution context from a closed
// typed command. Restore is intentionally absent until BinaryRunner has a
// bounded cancellable stdin-source contract.
func (r *OSRunner) StreamPostgresDump(
	ctx context.Context,
	command PostgresDumpCommand,
	sink BinarySink,
) (Result, error) {
	opts, err := command.runCmdOpts()
	if err != nil {
		return Result{ExitCode: -1}, err
	}
	return r.streamBinary(ctx, opts, sink)
}

// streamBinary is private so callers cannot mutate executable, argv,
// environment, working directory, stdin, or diagnostic policy.
func (r *OSRunner) streamBinary(ctx context.Context, opts RunCmdOpts, sink BinarySink) (Result, error) {
	if err := validateBinarySink(sink); err != nil {
		return Result{ExitCode: -1}, err
	}
	stderrCapture, err := newRedactedStderr(opts.StderrRedactions)
	if err != nil {
		return Result{ExitCode: -1}, err
	}
	if err := r.validateBinaryCommand(opts); err != nil {
		return Result{ExitCode: -1}, err
	}

	ctx, cancel := withTimeout(ctx, opts.Timeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return Result{ExitCode: -1}, err
	}

	cmd := exec.Command(opts.Name, opts.Args...)
	// Cancellation signals this local process group. The binary allowlist
	// excludes detach modes, but a process that creates a new session or moves
	// itself to another group is outside this guarantee; this is not a cgroup
	// or arbitrary process-tree kill.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Dir = opts.Dir
	cmd.Env = commandEnvironment(cmd, opts)
	if opts.Stdin != nil {
		cmd.Stdin = bytes.NewReader(opts.Stdin)
	}

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return Result{ExitCode: -1}, binaryOperationalError(err)
	}
	stdout := &ownedReadCloser{ReadCloser: stdoutPipe}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return Result{ExitCode: -1}, binaryOperationalError(err, stdout.close())
	}
	stderr := &ownedReadCloser{ReadCloser: stderrPipe}

	if r.Logger != nil {
		r.Logger.Debug("runner: binary stream", "name", opts.Name, "arg_count", len(opts.Args), "dir", opts.Dir)
	}
	if err := cmd.Start(); err != nil {
		stdoutCloseErr := stdout.close()
		stderrCloseErr := stderr.close()
		if ctx.Err() != nil {
			return Result{ExitCode: -1}, ctx.Err()
		}
		return Result{ExitCode: -1}, binaryOperationalError(err, stdoutCloseErr, stderrCloseErr)
	}

	processDone := make(chan struct{})
	interruptRequest := make(chan error, 1)
	interruptResult := make(chan binaryInterruptResult, 1)
	process := binaryProcess{
		pid:         cmd.Process.Pid,
		signalGroup: r.processGroupSignaler(),
		stdout:      stdout,
		stderr:      stderr,
		sink:        sink,
	}
	go func() {
		select {
		case <-ctx.Done():
			interruptResult <- process.interrupt(ctx.Err())
		case cause := <-interruptRequest:
			interruptResult <- process.interrupt(cause)
		case <-processDone:
			interruptResult <- binaryInterruptResult{}
		}
	}()

	stdoutResult := make(chan binaryCopyResult, 1)
	stderrResult := make(chan binaryCopyResult, 1)
	go func() {
		_, copyErr := io.Copy(sink.Writer, stdout)
		if copyErr != nil {
			requestBinaryInterrupt(interruptRequest, copyErr)
		}
		stdoutResult <- binaryCopyResult{copyErr: copyErr, closeErr: stdout.close()}
	}()
	go func() {
		_, copyErr := io.Copy(stderrCapture, stderr)
		if copyErr != nil {
			requestBinaryInterrupt(interruptRequest, copyErr)
		}
		stderrResult <- binaryCopyResult{copyErr: copyErr, closeErr: stderr.close()}
	}()

	stdoutCopy := <-stdoutResult
	stderrCopy := <-stderrResult
	interrupt := binaryInterruptResult{}
	interrupted := false
	if ctx.Err() != nil || stdoutCopy.copyErr != nil || stderrCopy.copyErr != nil {
		// The watcher must publish the completed interruption before Wait is
		// considered. In particular, failed signaling closes the owned pipes,
		// so both copies can finish while sink.Interrupt is still in progress.
		interrupt = <-interruptResult
		interrupted = true
	} else {
		select {
		case interrupt = <-interruptResult:
			interrupted = true
		default:
		}
	}
	if interrupt.signalErr != nil {
		// A kernel refusal leaves no userspace mechanism that can prove this
		// process terminated. Both owned pipes are already closed and the sink
		// is interrupted. Release transfers any remaining process lifecycle to
		// the OS instead of leaking a Wait goroutine or Go process handle.
		releaseErr := cmd.Process.Release()
		process.finish()
		close(processDone)
		result := Result{Stderr: stderrCapture.Bytes(), ExitCode: -1}
		if ctx.Err() != nil {
			return result, ctx.Err()
		}
		return result, binaryOperationalError(
			stdoutCopy.copyErr,
			stderrCopy.copyErr,
			stdoutCopy.closeErr,
			stderrCopy.closeErr,
			interrupt.signalErr,
			interrupt.stdoutCloseErr,
			interrupt.stderrCloseErr,
			interrupt.sinkErr,
			releaseErr,
		)
	}
	waitErr := cmd.Wait()
	process.finish()
	close(processDone)
	if !interrupted {
		interrupt = <-interruptResult
	}

	result := Result{Stderr: stderrCapture.Bytes(), ExitCode: -1}
	if cmd.ProcessState != nil {
		result.ExitCode = cmd.ProcessState.ExitCode()
	}
	if ctx.Err() != nil {
		return result, ctx.Err()
	}
	return result, binaryOperationalError(
		stdoutCopy.copyErr,
		stderrCopy.copyErr,
		waitErr,
		stdoutCopy.closeErr,
		stderrCopy.closeErr,
		interrupt.signalErr,
		interrupt.stdoutCloseErr,
		interrupt.stderrCloseErr,
		interrupt.sinkErr,
	)
}

func validateBinarySink(sink BinarySink) error {
	if sink.Writer == nil || sink.Interrupt == nil {
		return errs.New(errs.KindValidationFailed, "runner: binary sink requires writer and interrupt")
	}
	return nil
}

func (r *OSRunner) validateBinaryCommand(opts RunCmdOpts) error {
	policy, ok := r.binaryCommands[opts.Name]
	if !ok || policy.validate == nil || !policy.validate(opts) {
		return errs.New(errs.KindValidationFailed, "runner: binary command is not allowlisted")
	}
	return nil
}

func validDockerBinaryCommand(opts RunCmdOpts) bool {
	if opts.Dir != "" || len(opts.Env) != 0 || opts.ReplaceEnv || opts.Stdin != nil ||
		len(opts.StderrRedactions) != 0 {
		return false
	}
	args := opts.Args
	if len(args) != 16 || args[0] != "container" || args[1] != "exec" || args[2] != "-i" ||
		args[3] != "--user" || args[4] != "postgres" || args[6] != "pg_dump" {
		return false
	}
	const databasePrefix = "--dbname="
	const rolePrefix = "--role="
	if !validContainerID(args[5]) || args[7] != "--format=custom" || args[8] != "--compress=0" ||
		args[9] != "--no-owner" || args[10] != "--no-acl" ||
		args[11] != "--host=/var/run/postgresql" || args[12] != "--username=postgres" ||
		args[13] != "--no-password" || !bytes.HasPrefix([]byte(args[14]), []byte(rolePrefix)) ||
		!bytes.HasPrefix([]byte(args[15]), []byte(databasePrefix)) {
		return false
	}
	command := PostgresDumpCommand{
		ContainerID: args[5],
		Database:    args[15][len(databasePrefix):],
		Role:        args[14][len(rolePrefix):],
		Timeout:     opts.Timeout,
	}
	canonical, err := command.runCmdOpts()
	return err == nil && equalStrings(args, canonical.Args)
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

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func (r *OSRunner) processGroupSignaler() func(int) error {
	if r.signalGroup != nil {
		return r.signalGroup
	}
	return signalProcessGroup
}

func signalProcessGroup(pid int) error {
	if pid <= 0 {
		return fmt.Errorf("runner: process group pid is invalid")
	}
	groupErr := syscall.Kill(-pid, syscall.SIGKILL)
	if groupErr == nil || errors.Is(groupErr, syscall.ESRCH) {
		return nil
	}
	directErr := syscall.Kill(pid, syscall.SIGKILL)
	if directErr == nil || errors.Is(directErr, syscall.ESRCH) {
		return fmt.Errorf("runner: process group signal failed; direct process signaled: %w", groupErr)
	}
	return errors.Join(
		fmt.Errorf("runner: process group signal failed: %w", groupErr),
		fmt.Errorf("runner: direct process signal failed: %w", directErr),
	)
}

type ownedReadCloser struct {
	io.ReadCloser
	once sync.Once
	err  error
}

func (pipe *ownedReadCloser) close() error {
	pipe.once.Do(func() { pipe.err = pipe.ReadCloser.Close() })
	return pipe.err
}

type binaryCopyResult struct {
	copyErr  error
	closeErr error
}

type binaryInterruptResult struct {
	signalErr      error
	stdoutCloseErr error
	stderrCloseErr error
	sinkErr        error
}

type binaryProcess struct {
	mu          sync.Mutex
	finished    bool
	pid         int
	signalGroup func(int) error
	stdout      *ownedReadCloser
	stderr      *ownedReadCloser
	sink        BinarySink
}

func (process *binaryProcess) interrupt(cause error) binaryInterruptResult {
	process.mu.Lock()
	defer process.mu.Unlock()
	if process.finished {
		return binaryInterruptResult{}
	}
	result := binaryInterruptResult{signalErr: process.signalGroup(process.pid)}
	if result.signalErr != nil {
		result.stdoutCloseErr = process.stdout.close()
		result.stderrCloseErr = process.stderr.close()
	}
	result.sinkErr = process.sink.Interrupt(cause)
	return result
}

func (process *binaryProcess) finish() {
	process.mu.Lock()
	process.finished = true
	process.mu.Unlock()
}

func requestBinaryInterrupt(request chan<- error, cause error) {
	select {
	case request <- cause:
	default:
	}
}

func binaryOperationalError(primary error, subordinate ...error) error {
	causes := make([]error, 0, 1+len(subordinate))
	if primary != nil {
		causes = append(causes, primary)
	}
	for _, cause := range subordinate {
		if cause != nil {
			causes = append(causes, cause)
		}
	}
	if len(causes) == 0 {
		return nil
	}
	return errs.Wrap(errs.KindInternal, errors.Join(causes...))
}

type redactedStderr struct {
	limit     int
	secrets   [][]byte
	maxSecret int
	pending   []byte
	output    []byte
	truncated bool
}

func newRedactedStderr(secrets []string) (*redactedStderr, error) {
	if len(secrets) > maxRedactionItems {
		return nil, errs.New(errs.KindValidationFailed, "runner: too many stderr redactions")
	}
	capture := &redactedStderr{
		limit:  maxBinaryStderrBytes,
		output: make([]byte, 0, maxBinaryStderrBytes),
	}
	total := 0
	for _, secret := range secrets {
		if len(secret) > maxRedactionItemBytes {
			return nil, errs.New(errs.KindValidationFailed, "runner: stderr redaction is too large")
		}
		total += len(secret)
		if total > maxRedactionTotalBytes {
			return nil, errs.New(errs.KindValidationFailed, "runner: stderr redactions are too large")
		}
		if secret == "" {
			continue
		}
		capture.secrets = append(capture.secrets, []byte(secret))
		if len(secret) > capture.maxSecret {
			capture.maxSecret = len(secret)
		}
	}
	return capture, nil
}

func (r *redactedStderr) Write(p []byte) (int, error) {
	written := len(p)
	for len(p) > 0 && !r.truncated {
		chunkSize := min(len(p), redactionWriteBytes)
		r.pending = append(r.pending, p[:chunkSize]...)
		p = p[chunkSize:]
		r.flushSafePrefix()
	}
	if r.truncated {
		r.pending = nil
	}
	return written, nil
}

func (r *redactedStderr) flushSafePrefix() {
	flushLen := len(r.pending)
	if r.maxSecret > 0 {
		flushLen -= r.maxSecret
		if flushLen < 0 {
			return
		}
		for {
			adjusted := false
			for _, secret := range r.secrets {
				for start := 0; start+len(secret) <= len(r.pending); {
					index := bytes.Index(r.pending[start:], secret)
					if index < 0 {
						break
					}
					index += start
					if index < flushLen && index+len(secret) > flushLen {
						flushLen = index + len(secret)
						adjusted = true
					}
					start = index + 1
				}
			}
			if !adjusted {
				break
			}
		}
	}
	r.flush(flushLen)
}

func (r *redactedStderr) flush(length int) {
	if length <= 0 {
		return
	}
	r.appendRedacted(r.pending[:length])
	r.pending = append(r.pending[:0], r.pending[length:]...)
}

func (r *redactedStderr) appendRedacted(chunk []byte) {
	for len(chunk) > 0 && !r.truncated {
		matchAt := len(chunk)
		matchLength := 0
		for _, secret := range r.secrets {
			if index := bytes.Index(chunk, secret); index >= 0 &&
				(index < matchAt || index == matchAt && len(secret) > matchLength) {
				matchAt = index
				matchLength = len(secret)
			}
		}
		if matchLength == 0 {
			r.appendOutput(chunk)
			return
		}
		r.appendOutput(chunk[:matchAt])
		r.appendOutput([]byte(stderrRedactedMark))
		chunk = chunk[matchAt+matchLength:]
	}
}

func (r *redactedStderr) appendOutput(p []byte) {
	if r.truncated || len(p) == 0 {
		return
	}
	remaining := r.limit - len(r.output)
	if len(p) > remaining {
		r.output = append(r.output, p[:remaining]...)
		r.truncated = true
		return
	}
	r.output = append(r.output, p...)
}

func (r *redactedStderr) Bytes() []byte {
	if !r.truncated {
		r.flush(len(r.pending))
	}
	if !r.truncated {
		return append([]byte(nil), r.output...)
	}
	mark := []byte(stderrTruncatedMark)
	if len(mark) >= r.limit {
		return append([]byte(nil), mark[:r.limit]...)
	}
	keep := min(r.limit-len(mark), len(r.output))
	result := append([]byte(nil), r.output[:keep]...)
	return append(result, mark...)
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
