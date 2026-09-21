package runner

import (
	"bytes"
	"context"
	"io"
	"os/exec"
	"syscall"
)

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
