package runner

import (
	"bytes"
	"context"
	"errors"
	"os/exec"
	"sync"
	"syscall"
)

type capturedProcess struct {
	mu sync.Mutex

	cmd              *exec.Cmd
	stdout           *ownedReadCloser
	stderr           *ownedReadCloser
	signalGroup      func(int) error
	finished         bool
	processDone      chan struct{}
	interruptRequest chan error
	interruptResult  chan commandInterruptResult
}

type commandInterruptResult struct {
	signalErr      error
	stdoutCloseErr error
	stderrCloseErr error
}

func (r *OSRunner) startCapturedProcess(
	ctx context.Context,
	opts RunCmdOpts,
	capture *outputCapture,
	logOperation string,
) (*capturedProcess, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cmd := exec.Command(opts.Name, opts.Args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Dir = opts.Dir
	cmd.Env = commandEnvironment(cmd, opts)
	if opts.Stdin != nil {
		cmd.Stdin = bytes.NewReader(opts.Stdin)
	}
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return nil, subprocessOperationalError(err)
	}
	stdout := &ownedReadCloser{ReadCloser: stdoutPipe}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return nil, subprocessOperationalError(errors.Join(err, stdout.close()))
	}
	stderr := &ownedReadCloser{ReadCloser: stderrPipe}
	if r.Logger != nil {
		r.Logger.Debug(
			"runner: "+logOperation,
			"name", opts.Name,
			"arg_count", len(opts.Args),
			"dir", opts.Dir,
		)
	}
	if err := cmd.Start(); err != nil {
		closeErr := errors.Join(stdout.close(), stderr.close())
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, subprocessOperationalError(errors.Join(err, closeErr))
	}
	process := &capturedProcess{
		cmd:              cmd,
		stdout:           stdout,
		stderr:           stderr,
		signalGroup:      r.processGroupSignaler(),
		processDone:      make(chan struct{}),
		interruptRequest: make(chan error, 1),
		interruptResult:  make(chan commandInterruptResult, 1),
	}
	go process.watch(ctx, capture.exceededSignal)
	return process, nil
}

func (process *capturedProcess) watch(ctx context.Context, exceeded <-chan struct{}) {
	select {
	case <-ctx.Done():
		process.interruptResult <- process.interrupt()
	case <-exceeded:
		process.interruptResult <- process.interrupt()
	case <-process.interruptRequest:
		process.interruptResult <- process.interrupt()
	case <-process.processDone:
		process.interruptResult <- commandInterruptResult{}
	}
}

func (process *capturedProcess) requestInterrupt(cause error) {
	select {
	case process.interruptRequest <- cause:
	default:
	}
}

func (process *capturedProcess) interrupt() commandInterruptResult {
	process.mu.Lock()
	defer process.mu.Unlock()
	if process.finished {
		return commandInterruptResult{}
	}
	result := commandInterruptResult{signalErr: process.signalGroup(process.cmd.Process.Pid)}
	if result.signalErr != nil {
		result.stdoutCloseErr = process.stdout.close()
		result.stderrCloseErr = process.stderr.close()
	}
	return result
}

func (process *capturedProcess) finish(interruptionExpected bool) error {
	interrupt := commandInterruptResult{}
	interrupted := false
	if interruptionExpected {
		interrupt = <-process.interruptResult
		interrupted = true
	} else {
		select {
		case interrupt = <-process.interruptResult:
			interrupted = true
		default:
		}
	}
	if interrupt.signalErr != nil {
		releaseErr := process.cmd.Process.Release()
		process.markFinished()
		close(process.processDone)
		return errors.Join(
			interrupt.signalErr,
			interrupt.stdoutCloseErr,
			interrupt.stderrCloseErr,
			releaseErr,
		)
	}
	waitErr := process.cmd.Wait()
	process.markFinished()
	close(process.processDone)
	if !interrupted {
		interrupt = <-process.interruptResult
	}
	return errors.Join(
		waitErr,
		interrupt.signalErr,
		interrupt.stdoutCloseErr,
		interrupt.stderrCloseErr,
	)
}

func (process *capturedProcess) markFinished() {
	process.mu.Lock()
	process.finished = true
	process.mu.Unlock()
}

func (process *capturedProcess) exitCode() int {
	if process.cmd.ProcessState == nil {
		return -1
	}
	return process.cmd.ProcessState.ExitCode()
}
