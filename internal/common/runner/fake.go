package runner

import (
	"context"
	"io"
	"sync"
)

const fakeBinaryChunkBytes = 32 * 1024

// FakeRunner is the L1 hermetic-test double: no real subprocess ever runs.
// Configure its exported fields before concurrent use; invocation and call
// recording are race-safe. StreamPostgresDumpFunc, when set, explicitly owns
// incremental writes and its Result/error are returned unchanged after common
// typed-command and timeout validation. The default binary path mirrors
// OSRunner's cancellation, copy-failure interruption, and error precedence.
type FakeRunner struct {
	mu sync.Mutex

	Calls                  []RunCmdOpts
	Results                map[string]Result
	Errors                 map[string]error
	RunFunc                func(ctx context.Context, opts RunCmdOpts) (Result, error)
	StreamPostgresDumpFunc func(context.Context, PostgresDumpCommand, BinarySink) (Result, error)
}

func NewFake() *FakeRunner {
	return &FakeRunner{Results: map[string]Result{}, Errors: map[string]error{}}
}

func (f *FakeRunner) Run(ctx context.Context, opts RunCmdOpts) (Result, error) {
	res, runErr, runFunc, _ := f.record(opts)
	if runFunc != nil {
		return runFunc(ctx, opts)
	}
	return res, runErr
}

func (f *FakeRunner) Stream(
	ctx context.Context,
	opts RunCmdOpts,
	onLine func(stderr bool, line string),
) (Result, error) {
	res, err := f.Run(ctx, opts)
	for _, line := range splitLines(res.Stdout) {
		onLine(false, line)
	}
	for _, line := range splitLines(res.Stderr) {
		onLine(true, line)
	}
	return res, err
}

func (f *FakeRunner) StreamPostgresDump(
	ctx context.Context,
	command PostgresDumpCommand,
	sink BinarySink,
) (Result, error) {
	opts, err := command.runCmdOpts()
	if err != nil {
		return Result{ExitCode: -1}, err
	}
	if err := validateBinarySink(sink); err != nil {
		return Result{ExitCode: -1}, err
	}
	capture, err := newRedactedStderr(nil)
	if err != nil {
		return Result{ExitCode: -1}, err
	}
	ctx, cancel := withTimeout(ctx, command.Timeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return Result{ExitCode: -1}, err
	}
	res, runErr, _, streamPostgresDumpFunc := f.record(opts)
	if streamPostgresDumpFunc != nil {
		return streamPostgresDumpFunc(ctx, command, sink)
	}
	if _, err := capture.Write(res.Stderr); err != nil {
		return Result{ExitCode: -1}, binaryOperationalError(err)
	}
	result := Result{Stderr: capture.Bytes(), ExitCode: res.ExitCode}

	done := make(chan struct{})
	lifecycle := fakeBinaryLifecycle{sink: sink}
	contextInterrupt := make(chan error, 1)
	go func() {
		select {
		case <-ctx.Done():
			contextInterrupt <- lifecycle.interrupt(ctx.Err())
		case <-done:
			contextInterrupt <- nil
		}
	}()

	var writeErr error
	for remaining := res.Stdout; len(remaining) > 0; {
		chunkSize := min(len(remaining), fakeBinaryChunkBytes)
		written, err := sink.Writer.Write(remaining[:chunkSize])
		if err == nil && written != chunkSize {
			err = io.ErrShortWrite
		}
		if err != nil {
			writeErr = err
			break
		}
		if ctx.Err() != nil {
			writeErr = ctx.Err()
			break
		}
		remaining = remaining[chunkSize:]
	}
	var writeInterruptErr error
	if writeErr != nil && ctx.Err() == nil {
		writeInterruptErr = lifecycle.interrupt(writeErr)
	}
	lifecycle.finish()
	close(done)
	contextInterruptErr := <-contextInterrupt
	if ctx.Err() != nil {
		return result, ctx.Err()
	}
	return result, binaryOperationalError(writeErr, runErr, writeInterruptErr, contextInterruptErr)
}

type fakeBinaryLifecycle struct {
	mu          sync.Mutex
	finished    bool
	interrupted bool
	sink        BinarySink
}

func (lifecycle *fakeBinaryLifecycle) interrupt(cause error) error {
	lifecycle.mu.Lock()
	defer lifecycle.mu.Unlock()
	if lifecycle.finished || lifecycle.interrupted {
		return nil
	}
	lifecycle.interrupted = true
	return lifecycle.sink.Interrupt(cause)
}

func (lifecycle *fakeBinaryLifecycle) finish() {
	lifecycle.mu.Lock()
	lifecycle.finished = true
	lifecycle.mu.Unlock()
}

// RecordedCalls returns a deep snapshot suitable for assertions after or
// during concurrent invocation.
func (f *FakeRunner) RecordedCalls() []RunCmdOpts {
	f.mu.Lock()
	defer f.mu.Unlock()
	calls := make([]RunCmdOpts, len(f.Calls))
	for index, call := range f.Calls {
		calls[index] = cloneOpts(call)
	}
	return calls
}

func (f *FakeRunner) record(
	opts RunCmdOpts,
) (
	Result,
	error,
	func(context.Context, RunCmdOpts) (Result, error),
	func(context.Context, PostgresDumpCommand, BinarySink) (Result, error),
) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Calls = append(f.Calls, cloneOpts(opts))
	res := cloneResult(f.Results[opts.Name])
	return res, f.Errors[opts.Name], f.RunFunc, f.StreamPostgresDumpFunc
}

func cloneOpts(opts RunCmdOpts) RunCmdOpts {
	opts.Args = append([]string(nil), opts.Args...)
	opts.Env = append([]string(nil), opts.Env...)
	opts.Stdin = append([]byte(nil), opts.Stdin...)
	opts.StderrRedactions = append([]string(nil), opts.StderrRedactions...)
	return opts
}

func cloneResult(result Result) Result {
	result.Stdout = append([]byte(nil), result.Stdout...)
	result.Stderr = append([]byte(nil), result.Stderr...)
	return result
}
