package runner

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const binaryHelperModeEnv = "GROUNDPLANE_RUNNER_BINARY_HELPER"

// Rationale: subprocess fixtures must emit only their requested binary stream
// and must fail deterministically instead of silently accepting partial writes.
func TestRunnerBinaryHelper(t *testing.T) {
	mode := os.Getenv(binaryHelperModeEnv)
	if mode == "" {
		return
	}
	switch mode {
	case "raw":
		if err := writeFull(os.Stdout, []byte{'a', 0, 'b', 1, 0xff}); err != nil {
			os.Exit(93)
		}
	case "blocked":
		if err := writeFull(os.Stdout, bytes.Repeat([]byte("x"), 1024*1024)); err != nil {
			os.Exit(93)
		}
	case "simultaneous":
		start := make(chan struct{})
		var writers sync.WaitGroup
		writers.Add(2)
		go helperWrite(&writers, start, os.Stdout, 'o')
		go helperWrite(&writers, start, os.Stderr, 'e')
		close(start)
		writers.Wait()
	case "group-parent":
		child := exec.Command(os.Args[0], "-test.run=TestRunnerBinaryHelper")
		child.Env = []string{binaryHelperModeEnv + "=group-child"}
		child.Stdout = os.Stdout
		child.Stderr = os.Stderr
		if err := child.Start(); err != nil {
			os.Exit(91)
		}
		if err := child.Wait(); err != nil {
			os.Exit(94)
		}
	case "group-child":
		if err := writeFull(os.Stdout, []byte(fmt.Sprintf("child-ready:%d\n", os.Getpid()))); err != nil {
			os.Exit(93)
		}
		for {
			if err := syscall.Pause(); err != nil && !errors.Is(err, syscall.EINTR) {
				os.Exit(95)
			}
		}
	case "signal-failure-linger":
		if err := writeFull(os.Stdout, []byte("linger")); err != nil {
			os.Exit(93)
		}
		for {
			if err := syscall.Pause(); err != nil && !errors.Is(err, syscall.EINTR) {
				os.Exit(95)
			}
		}
	default:
		os.Exit(92)
	}
	os.Exit(0)
}

func helperWrite(writers *sync.WaitGroup, start <-chan struct{}, file *os.File, value byte) {
	defer writers.Done()
	<-start
	chunk := bytes.Repeat([]byte{value}, 32*1024)
	for range 64 {
		if err := writeFull(file, chunk); err != nil {
			os.Exit(93)
		}
	}
}

func writeFull(writer io.Writer, payload []byte) error {
	for len(payload) > 0 {
		written, err := writer.Write(payload)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		payload = payload[written:]
	}
	return nil
}

// Rationale: backup archives contain arbitrary bytes and must reach a
// backpressured sink incrementally without accumulating in Result.Stdout.
func TestOSRunnerStreamBinaryPreservesRawBytesIncrementally(t *testing.T) {
	runner, opts := binaryHelperRunner(t, "raw")
	sink := newGateSink()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	t.Cleanup(sink.releaseWrites)
	resultCh := make(chan binaryResult, 1)
	go func() {
		result, err := runner.streamBinary(ctx, opts, sink.binarySink())
		resultCh <- binaryResult{result: result, err: err}
	}()

	awaitValue(t, sink.entered, "incremental sink entry")
	select {
	case result := <-resultCh:
		t.Fatalf("StreamBinary() returned before the incremental sink accepted data: %v", result.err)
	default:
	}
	sink.releaseWrites()
	result := awaitValue(t, resultCh, "incremental stream result")
	if result.err != nil {
		t.Fatalf("StreamBinary() error = %v", result.err)
	}
	want := []byte{'a', 0, 'b', 1, 0xff}
	if got := sink.bytes(); !bytes.Equal(got, want) {
		t.Fatalf("stdout = %v, want %v", got, want)
	}
	if len(result.result.Stdout) != 0 {
		t.Fatalf("Result.Stdout length = %d, want 0", len(result.result.Stdout))
	}
}

// Rationale: concurrent large stderr must never deadlock stdout capture, and
// diagnostics must remain bounded while every stdout byte reaches the sink.
func TestOSRunnerStreamBinaryDrainsLargeSimultaneousOutput(t *testing.T) {
	runner, opts := binaryHelperRunner(t, "simultaneous")
	sink := &memorySink{}
	result, err := runner.streamBinary(context.Background(), opts, sink.binarySink())
	if err != nil {
		t.Fatalf("StreamBinary() error = %v", err)
	}
	if got, want := len(sink.bytes()), 2*1024*1024; got != want {
		t.Fatalf("stdout length = %d, want %d", got, want)
	}
	if len(result.Stderr) != maxBinaryStderrBytes ||
		!bytes.HasSuffix(result.Stderr, []byte(stderrTruncatedMark)) {
		t.Fatalf("stderr length/suffix = %d/%q", len(result.Stderr), result.Stderr[len(result.Stderr)-20:])
	}
}

// Rationale: cancellation must interrupt a contract-compliant blocked sink so
// neither the copier nor the subprocess can retain the operation indefinitely.
func TestOSRunnerStreamBinaryCancellationInterruptsBlockedSink(t *testing.T) {
	runner, opts := binaryHelperRunner(t, "blocked")
	ctx, cancel := context.WithCancel(context.Background())
	sink := newBlockedSink()
	resultCh := make(chan error, 1)
	go func() {
		_, err := runner.streamBinary(ctx, opts, sink.binarySink())
		resultCh <- err
	}()

	t.Cleanup(cancel)
	awaitValue(t, sink.entered, "blocked sink entry")
	cancel()
	select {
	case err := <-resultCh:
		if err != context.Canceled {
			t.Fatalf("StreamBinary() error = %v, want authoritative context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("StreamBinary() did not return after synchronized cancellation")
	}
}

// Rationale: cancellation owns the local process group so a child retaining
// inherited archive pipes cannot outlive its parent and hang task teardown.
func TestOSRunnerStreamBinaryCancellationSignalsSameProcessGroupChild(t *testing.T) {
	runner, opts := binaryHelperRunner(t, "group-parent")
	ctx, cancel := context.WithCancel(context.Background())
	sink := newReadySink()
	resultCh := make(chan error, 1)
	go func() {
		_, err := runner.streamBinary(ctx, opts, sink.binarySink())
		resultCh <- err
	}()

	t.Cleanup(cancel)
	awaitValue(t, sink.ready, "process-group child readiness")
	cancel()
	select {
	case err := <-resultCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("StreamBinary() error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("same-process-group child retained the stdout pipe")
	}
}

// Rationale: the binary runner is not a generic execution surface; only the
// constructor-owned PostgreSQL dump grammar may produce archive stdout.
func TestOSRunnerStreamBinaryRejectsUntypedAndRestoreCommands(t *testing.T) {
	sink := (&memorySink{}).binarySink()
	_, err := NewBinary(nil).StreamPostgresDump(
		context.Background(),
		PostgresDumpCommand{ContainerID: "not-a-container", Database: "api_tsv4rr", Role: "api_tsv4rr"},
		sink,
	)
	assertErrorKind(t, err, errs.KindValidationFailed)

	runner := &OSRunner{binaryCommands: productionBinaryCommands}
	err = runner.validateBinaryCommand(RunCmdOpts{
		Name: "docker", Args: []string{"container", "exec", "-i", "--detach", "container", "pg_dump"},
	})
	assertErrorKind(t, err, errs.KindValidationFailed)
	err = runner.validateBinaryCommand(RunCmdOpts{
		Name: "docker",
		Args: []string{
			"container", "exec", "-i", "--user", "postgres", "0123456789ab", "pg_restore",
			"--clean", "--if-exists", "--no-owner", "--no-acl", "--exit-on-error",
			"--single-transaction", "--host=/var/run/postgresql", "--username=postgres",
			"--no-password", "--role=api_tsv4rr", "--dbname=api_tsv4rr",
		},
	})
	assertErrorKind(t, err, errs.KindValidationFailed)
}

// Rationale: redaction must span copy boundaries while bounding retained
// working memory for both ordinary and maximum-sized configured values.
func TestRedactedStderrPreservesSplitRedactionAndBoundsWorkingMemory(t *testing.T) {
	capture, err := newRedactedStderr([]string{"backup-secret-token"})
	if err != nil {
		t.Fatalf("newRedactedStderr() error = %v", err)
	}
	if _, err := capture.Write([]byte("prefix backup-secr")); err != nil {
		t.Fatalf("Write(first redaction segment) error = %v", err)
	}
	if _, err := capture.Write([]byte("et-token suffix")); err != nil {
		t.Fatalf("Write(second redaction segment) error = %v", err)
	}
	if got, want := string(capture.Bytes()), "prefix "+stderrRedactedMark+" suffix"; got != want {
		t.Fatalf("stderr = %q, want %q", got, want)
	}

	capture, err = newRedactedStderr([]string{strings.Repeat("s", maxRedactionItemBytes)})
	if err != nil {
		t.Fatalf("newRedactedStderr() error = %v", err)
	}
	if _, err := capture.Write(bytes.Repeat([]byte("x"), maxBinaryStderrBytes*8)); err != nil {
		t.Fatalf("Write(oversized diagnostics) error = %v", err)
	}
	if len(capture.pending) != 0 || cap(capture.output) > maxBinaryStderrBytes {
		t.Fatalf("working memory pending/output cap = %d/%d", len(capture.pending), cap(capture.output))
	}
}

// Rationale: overlapping secrets must select the longest match at the same
// byte offset regardless of configuration order or write segmentation.
func TestRedactedStderrUsesLongestOverlappingMatch(t *testing.T) {
	for _, secrets := range [][]string{{"a", "abc"}, {"abc", "a"}} {
		capture, err := newRedactedStderr(secrets)
		if err != nil {
			t.Fatalf("newRedactedStderr(%q) error = %v", secrets, err)
		}
		if _, err := capture.Write([]byte("prefix a")); err != nil {
			t.Fatalf("Write(first) error = %v", err)
		}
		if _, err := capture.Write([]byte("bc suffix")); err != nil {
			t.Fatalf("Write(second) error = %v", err)
		}
		if got, want := string(capture.Bytes()), "prefix "+stderrRedactedMark+" suffix"; got != want {
			t.Fatalf("stderr with secrets %q = %q, want %q", secrets, got, want)
		}
	}
}

// Rationale: redaction configuration itself is untrusted bounded task input
// and must not permit unbounded item count or retained matcher state.
func TestRedactedStderrRejectsUnboundedInputs(t *testing.T) {
	_, err := newRedactedStderr(make([]string, maxRedactionItems+1))
	assertErrorKind(t, err, errs.KindValidationFailed)
	_, err = newRedactedStderr([]string{strings.Repeat("x", maxRedactionItemBytes+1)})
	assertErrorKind(t, err, errs.KindValidationFailed)
}

// Rationale: the established Runner contract must remain implementable by
// Run/Stream-only doubles when binary backup capture is introduced separately.
func TestRunnerInterfaceRemainsCompatibleWithRunStreamImplementers(t *testing.T) {
	var stable Runner = &legacyRunner{}
	if _, widened := stable.(BinaryRunner); widened {
		t.Fatal("legacy Runner unexpectedly implements BinaryRunner")
	}
}

// Rationale: cancellation that already exists at entry must not launch or
// record any subprocess and must remain the authoritative returned error.
func TestOSRunnerStreamBinaryDoesNotStartWithPreCanceledContext(t *testing.T) {
	runner, opts := binaryHelperRunner(t, "raw")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	sink := &memorySink{}
	result, err := runner.streamBinary(ctx, opts, sink.binarySink())
	if !errors.Is(err, context.Canceled) || result.ExitCode != -1 || len(sink.bytes()) != 0 {
		t.Fatalf("StreamBinary(pre-canceled) = %#v, %v, stdout %q", result, err, sink.bytes())
	}
}

// Rationale: capture argv must include every normative fixed flag exactly
// once, accept only resolved identities, and leave restore unsupported.
func TestPostgresDumpCommandOwnsClosedArgvGrammar(t *testing.T) {
	command := PostgresDumpCommand{
		ContainerID: "0123456789ab",
		Database:    "api-web_tsv4rr",
		Role:        "api_worker_tsv4rr",
	}
	accepted, err := command.runCmdOpts()
	if err != nil {
		t.Fatalf("runCmdOpts() error = %v", err)
	}
	want := []string{
		"container", "exec", "-i", "--user", "postgres", "0123456789ab", "pg_dump",
		"--format=custom", "--compress=0", "--no-owner", "--no-acl",
		"--host=/var/run/postgresql", "--username=postgres", "--no-password",
		"--role=api_worker_tsv4rr", "--dbname=api-web_tsv4rr",
	}
	if !equalStrings(accepted.Args, want) {
		t.Fatalf("runCmdOpts().Args = %#v, want %#v", accepted.Args, want)
	}
	runner := &OSRunner{binaryCommands: productionBinaryCommands}
	if err := runner.validateBinaryCommand(accepted); err != nil {
		t.Fatalf("validateBinaryCommand(accepted) error = %v", err)
	}

	type rejection struct {
		name   string
		mutate func(*RunCmdOpts)
	}
	rejections := []rejection{
		{name: "added output file", mutate: func(opts *RunCmdOpts) {
			opts.Args = append(opts.Args, "--file=/tmp/archive")
		}},
		{name: "bad container", mutate: func(opts *RunCmdOpts) { opts.Args[5] = "container-name" }},
		{name: "bad database", mutate: func(opts *RunCmdOpts) {
			opts.Args[15] = "--dbname=postgres host=remote"
		}},
		{name: "bad role", mutate: func(opts *RunCmdOpts) { opts.Args[14] = "--role=-superuser" }},
		{name: "missing compress", mutate: func(opts *RunCmdOpts) {
			opts.Args = append(opts.Args[:8], opts.Args[9:]...)
		}},
		{name: "reordered flags", mutate: func(opts *RunCmdOpts) {
			opts.Args[7], opts.Args[8] = opts.Args[8], opts.Args[7]
		}},
		{name: "restore", mutate: func(opts *RunCmdOpts) { opts.Args[6] = "pg_restore" }},
		{name: "missing no password", mutate: func(opts *RunCmdOpts) {
			opts.Args = append(opts.Args[:13], opts.Args[14:]...)
		}},
		{name: "stdin", mutate: func(opts *RunCmdOpts) { opts.Stdin = []byte("archive") }},
		{name: "environment", mutate: func(opts *RunCmdOpts) { opts.Env = []string{"PGHOST=remote"} }},
		{name: "replace environment", mutate: func(opts *RunCmdOpts) { opts.ReplaceEnv = true }},
		{name: "directory", mutate: func(opts *RunCmdOpts) { opts.Dir = "/tmp" }},
		{name: "redactions", mutate: func(opts *RunCmdOpts) { opts.StderrRedactions = []string{"value"} }},
	}
	for _, test := range rejections {
		candidate := cloneOpts(accepted)
		test.mutate(&candidate)
		t.Run(test.name, func(t *testing.T) {
			assertErrorKind(t, runner.validateBinaryCommand(candidate), errs.KindValidationFailed)
		})
	}
	invalidCommands := []struct {
		name    string
		command PostgresDumpCommand
	}{
		{name: "container", command: PostgresDumpCommand{
			ContainerID: "not-an-id", Database: command.Database, Role: command.Role,
		}},
		{name: "database", command: PostgresDumpCommand{
			ContainerID: command.ContainerID, Database: "api-bad_iloooo", Role: command.Role,
		}},
		{name: "role", command: PostgresDumpCommand{
			ContainerID: command.ContainerID, Database: command.Database, Role: "",
		}},
	}
	for _, test := range invalidCommands {
		t.Run("constructor "+test.name, func(t *testing.T) {
			_, err := test.command.runCmdOpts()
			assertErrorKind(t, err, errs.KindValidationFailed)
		})
	}
}

// Rationale: FakeRunner call recording is shared by parallel L1 workers and
// must provide owned snapshots without data races or caller mutation aliases.
func TestFakeRunnerConcurrentCallsAreRaceSafe(t *testing.T) {
	fake := NewFake()
	fake.Results["fixed"] = Result{Stdout: []byte("result"), ExitCode: 0}
	var callers sync.WaitGroup
	callers.Add(64)
	for index := range 64 {
		go func() {
			defer callers.Done()
			result, err := fake.Run(context.Background(), RunCmdOpts{
				Name: "fixed", Args: []string{strconv.Itoa(index)},
			})
			if err != nil || string(result.Stdout) != "result" {
				t.Errorf("Run() = %#v, %v", result, err)
			}
		}()
	}
	callers.Wait()
	if got := len(fake.RecordedCalls()); got != 64 {
		t.Fatalf("recorded calls = %d, want 64", got)
	}
}

// Rationale: the fake must interrupt exactly once on a copy failure and retain
// write, wait, and cleanup causes under the same typed operational error.
func TestFakeRunnerStreamBinaryWritesIncrementallyAndJoinsErrors(t *testing.T) {
	fake := NewFake()
	runErr := fmt.Errorf("run failed")
	writeErr := fmt.Errorf("write failed")
	fake.Results["docker"] = Result{Stdout: bytes.Repeat([]byte("x"), fakeBinaryChunkBytes+1), ExitCode: 7}
	fake.Errors["docker"] = runErr
	sink := &failAfterSink{failAt: 2, err: writeErr}
	result, err := fake.StreamPostgresDump(
		context.Background(), validPostgresDumpCommand(), sink.binarySink(),
	)
	if result.ExitCode != 7 || sink.writes != 2 || sink.interrupts != 1 ||
		!errors.Is(sink.interruptCause, writeErr) || !errors.Is(err, runErr) || !errors.Is(err, writeErr) {
		t.Fatalf("StreamBinary() = %#v, writes %d, error %v", result, sink.writes, err)
	}
	assertErrorKind(t, err, errs.KindInternal)
}

// Rationale: a nil-error short write is still a failed archive copy and must
// interrupt the sink once rather than silently truncating the artifact.
func TestFakeRunnerStreamBinaryRejectsShortWrite(t *testing.T) {
	fake := NewFake()
	fake.Results["docker"] = Result{Stdout: []byte("archive"), ExitCode: 0}
	sink := &shortWriteSink{}
	_, err := fake.StreamPostgresDump(context.Background(), validPostgresDumpCommand(), sink.binarySink())
	if !errors.Is(err, io.ErrShortWrite) || sink.interrupts != 1 {
		t.Fatalf("StreamBinary(short write) error = %v, interrupts = %d", err, sink.interrupts)
	}
	assertErrorKind(t, err, errs.KindInternal)
}

// Rationale: the fake is an execution contract double, so it must reject an
// invalid generated identity before recording a call or delivering any bytes.
func TestFakeRunnerStreamPostgresDumpEnforcesTypedGrammar(t *testing.T) {
	fake := NewFake()
	command := validPostgresDumpCommand()
	command.Role = "api_bad_iiiiii"
	_, err := fake.StreamPostgresDump(context.Background(), command, (&memorySink{}).binarySink())
	assertErrorKind(t, err, errs.KindValidationFailed)
	if calls := fake.RecordedCalls(); len(calls) != 0 {
		t.Fatalf("RecordedCalls() = %#v, want none for invalid command", calls)
	}
}

// Rationale: the fake's default streaming lifecycle must enforce the typed
// command timeout and interrupt a blocked sink without relying on its caller.
func TestFakeRunnerStreamPostgresDumpEnforcesTimeout(t *testing.T) {
	fake := NewFake()
	fake.Results["docker"] = Result{Stdout: bytes.Repeat([]byte("x"), fakeBinaryChunkBytes), ExitCode: 0}
	command := validPostgresDumpCommand()
	command.Timeout = 30 * time.Millisecond
	sink := newBlockedSink()
	outer, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := fake.StreamPostgresDump(outer, command, sink.binarySink())
	if err != context.DeadlineExceeded || outer.Err() != nil {
		t.Fatalf("StreamPostgresDump(timeout) error = %v, outer error = %v", err, outer.Err())
	}
}

// Rationale: a process-group signaling failure must remain observable as
// typed cleanup evidence while owned pipes are closed and the call terminates.
func TestOSRunnerStreamBinaryReturnsProcessGroupSignalFailure(t *testing.T) {
	runner, opts := binaryHelperRunner(t, "signal-failure-linger")
	signalErr := errors.New("injected process-group signal failure")
	processID := make(chan int, 1)
	runner.signalGroup = func(pid int) error {
		processID <- pid
		return signalErr
	}
	writeErr := errors.New("sink write failed")
	sink := newGatedInterruptFailSink(writeErr)
	t.Cleanup(sink.releaseInterrupt)
	result := make(chan error, 1)
	go func() {
		_, err := runner.streamBinary(context.Background(), opts, sink.binarySink())
		result <- err
	}()
	pid := awaitValue(t, processID, "failed process-group signal")
	awaitValue(t, sink.interruptEntered, "gated sink interruption")
	select {
	case err := <-result:
		t.Fatalf("StreamBinary returned before sink interruption completed: %v", err)
	default:
	}
	sink.releaseInterrupt()
	var err error
	select {
	case err = <-result:
	case <-time.After(500 * time.Millisecond):
		if killErr := syscall.Kill(pid, syscall.SIGKILL); killErr != nil && !errors.Is(killErr, syscall.ESRCH) {
			t.Fatalf("cleanup kill after blocked signal failure = %v", killErr)
		}
		awaitValue(t, result, "stream result after watchdog cleanup")
		t.Fatal("StreamBinary waited after the kernel signaler reported failure")
	}
	if killErr := syscall.Kill(pid, syscall.SIGKILL); killErr != nil && !errors.Is(killErr, syscall.ESRCH) {
		t.Fatalf("released process cleanup kill = %v", killErr)
	}
	if !errors.Is(err, writeErr) || !errors.Is(err, signalErr) {
		t.Fatalf("StreamBinary(signal failure) error = %v", err)
	}
	assertErrorKind(t, err, errs.KindInternal)
}

// Rationale: when no context error exists, copy failure is primary, wait
// failure is subordinate, and all private causes remain behind one safe type.
func TestBinaryOperationalErrorHasDeterministicTypedPrecedence(t *testing.T) {
	copyErr := errors.New("copy locator must remain private")
	waitErr := errors.New("wait failed")
	cleanupErr := errors.New("cleanup failed")
	err := binaryOperationalError(copyErr, waitErr, cleanupErr)
	assertErrorKind(t, err, errs.KindInternal)
	if !errors.Is(err, copyErr) || !errors.Is(err, waitErr) || !errors.Is(err, cleanupErr) {
		t.Fatalf("binaryOperationalError() lost a private cause: %v", err)
	}
	if strings.Index(err.Error(), copyErr.Error()) > strings.Index(err.Error(), waitErr.Error()) {
		t.Fatalf("binaryOperationalError() precedence = %q", err)
	}
	var typed *errs.Error
	if !errors.As(err, &typed) || strings.Contains(typed.ToProblem().Detail, "locator") {
		t.Fatalf("binaryOperationalError() public problem = %#v", typed.ToProblem())
	}
}

// Rationale: tests that need custom binary timing may own delivery explicitly,
// while FakeRunner still records exactly one immutable invocation.
func TestFakeRunnerStreamBinaryFuncOwnsIncrementalDelivery(t *testing.T) {
	fake := NewFake()
	wantErr := fmt.Errorf("stream failed")
	firstWritten := make(chan struct{})
	continueStream := make(chan struct{})
	var continueOnce sync.Once
	t.Cleanup(func() { continueOnce.Do(func() { close(continueStream) }) })
	fake.StreamPostgresDumpFunc = func(
		_ context.Context,
		_ PostgresDumpCommand,
		sink BinarySink,
	) (Result, error) {
		if _, err := sink.Writer.Write([]byte("first")); err != nil {
			return Result{}, err
		}
		close(firstWritten)
		<-continueStream
		if _, err := sink.Writer.Write([]byte("second")); err != nil {
			return Result{}, err
		}
		return Result{ExitCode: 9}, wantErr
	}
	sink := &memorySink{}
	resultCh := make(chan binaryResult, 1)
	go func() {
		result, err := fake.StreamPostgresDump(
			context.Background(), validPostgresDumpCommand(), sink.binarySink(),
		)
		resultCh <- binaryResult{result: result, err: err}
	}()
	awaitValue(t, firstWritten, "fake custom stream first write")
	if got := string(sink.bytes()); got != "first" {
		t.Fatalf("incremental stdout = %q, want first", got)
	}
	continueOnce.Do(func() { close(continueStream) })
	result := awaitValue(t, resultCh, "fake custom stream result")
	if result.result.ExitCode != 9 || !errors.Is(result.err, wantErr) ||
		string(sink.bytes()) != "firstsecond" {
		t.Fatalf("StreamBinary() = %#v, %v, stdout %q", result.result, result.err, sink.bytes())
	}
	if got := len(fake.RecordedCalls()); got != 1 {
		t.Fatalf("recorded calls = %d, want 1", got)
	}
}

type binaryResult struct {
	result Result
	err    error
}

type memorySink struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (s *memorySink) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buffer.Write(p)
}

func (s *memorySink) bytes() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]byte(nil), s.buffer.Bytes()...)
}

func (s *memorySink) binarySink() BinarySink {
	return BinarySink{Writer: s, Interrupt: func(error) error { return nil }}
}

type gateSink struct {
	memorySink
	entered     chan struct{}
	release     chan struct{}
	enterOnce   sync.Once
	releaseOnce sync.Once
}

func newGateSink() *gateSink {
	return &gateSink{entered: make(chan struct{}), release: make(chan struct{})}
}

func (s *gateSink) Write(p []byte) (int, error) {
	s.enterOnce.Do(func() { close(s.entered) })
	<-s.release
	return s.memorySink.Write(p)
}

func (s *gateSink) releaseWrites() {
	s.releaseOnce.Do(func() { close(s.release) })
}

func (s *gateSink) binarySink() BinarySink {
	return BinarySink{Writer: s, Interrupt: func(error) error { return nil }}
}

var errSinkInterrupted = fmt.Errorf("sink interrupted")

type blockedSink struct {
	entered     chan struct{}
	unblocked   chan struct{}
	enteredOnce sync.Once
	unblockOnce sync.Once
}

func newBlockedSink() *blockedSink {
	return &blockedSink{entered: make(chan struct{}), unblocked: make(chan struct{})}
}

func (s *blockedSink) Write([]byte) (int, error) {
	s.enteredOnce.Do(func() { close(s.entered) })
	<-s.unblocked
	return 0, errSinkInterrupted
}

func (s *blockedSink) binarySink() BinarySink {
	return BinarySink{
		Writer: s,
		Interrupt: func(error) error {
			s.unblockOnce.Do(func() { close(s.unblocked) })
			return errSinkInterrupted
		},
	}
}

type readySink struct {
	memorySink
	ready chan struct{}
	once  sync.Once
}

func newReadySink() *readySink {
	return &readySink{ready: make(chan struct{})}
}

func (s *readySink) Write(p []byte) (int, error) {
	written, err := s.memorySink.Write(p)
	if bytes.Contains(s.bytes(), []byte("child-ready:")) {
		s.once.Do(func() { close(s.ready) })
	}
	return written, err
}

func (s *readySink) binarySink() BinarySink {
	return BinarySink{Writer: s, Interrupt: func(error) error { return nil }}
}

type failAfterSink struct {
	writes         int
	failAt         int
	err            error
	interrupts     int
	interruptCause error
}

func (s *failAfterSink) Write(p []byte) (int, error) {
	s.writes++
	if s.writes == s.failAt {
		return 0, s.err
	}
	return len(p), nil
}

func (s *failAfterSink) binarySink() BinarySink {
	return BinarySink{
		Writer: s,
		Interrupt: func(cause error) error {
			s.interrupts++
			s.interruptCause = cause
			return nil
		},
	}
}

type shortWriteSink struct {
	interrupts int
}

type gatedInterruptFailSink struct {
	err              error
	interruptEntered chan struct{}
	interruptRelease chan struct{}
	enterOnce        sync.Once
	releaseOnce      sync.Once
}

func newGatedInterruptFailSink(err error) *gatedInterruptFailSink {
	return &gatedInterruptFailSink{
		err:              err,
		interruptEntered: make(chan struct{}),
		interruptRelease: make(chan struct{}),
	}
}

func (sink *gatedInterruptFailSink) Write([]byte) (int, error) {
	return 0, sink.err
}

func (sink *gatedInterruptFailSink) binarySink() BinarySink {
	return BinarySink{
		Writer: sink,
		Interrupt: func(error) error {
			sink.enterOnce.Do(func() { close(sink.interruptEntered) })
			<-sink.interruptRelease
			return nil
		},
	}
}

func (sink *gatedInterruptFailSink) releaseInterrupt() {
	sink.releaseOnce.Do(func() { close(sink.interruptRelease) })
}

func (*shortWriteSink) Write(p []byte) (int, error) {
	return max(0, len(p)-1), nil
}

func (s *shortWriteSink) binarySink() BinarySink {
	return BinarySink{
		Writer: s,
		Interrupt: func(error) error {
			s.interrupts++
			return nil
		},
	}
}

type legacyRunner struct{}

func (*legacyRunner) Run(context.Context, RunCmdOpts) (Result, error) {
	return Result{}, nil
}

func (*legacyRunner) Stream(context.Context, RunCmdOpts, func(bool, string)) (Result, error) {
	return Result{}, nil
}

var _ Runner = (*legacyRunner)(nil)
var _ Runner = (*OSRunner)(nil)
var _ BinaryRunner = (*OSRunner)(nil)
var _ Runner = (*FakeRunner)(nil)
var _ BinaryRunner = (*FakeRunner)(nil)

func binaryHelperRunner(t *testing.T, mode string) (*OSRunner, RunCmdOpts) {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable() error = %v", err)
	}
	runner := &OSRunner{binaryCommands: map[string]binaryCommandPolicy{
		executable: {validate: func(RunCmdOpts) bool { return true }},
	}}
	return runner, RunCmdOpts{
		Name:    executable,
		Args:    []string{"-test.run=TestRunnerBinaryHelper"},
		Env:     []string{binaryHelperModeEnv + "=" + mode},
		Timeout: 5 * time.Second,
	}
}

func validPostgresDumpCommand() PostgresDumpCommand {
	return PostgresDumpCommand{
		ContainerID: "0123456789ab",
		Database:    "api-web_tsv4rr",
		Role:        "api_worker_tsv4rr",
	}
}

func assertErrorKind(t *testing.T, err error, want errs.Kind) {
	t.Helper()
	got, ok := errs.KindOf(err)
	if !ok || got != want {
		t.Fatalf("error = %v, kind = %v/%t, want %v", err, got, ok, want)
	}
}

func awaitValue[T any](t *testing.T, channel <-chan T, description string) T {
	t.Helper()
	select {
	case value := <-channel:
		return value
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
		var zero T
		return zero
	}
}
