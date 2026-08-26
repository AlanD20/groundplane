package runner

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const outputLimitHelperModeEnv = "GROUNDPLANE_RUNNER_OUTPUT_LIMIT_HELPER"

// Rationale: capture-limit tests need deterministic child output without
// depending on host shell behavior or commands outside the test binary.
func TestRunnerOutputLimitHelper(t *testing.T) {
	mode := os.Getenv(outputLimitHelperModeEnv)
	if mode == "" {
		return
	}
	switch mode {
	case "stdout-overflow":
		mustHelperWrite(os.Stdout, bytes.Repeat([]byte("o"), 65))
	case "stderr-overflow":
		mustHelperWrite(os.Stderr, bytes.Repeat([]byte("e"), 65))
	case "combined-overflow":
		var writers sync.WaitGroup
		writers.Add(2)
		go func() {
			defer writers.Done()
			mustHelperWrite(os.Stdout, bytes.Repeat([]byte("o"), 48))
		}()
		go func() {
			defer writers.Done()
			mustHelperWrite(os.Stderr, bytes.Repeat([]byte("e"), 48))
		}()
		writers.Wait()
	case "exact-boundary":
		mustHelperWrite(os.Stdout, bytes.Repeat([]byte("b"), 64))
	case "stream-overflow":
		mustHelperWrite(os.Stdout, []byte("one\nxxxxxxxx"))
	case "overflow-then-block":
		mustHelperWrite(os.Stdout, bytes.Repeat([]byte("h"), 65))
		for {
			if err := syscall.Pause(); err != nil && !errors.Is(err, syscall.EINTR) {
				os.Exit(95)
			}
		}
	case "descendant-parent":
		child := exec.Command(os.Args[0], "-test.run=TestRunnerOutputLimitHelper")
		child.Env = append(os.Environ(), outputLimitHelperModeEnv+"=descendant-writer")
		child.Stdout = os.Stdout
		child.Stderr = os.Stderr
		if err := child.Start(); err != nil {
			os.Exit(91)
		}
		if err := child.Wait(); err != nil {
			os.Exit(94)
		}
	case "descendant-writer":
		chunk := bytes.Repeat([]byte("d"), 32*1024)
		for {
			mustHelperWrite(os.Stdout, chunk)
		}
	case "unlimited":
		mustHelperWrite(os.Stdout, bytes.Repeat([]byte("u"), 256))
		mustHelperWrite(os.Stderr, bytes.Repeat([]byte("v"), 256))
	default:
		os.Exit(92)
	}
	os.Exit(0)
}

func mustHelperWrite(file *os.File, payload []byte) {
	if err := writeFull(file, payload); err != nil {
		os.Exit(93)
	}
}

// Rationale: stdout alone must not retain beyond the configured combined
// budget and must fail with the stable typed classification.
func TestOSRunnerRunCapsStdout(t *testing.T) {
	result, err := New(nil).Run(context.Background(), outputLimitHelperOpts(t, "stdout-overflow", 64))
	assertOutputLimitError(t, err)
	if len(result.Stdout) != 64 || len(result.Stderr) != 0 ||
		!bytes.Equal(result.Stdout, bytes.Repeat([]byte("o"), 64)) {
		t.Fatalf("Run() output lengths = %d/%d, stdout %q", len(result.Stdout), len(result.Stderr), result.Stdout)
	}
}

// Rationale: stderr shares the same budget and preserves its bounded prefix
// for diagnostics instead of losing all evidence on overflow.
func TestOSRunnerRunCapsStderr(t *testing.T) {
	result, err := New(nil).Run(context.Background(), outputLimitHelperOpts(t, "stderr-overflow", 64))
	assertOutputLimitError(t, err)
	if len(result.Stdout) != 0 || len(result.Stderr) != 64 ||
		!bytes.Equal(result.Stderr, bytes.Repeat([]byte("e"), 64)) {
		t.Fatalf("Run() output lengths = %d/%d, stderr %q", len(result.Stdout), len(result.Stderr), result.Stderr)
	}
}

// Rationale: concurrent stdout and stderr writers must consume one race-safe
// budget rather than independently retaining the full configured amount.
func TestOSRunnerRunCapsCombinedConcurrentOutput(t *testing.T) {
	result, err := New(nil).Run(context.Background(), outputLimitHelperOpts(t, "combined-overflow", 64))
	assertOutputLimitError(t, err)
	if len(result.Stdout)+len(result.Stderr) != 64 ||
		!bytes.Equal(result.Stdout, bytes.Repeat([]byte("o"), len(result.Stdout))) ||
		!bytes.Equal(result.Stderr, bytes.Repeat([]byte("e"), len(result.Stderr))) {
		t.Fatalf("Run() retained stdout/stderr lengths = %d/%d", len(result.Stdout), len(result.Stderr))
	}
}

// Rationale: the configured byte count is inclusive; reaching it exactly is
// successful and must not be confused with exceeding it.
func TestOSRunnerRunAllowsExactCaptureBoundary(t *testing.T) {
	result, err := New(nil).Run(context.Background(), outputLimitHelperOpts(t, "exact-boundary", 64))
	if err != nil || result.ExitCode != 0 || len(result.Stdout) != 64 {
		t.Fatalf("Run(exact boundary) = %#v, %v", result, err)
	}
}

// Rationale: a successful process result must not mask overflow already
// observed by the shared capture path.
func TestOSRunnerRunReturnsOutputLimitAfterExitZero(t *testing.T) {
	capture, err := newOutputCapture(64)
	if err != nil {
		t.Fatalf("newOutputCapture() error = %v", err)
	}
	capture.write(false, bytes.Repeat([]byte("z"), 65))
	result := capture.result(0)
	err = capture.resolveError(nil, nil)
	assertOutputLimitError(t, err)
	if result.ExitCode != 0 || len(result.Stdout) != 64 {
		t.Fatalf("resolved exit-zero overflow = %#v, %v", result, err)
	}
}

// Rationale: the output-limit classification is authoritative while the
// simultaneous process failure remains available to typed cause inspection.
func TestOutputLimitPreservesSimultaneousProcessCause(t *testing.T) {
	capture, err := newOutputCapture(1)
	if err != nil {
		t.Fatalf("newOutputCapture() error = %v", err)
	}
	capture.write(false, []byte("xx"))
	processErr := &exec.ExitError{}
	err = capture.resolveError(nil, processErr)
	assertOutputLimitError(t, err)
	if !errors.Is(err, processErr) {
		t.Fatalf("output-limit error lost process cause: %v", err)
	}
}

// Rationale: Stream must apply the same combined budget while continuing to
// deliver complete and final-prefix lines retained before overflow.
func TestOSRunnerStreamCapsOutputAndDeliversPrefix(t *testing.T) {
	var lines []string
	result, err := New(nil).Stream(
		context.Background(),
		outputLimitHelperOpts(t, "stream-overflow", 8),
		func(stderr bool, line string) { lines = append(lines, line) },
	)
	assertOutputLimitError(t, err)
	if got, want := string(result.Stdout), "one\nxxxx"; got != want || strings.Join(lines, "|") != "one|xxxx" {
		t.Fatalf("Stream() stdout/lines = %q/%q, want %q/%q", got, lines, want, []string{"one", "xxxx"})
	}
}

// Rationale: when both safeguards are configured, observed overflow must
// terminate an infinite writer promptly and remain the fail-closed result.
func TestOSRunnerOutputLimitPrecedesLaterTimeout(t *testing.T) {
	opts := outputLimitHelperOpts(t, "overflow-then-block", 64)
	opts.Timeout = 2 * time.Second
	started := time.Now()
	result, err := New(nil).Run(context.Background(), opts)
	assertOutputLimitError(t, err)
	if errors.Is(err, context.DeadlineExceeded) || time.Since(started) >= time.Second || len(result.Stdout) != 64 {
		t.Fatalf("Run(limit plus timeout) = %#v, %v after %s", result, err, time.Since(started))
	}
}

// Rationale: overflow must terminate a whole process group before waiting for
// EOF when a descendant inherits the command's stdout pipe.
func TestOSRunnerOutputLimitTerminatesInheritedStdioDescendant(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(map[bool]string{false: "run", true: "stream"}[stream], func(t *testing.T) {
			opts := outputLimitHelperOpts(t, "descendant-parent", 64)
			opts.Timeout = 2 * time.Second
			started := time.Now()
			var result Result
			var err error
			if stream {
				result, err = New(nil).Stream(context.Background(), opts, func(bool, string) {})
			} else {
				result, err = New(nil).Run(context.Background(), opts)
			}
			assertOutputLimitError(t, err)
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) || time.Since(started) >= time.Second ||
				len(result.Stdout)+len(result.Stderr) != 64 {
				t.Fatalf("inherited-stdio stream=%t = %#v, %v after %s", stream, result, err, time.Since(started))
			}
		})
	}
}

// Rationale: zero is the compatibility default, so existing Run and Stream
// callers retain complete output without acquiring a hidden bound.
func TestOSRunnerZeroCaptureLimitRemainsUnlimited(t *testing.T) {
	runner := New(nil)
	for _, stream := range []bool{false, true} {
		opts := outputLimitHelperOpts(t, "unlimited", 0)
		var result Result
		var err error
		if stream {
			result, err = runner.Stream(context.Background(), opts, func(bool, string) {})
		} else {
			result, err = runner.Run(context.Background(), opts)
		}
		if err != nil || result.ExitCode != 0 || len(result.Stdout) != 256 || len(result.Stderr) != 256 {
			t.Fatalf("unlimited stream=%t = %#v, %v", stream, result, err)
		}
	}
}

func outputLimitHelperOpts(t *testing.T, mode string, limitBytes int64) RunCmdOpts {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable() error = %v", err)
	}
	return RunCmdOpts{
		Name:              executable,
		Args:              []string{"-test.run=TestRunnerOutputLimitHelper"},
		Env:               []string{outputLimitHelperModeEnv + "=" + mode},
		Timeout:           5 * time.Second,
		CaptureLimitBytes: limitBytes,
	}
}

func assertOutputLimitError(t *testing.T, err error) {
	t.Helper()
	assertErrorKind(t, err, errs.KindInternal)
}
