package agent

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/common/runner"
)

const (
	backingHookTestContainer = "0123456789abcdef"
	backingHookTestService   = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	backingHookTestResult    = "/tmp/groundplane-hook.Ab12Cd/result"
)

// BACK-13
// Rationale: Task cancellation must not detach Docker exec and race result
// cleanup against a hook that is still running inside the container.
func TestExecuteBackingHookJoinsCancellationBeforeCleanup(t *testing.T) {
	secret := "private-hook-input"
	started := make(chan struct{})
	release := make(chan struct{})
	cleanup := make(chan struct{})
	executionContextCanceled := make(chan struct{})
	callCount := 0
	var commandContextDeadline time.Time
	var commandTimeout time.Duration
	taskRunner := runner.NewFake()
	taskRunner.RunFunc = func(ctx context.Context, opts runner.RunCmdOpts) (runner.Result, error) {
		callCount++
		switch callCount {
		case 1:
			return runner.Result{Stdout: []byte(backingHookTestResult + "\n")}, nil
		case 2:
			commandContextDeadline, _ = ctx.Deadline()
			commandTimeout = opts.Timeout
			close(started)
			select {
			case <-release:
				return runner.Result{Stdout: []byte(backingHookComplete)}, nil
			case <-ctx.Done():
				close(executionContextCanceled)
				return runner.Result{ExitCode: -1}, ctx.Err()
			}
		case 3:
			close(cleanup)
			return runner.Result{}, nil
		default:
			return runner.Result{ExitCode: -1}, fmt.Errorf("unexpected runner call %d", callCount)
		}
	}

	taskDeadline := time.Now().Add(time.Minute)
	ctx, cancel := context.WithDeadline(context.Background(), taskDeadline)
	defer cancel()
	type hookResult struct {
		output backinghook.Output
		err    error
	}
	done := make(chan hookResult, 1)
	go func() {
		output, err := ExecuteBackingHook(
			ctx,
			taskRunner,
			backingHookTestContainer,
			backinghook.Definition{
				Command: []string{"/bin/true"}, TimeoutSeconds: backinghook.MaximumTimeoutSeconds,
			},
			backinghook.Input{
				Context: backinghook.Context{Event: backinghook.BeforeStop, BackingServiceID: backingHookTestService},
				Values:  []backinghook.Value{{Key: "PASSWORD", Value: []byte(secret)}},
			},
			nil,
		)
		done <- hookResult{output: output, err: err}
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("ExecuteBackingHook did not start its command")
	}
	cancel()
	select {
	case <-cleanup:
		t.Error("result cleanup started before the in-container command completed")
	default:
	}
	select {
	case <-done:
		t.Error("ExecuteBackingHook returned before the in-container command completed")
	default:
	}
	close(release)

	select {
	case result := <-done:
		if !errors.Is(result.err, context.Canceled) {
			t.Fatalf("ExecuteBackingHook error = %v, want context cancellation", result.err)
		}
		if len(result.output.Facts) != 0 {
			t.Fatalf("ExecuteBackingHook returned %d facts after cancellation", len(result.output.Facts))
		}
		if strings.Contains(result.err.Error(), secret) {
			t.Fatal("ExecuteBackingHook cancellation error disclosed a secret input")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ExecuteBackingHook did not return after the command completed")
	}
	select {
	case <-cleanup:
	default:
		t.Fatal("result cleanup did not run after the command completed")
	}
	select {
	case <-executionContextCanceled:
		t.Fatal("Task cancellation canceled the Docker exec instead of joining it")
	default:
	}
	if callCount != 3 {
		t.Fatalf("runner calls = %d, want setup, command, and cleanup", callCount)
	}
	latestJoinDeadline := taskDeadline.Add(-2 * backingHookHelperTimeout)
	if commandContextDeadline.IsZero() || commandContextDeadline.After(latestJoinDeadline) {
		t.Fatalf("command context deadline = %v, want no later than %v", commandContextDeadline, latestJoinDeadline)
	}
	if latestJoinDeadline.Sub(commandContextDeadline) > time.Second {
		t.Fatalf(
			"command context deadline = %v, unexpectedly short of Task budget ending %v",
			commandContextDeadline,
			latestJoinDeadline,
		)
	}
	if commandTimeout <= 0 || commandTimeout > time.Minute-2*backingHookHelperTimeout {
		t.Fatalf("command timeout = %v, want positive and capped by remaining Task budget", commandTimeout)
	}
}

// BACK-13
// Rationale: a Docker transport failure is not remote completion evidence, so
// cleanup must wait out the sealed timeout/KILL window without leaking output.
func TestExecuteBackingHookCapsJoinAndRedactsRunnerFailure(t *testing.T) {
	secret := "secret-from-runner-failure"
	callCount := 0
	var commandArgs []string
	taskRunner := runner.NewFake()
	taskRunner.RunFunc = func(ctx context.Context, opts runner.RunCmdOpts) (runner.Result, error) {
		callCount++
		switch callCount {
		case 1:
			return runner.Result{Stdout: []byte(backingHookTestResult + "\n")}, nil
		case 2:
			commandArgs = append([]string(nil), opts.Args...)
			return runner.Result{
				Stdout:   []byte(secret),
				Stderr:   []byte(secret),
				ExitCode: -1,
			}, fmt.Errorf("transport failure containing %s", secret)
		case 3:
			return runner.Result{}, nil
		default:
			return runner.Result{ExitCode: -1}, fmt.Errorf("unexpected runner call %d", callCount)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	startedAt := time.Now()
	output, err := ExecuteBackingHook(
		ctx,
		taskRunner,
		backingHookTestContainer,
		backinghook.Definition{Command: []string{"/bin/false"}, TimeoutSeconds: 1},
		backinghook.Input{
			Context: backinghook.Context{Event: backinghook.BeforeStop, BackingServiceID: backingHookTestService},
			Values:  []backinghook.Value{{Key: "PASSWORD", Value: []byte(secret)}},
		},
		nil,
	)
	if err == nil {
		t.Fatal("ExecuteBackingHook succeeded after a runner failure")
	}
	if len(output.Facts) != 0 {
		t.Fatalf("ExecuteBackingHook returned %d facts after a runner failure", len(output.Facts))
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatal("ExecuteBackingHook disclosed runner output or error text")
	}
	if strings.Contains(strings.Join(commandArgs, "\x00"), secret) {
		t.Fatal("ExecuteBackingHook interpolated a secret into command argv")
	}
	minimumHold := time.Second + backingHookKillAfter + backingHookRunGrace - 500*time.Millisecond
	if elapsed := time.Since(startedAt); elapsed < minimumHold {
		t.Fatalf("transport failure held authority for %v, want at least %v before cleanup", elapsed, minimumHold)
	}
	if callCount != 3 {
		t.Fatalf("runner calls = %d, want setup, failed command, and cleanup", callCount)
	}
}
