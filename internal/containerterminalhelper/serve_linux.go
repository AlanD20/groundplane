package containerterminalhelper

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/terminalsession"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/creack/pty"
	"golang.org/x/sys/unix"
)

const maximumEncodedFrame = 128 << 10

type output struct {
	mu      sync.Mutex
	encoder *json.Encoder
}

func (o *output) send(event terminalsession.Event) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := o.encoder.Encode(event); err != nil {
		return errs.Wrap(errs.KindRequestUnavailable, err)
	}
	return nil
}

func readFrame[T any](reader *bufio.Reader, value *T) error {
	line, err := reader.ReadSlice('\n')
	if err != nil {
		return errs.Wrap(errs.KindRequestUnavailable, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(line))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return errs.Wrap(errs.KindValidationFailed, err)
	}
	if decoder.Decode(new(json.RawMessage)) != io.EOF {
		return errs.New(errs.KindValidationFailed, "terminal frame contains trailing data")
	}
	return nil
}

// Run executes only the Agent's validated request. The helper runs as container
// root to supervise cleanup; its child PTY runs with the requested credentials.
// No environment or host files are supplied by the Controller or Agent.
func Run(ctx context.Context, input io.Reader, stdout io.Writer) (int, error) {
	writer := &output{encoder: json.NewEncoder(stdout)}
	reader := bufio.NewReaderSize(input, maximumEncodedFrame)
	// No copied executable remains in the workload after startup. The kernel
	// retains the running inode; all later control uses the existing input pipe.
	executable, err := os.Executable()
	directory := filepath.Dir(executable)
	sessionID := strings.TrimPrefix(filepath.Base(directory), ".groundplane-terminal-")
	if os.Geteuid() != 0 || err != nil || ids.Validate(ids.KindTerminal, sessionID) != nil ||
		filepath.Clean(executable) != filepath.Join("/tmp", ".groundplane-terminal-"+sessionID, "helper") {
		return 1, errs.New(errs.KindValidationFailed, "container terminal helper location is invalid")
	}
	if err := os.Remove(executable); err != nil {
		return 1, errs.Wrap(errs.KindInternal, err)
	}
	if err := os.Remove(directory); err != nil {
		return 1, errs.Wrap(errs.KindInternal, err)
	}
	var options terminalsession.ContainerOptions
	optionsReady := make(chan error, 1)
	go func() { optionsReady <- readFrame(reader, &options) }()
	select {
	case <-ctx.Done():
		return 1, ctx.Err()
	case <-time.After(10 * time.Second):
		return 1, errs.New(errs.KindRequestUnavailable, "container terminal setup timed out")
	case err := <-optionsReady:
		if err != nil || options.Validate() != nil || options.SessionID != sessionID {
			_ = writer.send(terminalsession.Event{Closed: true, Reason: "invalid_options"})
			return 1, errs.New(errs.KindValidationFailed, "container terminal setup is invalid")
		}
	}
	pidfd, err := unix.PidfdOpen(os.Getpid(), 0)
	if err != nil {
		return 1, errs.Wrap(errs.KindRequestUnavailable, err)
	}
	_ = unix.Close(pidfd)
	if err := unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0); err != nil {
		_ = writer.send(terminalsession.Event{Closed: true, Reason: "supervision_unavailable"})
		return 1, errs.Wrap(errs.KindInternal, err)
	}
	credential, home, err := credentials(options.User)
	if err != nil {
		_ = writer.send(terminalsession.Event{Closed: true, Reason: "user_unavailable"})
		return 1, err
	}
	command := exec.Command(options.Command[0], options.Command[1:]...)
	command.Dir, command.Env = options.WorkingDirectory, commandEnvironment(home)
	command.SysProcAttr = &syscall.SysProcAttr{Setsid: true, Setctty: true, Credential: credential}
	file, err := pty.StartWithSize(command, &pty.Winsize{Cols: options.Size.Columns, Rows: options.Size.Rows})
	if err != nil {
		_ = writer.send(terminalsession.Event{Closed: true, Reason: "command_unavailable"})
		return 1, errs.Wrap(errs.KindRequestUnavailable, err)
	}
	defer file.Close()
	if err := writer.send(terminalsession.Event{Ready: true}); err != nil {
		_ = terminateDescendants()
		_ = command.Wait()
		return 1, err
	}
	outputDone := make(chan struct{})
	go func() {
		defer close(outputDone)
		buffer := make([]byte, terminalsession.MaximumFrameBytes)
		for {
			count, readErr := file.Read(buffer)
			if count > 0 && writer.send(terminalsession.Event{Data: buffer[:count]}) != nil {
				return
			}
			if readErr != nil {
				return
			}
		}
	}()
	stop := make(chan string, 1)
	activity := make(chan struct{}, 1)
	go readInput(reader, file, stop, activity)
	waited := make(chan error, 1)
	go func() { waited <- command.Wait() }()
	timer := time.NewTimer(time.Duration(options.InputInactivitySeconds) * time.Second)
	defer timer.Stop()
	reason, exitCode := "command_exited", 0
	waitComplete := false
	running := true
	for running {
		select {
		case <-ctx.Done():
			reason, running = "executor_stopped", false
		case reason = <-stop:
			running = false
		case <-timer.C:
			reason, running = "input_inactivity", false
		case <-activity:
			timer.Reset(time.Duration(options.InputInactivitySeconds) * time.Second)
		case err := <-waited:
			waitComplete, running = true, false
			var exit *exec.ExitError
			if errors.As(err, &exit) {
				exitCode = exit.ExitCode()
			} else if err != nil {
				exitCode = 1
			}
		}
	}
	cleanupErr := terminateDescendants()
	_ = file.Close()
	if !waitComplete {
		select {
		case <-waited:
		case <-time.After(3 * time.Second):
			cleanupErr = errs.New(errs.KindRequestUnavailable, "container command did not stop")
		}
	}
	select {
	case <-outputDone:
	case <-time.After(time.Second):
	}
	if cleanupErr != nil {
		reason = "cleanup_failed"
	}
	finished := make(chan struct{})
	go func() {
		_ = writer.send(terminalsession.Event{Closed: true, Reason: reason, ExitCode: exitCode})
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(time.Second):
	}
	return exitCode, cleanupErr
}

func readInput(reader *bufio.Reader, file *os.File, stop chan<- string, activity chan<- struct{}) {
	finish := func(reason string) {
		select {
		case stop <- reason:
		default:
		}
	}
	for {
		var frame terminalsession.Input
		if readFrame(reader, &frame) != nil {
			finish("connection_closed")
			return
		}
		forms := 0
		if len(frame.Data) > 0 {
			forms++
		}
		if frame.Size != nil {
			forms++
		}
		if frame.Stop {
			forms++
		}
		if forms != 1 || len(frame.Data) > terminalsession.MaximumFrameBytes {
			finish("invalid_input")
			return
		}
		if frame.Stop {
			finish("terminated")
			return
		}
		if frame.Size != nil {
			if frame.Size.Validate() != nil ||
				pty.Setsize(file, &pty.Winsize{Cols: frame.Size.Columns, Rows: frame.Size.Rows}) != nil {
				finish("resize_failed")
				return
			}
			continue
		}
		if _, err := file.Write(frame.Data); err != nil {
			finish("input_failed")
			return
		}
		select {
		case activity <- struct{}{}:
		default:
		}
	}
}
