package runner

import (
	"errors"
	"fmt"
	"github.com/AlanD20/groundplane/pkg/errs"
	"io"
	"sync"
	"syscall"
)

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
