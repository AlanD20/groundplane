package runner

import (
	"bytes"
	"errors"
	"io"
	"sync"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	streamReadBytes        = 32 * 1024
	outputLimitErrorDetail = "runner: combined stdout and stderr exceeded capture limit"
)

type outputCapture struct {
	mu sync.Mutex

	limitBytes     int64
	retainedBytes  int64
	exceeded       bool
	exceededSignal chan struct{}
	stdout         bytes.Buffer
	stderr         bytes.Buffer
}

func newOutputCapture(limitBytes int64) (*outputCapture, error) {
	if limitBytes < 0 {
		return nil, errs.New(errs.KindValidationFailed, "runner: capture limit bytes must not be negative")
	}
	return &outputCapture{limitBytes: limitBytes, exceededSignal: make(chan struct{})}, nil
}

func (capture *outputCapture) write(stderr bool, payload []byte) int {
	capture.mu.Lock()
	retained := len(payload)
	if capture.limitBytes > 0 {
		remaining := capture.limitBytes - capture.retainedBytes
		if int64(retained) > remaining {
			retained = int(remaining)
		}
	}
	if stderr {
		_, _ = capture.stderr.Write(payload[:retained])
	} else {
		_, _ = capture.stdout.Write(payload[:retained])
	}
	capture.retainedBytes += int64(retained)
	if retained < len(payload) && !capture.exceeded {
		capture.exceeded = true
		close(capture.exceededSignal)
	}
	capture.mu.Unlock()
	return retained
}

func (capture *outputCapture) result(exitCode int) Result {
	capture.mu.Lock()
	defer capture.mu.Unlock()
	return Result{
		Stdout:   append([]byte(nil), capture.stdout.Bytes()...),
		Stderr:   append([]byte(nil), capture.stderr.Bytes()...),
		ExitCode: exitCode,
	}
}

func (capture *outputCapture) exceededLimit() bool {
	capture.mu.Lock()
	defer capture.mu.Unlock()
	return capture.exceeded
}

func (capture *outputCapture) resolveError(contextErr, processErr error) error {
	if capture.exceededLimit() {
		return outputLimitExceededError(processErr)
	}
	if contextErr != nil && processErr != nil {
		return contextErr
	}
	return subprocessOperationalError(processErr)
}

func outputLimitExceededError(processErr error) error {
	causes := []error{errors.New(outputLimitErrorDetail)}
	if processErr != nil {
		causes = append(causes, processErr)
	}
	return errs.Wrap(errs.KindInternal, errors.Join(causes...))
}

func subprocessOperationalError(cause error) error {
	if cause == nil {
		return nil
	}
	return errs.Wrap(errs.KindInternal, cause)
}

type streamedLine struct {
	stderr bool
	value  string
}

type streamResult struct {
	err error
}

func readStream(
	reader *ownedReadCloser,
	stderr bool,
	capture *outputCapture,
	lines chan<- streamedLine,
	results chan<- streamResult,
	requestInterrupt func(error),
	readers *sync.WaitGroup,
) {
	defer readers.Done()
	emitter := streamLineEmitter{stderr: stderr, lines: lines}
	buffer := make([]byte, streamReadBytes)
	var readErr error
	for {
		read, err := reader.Read(buffer)
		if read > 0 {
			retained := capture.write(stderr, buffer[:read])
			emitter.write(buffer[:retained])
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				readErr = err
			}
			break
		}
	}
	emitter.finish()
	readErr = errors.Join(readErr, reader.close())
	if readErr != nil {
		requestInterrupt(readErr)
	}
	results <- streamResult{err: readErr}
}

func readCapturedOutput(
	reader *ownedReadCloser,
	stderr bool,
	capture *outputCapture,
	requestInterrupt func(error),
	result chan<- error,
) {
	buffer := make([]byte, streamReadBytes)
	var readErr error
	for {
		read, err := reader.Read(buffer)
		if read > 0 {
			capture.write(stderr, buffer[:read])
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				readErr = err
			}
			break
		}
	}
	readErr = errors.Join(readErr, reader.close())
	if readErr != nil {
		requestInterrupt(readErr)
	}
	result <- readErr
}

type streamLineEmitter struct {
	stderr  bool
	lines   chan<- streamedLine
	pending []byte
}

func (emitter *streamLineEmitter) write(payload []byte) {
	for len(payload) > 0 {
		newline := bytes.IndexByte(payload, '\n')
		if newline < 0 {
			emitter.pending = append(emitter.pending, payload...)
			return
		}
		emitter.pending = append(emitter.pending, payload[:newline]...)
		emitter.emitPending()
		payload = payload[newline+1:]
	}
}

func (emitter *streamLineEmitter) finish() {
	if len(emitter.pending) > 0 {
		emitter.emitPending()
	}
}

func (emitter *streamLineEmitter) emitPending() {
	line := bytes.TrimSuffix(emitter.pending, []byte{'\r'})
	emitter.lines <- streamedLine{stderr: emitter.stderr, value: string(line)}
	emitter.pending = emitter.pending[:0]
}
