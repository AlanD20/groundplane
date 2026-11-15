package containerterminal

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/AlanD20/groundplane/internal/common/terminalsession"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/client"
)

type process struct {
	attached   client.ExecAttachResult
	inputMu    sync.Mutex
	encoder    *json.Encoder
	output     *io.PipeReader
	writer     *io.PipeWriter
	ready      chan error
	done       chan struct{}
	finishOnce sync.Once
	stopOnce   sync.Once
	stopping   atomic.Bool
	err        error
	result     terminalsession.Exit
}

func openProcess(
	ctx context.Context,
	attached client.ExecAttachResult,
	options terminalsession.ContainerOptions,
) (terminalsession.Process, error) {
	reader, writer := io.Pipe()
	p := &process{attached: attached, encoder: json.NewEncoder(attached.Conn), output: reader, writer: writer,
		ready: make(chan error, 1), done: make(chan struct{})}
	framesIn, framesOut := io.Pipe()
	go func() {
		_, err := stdcopy.StdCopy(framesOut, io.Discard, attached.Reader)
		_ = framesOut.CloseWithError(err)
	}()
	go p.decode(framesIn)
	if err := attached.Conn.SetWriteDeadline(time.Now().Add(3 * time.Second)); err != nil {
		p.finish(errs.Wrap(errs.KindRequestUnavailable, err))
		return nil, p.err
	}
	if err := p.encoder.Encode(options); err != nil {
		p.finish(errs.Wrap(errs.KindRequestUnavailable, err))
		return nil, p.err
	}
	timer := time.NewTimer(15 * time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		p.finish(ctx.Err())
		return nil, ctx.Err()
	case <-timer.C:
		err := errs.New(errs.KindRequestUnavailable, "container command did not acknowledge startup")
		p.finish(err)
		return nil, err
	case err := <-p.ready:
		if err != nil {
			return nil, err
		}
	}
	go func() {
		select {
		case <-ctx.Done():
			_ = p.Terminate()
		case <-p.done:
		}
	}()
	return p, nil
}

func (p *process) decode(input *io.PipeReader) {
	defer input.Close()
	reader := bufio.NewReaderSize(input, 128<<10)
	ready := false
	for {
		line, err := reader.ReadSlice('\n')
		if err != nil {
			p.finish(errs.New(errs.KindRequestUnavailable, "container terminal transport ended before completion"))
			return
		}
		var event terminalsession.Event
		decoder := json.NewDecoder(strings.NewReader(string(line)))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&event) != nil || len(event.Data) > terminalsession.MaximumFrameBytes {
			p.finish(errs.New(errs.KindRequestUnavailable, "container terminal returned an invalid frame"))
			return
		}
		if event.Ready && !ready && !event.Closed && len(event.Data) == 0 {
			ready = true
			p.ready <- nil
			continue
		}
		if event.Closed && !event.Ready && len(event.Data) == 0 {
			var err error
			if !ready {
				err = terminalStartupError(event.Reason)
			} else if event.Reason == "cleanup_failed" {
				err = errs.New(errs.KindRequestUnavailable, "container terminal process cleanup could not be verified")
			}
			p.finishResult(err, terminalsession.Exit{Reason: event.Reason, Code: event.ExitCode})
			return
		}
		if !ready || event.Ready || len(event.Data) == 0 {
			p.finish(errs.New(errs.KindRequestUnavailable, "container terminal frame is out of order"))
			return
		}
		if _, err := p.writer.Write(event.Data); err != nil && !p.stopping.Load() {
			p.finish(errs.Wrap(errs.KindRequestUnavailable, err))
			return
		}
	}
}

func terminalStartupError(reason string) error {
	detail := "Container command could not start; check executable, user and working directory"
	switch reason {
	case "user_unavailable":
		detail = "Selected container user or group does not exist"
	case "supervision_unavailable":
		detail = "Container terminal process supervision is unavailable"
	case "invalid_options":
		detail = "Container terminal options were rejected"
	}
	return errs.New(errs.KindRequestUnavailable, detail)
}

func (p *process) finish(err error) {
	p.finishResult(err, terminalsession.Exit{Reason: "transport_failed", Code: -1})
}

func (p *process) finishResult(err error, result terminalsession.Exit) {
	p.finishOnce.Do(func() {
		p.err = err
		p.result = result
		select {
		case p.ready <- err:
		default:
		}
		_ = p.writer.CloseWithError(err)
		p.attached.Close()
		close(p.done)
	})
}

func (p *process) Read(data []byte) (int, error) { return p.output.Read(data) }

func (p *process) send(frame terminalsession.Input) error {
	p.inputMu.Lock()
	defer p.inputMu.Unlock()
	if err := p.attached.Conn.SetWriteDeadline(time.Now().Add(3 * time.Second)); err != nil {
		return errs.Wrap(errs.KindRequestUnavailable, err)
	}
	if err := p.encoder.Encode(frame); err != nil {
		return errs.Wrap(errs.KindRequestUnavailable, err)
	}
	return nil
}

func (p *process) Write(data []byte) (int, error) {
	if len(data) > terminalsession.MaximumFrameBytes {
		return 0, errs.New(errs.KindValidationFailed, "terminal input is too large")
	}
	if err := p.send(terminalsession.Input{Data: data}); err != nil {
		return 0, err
	}
	return len(data), nil
}

func (p *process) Resize(size terminalsession.Size) error {
	if err := size.Validate(); err != nil {
		return err
	}
	return p.send(terminalsession.Input{Size: &size})
}

func (p *process) Wait() error { <-p.done; return p.err }

func (p *process) Result() terminalsession.Exit { <-p.done; return p.result }

func (p *process) Terminate() error {
	p.stopOnce.Do(func() {
		p.stopping.Store(true)
		_ = p.output.Close()
		select {
		case <-p.done:
			return
		default:
		}
		if err := p.send(terminalsession.Input{Stop: true}); err != nil {
			p.finish(err)
			return
		}
		select {
		case <-p.done:
		case <-time.After(5 * time.Second):
			p.finish(errs.New(errs.KindRequestUnavailable, "container shell termination was not acknowledged"))
		}
	})
	return p.Wait()
}
