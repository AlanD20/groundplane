package postgres16execution

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash"
	"io"
	"time"

	"github.com/moby/moby/client"

	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const streamChunkBytes = 32 * 1024

type inputResult struct {
	evidence postgres16protocol.ConfinementStreamEvidence
	err      error
}

type outputResult struct {
	stdout postgres16protocol.ConfinementStreamEvidence
	stderr postgres16protocol.ConfinementStreamEvidence
	proof  []byte
	err    error
}

func (executor *Executor) attachAndInspect(
	ctx context.Context, expected Container, request postgres16protocol.Request,
	policy postgres16protocol.StreamPolicy, source io.ReadCloser, artifact io.WriteCloser,
	result Result,
) (Result, error) {
	attached, err := executor.engine.ExecAttach(ctx, result.ExecID, client.ExecAttachOptions{TTY: false})
	if err != nil {
		executor.attemptStop(expected.ID, request.Nonce)
		result.ExitCode = postgres16protocol.ExitRecoveryRequired
		return result, errs.New(errs.KindStateConflict, "managed PostgreSQL Exec start outcome is unknown")
	}
	if attached.Conn == nil || attached.Reader == nil {
		if attached.Conn != nil {
			attached.Close()
		}
		executor.attemptStop(expected.ID, request.Nonce)
		result.ExitCode = postgres16protocol.ExitRecoveryRequired
		return result, errs.New(errs.KindStateConflict, "managed PostgreSQL Exec transport is unavailable")
	}
	defer attached.Close()
	inputDone := make(chan inputResult, 1)
	outputDone := make(chan outputResult, 1)
	watchDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			attached.Close()
			if source != nil {
				_ = source.Close()
			}
			if artifact != nil {
				_ = artifact.Close()
			}
		case <-watchDone:
		}
	}()
	defer close(watchDone)
	go func() { inputDone <- pumpInput(&attached.HijackedResponse, source, request) }()
	go func() { outputDone <- readMultiplexed(attached.Reader, policy, artifact) }()
	var input inputResult
	var output outputResult
	inputReceived, outputReceived := false, false
	ctxDone := ctx.Done()
	for !inputReceived || !outputReceived {
		select {
		case input = <-inputDone:
			inputReceived = true
			if input.err != nil {
				attached.Close()
				if artifact != nil {
					_ = artifact.Close()
				}
			}
		case output = <-outputDone:
			outputReceived = true
			if output.err != nil {
				attached.Close()
				if source != nil {
					_ = source.Close()
				}
			}
		case <-ctxDone:
			ctxDone = nil
			attached.Close()
			if source != nil {
				_ = source.Close()
			}
			if artifact != nil {
				_ = artifact.Close()
			}
			// Closing the caller-owned ReadCloser/WriteCloser and Docker hijack
			// is the cancellation contract for both bounded pump goroutines.
		}
		if input.err != nil || output.err != nil {
			attached.Close()
			if source != nil {
				_ = source.Close()
			}
			if artifact != nil {
				_ = artifact.Close()
			}
		}
	}
	result.Stdin, result.Stdout, result.Stderr, result.Proof = input.evidence, output.stdout, output.stderr, output.proof
	if ctx.Err() != nil || input.err != nil || output.err != nil ||
		!input.evidence.EOF || !output.stdout.EOF || !output.stderr.EOF {
		executor.attemptStop(expected.ID, request.Nonce)
		result.ExitCode = postgres16protocol.ExitRecoveryRequired
		return result, errs.New(errs.KindStateConflict, "managed PostgreSQL Exec stream outcome is unknown")
	}
	inspected, code, err := executor.inspectExit(ctx, expected, result.ExecID)
	if err != nil {
		executor.attemptStop(expected.ID, request.Nonce)
		result.ExitCode = postgres16protocol.ExitRecoveryRequired
		return result, err
	}
	result.Inspect = inspected
	result.ExitCode = code
	if code != postgres16protocol.ExitSuccess {
		return result, errs.New(errs.KindRequestFailed, "managed PostgreSQL helper failed")
	}
	return result, nil
}

func pumpInput(
	attached *client.HijackedResponse, source io.ReadCloser, request postgres16protocol.Request,
) inputResult {
	var evidence postgres16protocol.ConfinementStreamEvidence
	if source == nil {
		evidence.SHA256 = sha256.Sum256(nil)
		evidence.EOF = true
		return inputResult{evidence: evidence}
	}
	closer, ok := attached.Conn.(interface{ CloseWrite() error })
	if !ok {
		return inputResult{err: errs.New(errs.KindInternal, "managed PostgreSQL Exec cannot close input")}
	}
	hasher := sha256.New()
	var chunk [streamChunkBytes]byte
	remaining := request.SourceSize
	for remaining > 0 {
		length := uint64(len(chunk))
		if remaining < length {
			length = remaining
		}
		n, err := io.ReadFull(source, chunk[:int(length)])
		if err != nil {
			return inputResult{err: errs.New(errs.KindStateConflict, "managed PostgreSQL input ended early")}
		}
		if err := writeAll(attached.Conn, chunk[:n]); err != nil {
			return inputResult{err: errs.Wrap(errs.KindInternal, err)}
		}
		_, _ = hasher.Write(chunk[:n])
		evidence.Bytes += uint64(n)
		remaining -= uint64(n)
	}
	var extra [1]byte
	n, err := source.Read(extra[:])
	if n != 0 || !errors.Is(err, io.EOF) {
		return inputResult{err: errs.New(errs.KindStateConflict, "managed PostgreSQL input length changed")}
	}
	copy(evidence.SHA256[:], hasher.Sum(nil))
	if evidence.SHA256 != request.SourceSHA256 {
		return inputResult{err: errs.New(errs.KindStateConflict, "managed PostgreSQL input digest changed")}
	}
	if err := closer.CloseWrite(); err != nil {
		return inputResult{err: errs.Wrap(errs.KindInternal, err)}
	}
	evidence.EOF = true
	return inputResult{evidence: evidence}
}

type boundedSink struct {
	limit   uint64
	written uint64
	hash    hash.Hash
	writer  io.Writer
	proof   []byte
	collect bool
}

func newBoundedSink(limit uint64, writer io.Writer, collect bool) *boundedSink {
	return &boundedSink{limit: limit, hash: sha256.New(), writer: writer, collect: collect}
}

func (sink *boundedSink) accept(reader io.Reader, length uint32) error {
	if uint64(length) > sink.limit-sink.written {
		return errs.New(errs.KindStateConflict, "managed PostgreSQL Exec stream exceeds limit")
	}
	var chunk [streamChunkBytes]byte
	remaining := uint64(length)
	for remaining > 0 {
		part := uint64(len(chunk))
		if remaining < part {
			part = remaining
		}
		if _, err := io.ReadFull(reader, chunk[:int(part)]); err != nil {
			return errs.New(errs.KindStateConflict, "managed PostgreSQL Exec stream ended inside a frame")
		}
		if sink.writer != nil {
			if err := writeAll(sink.writer, chunk[:int(part)]); err != nil {
				return errs.Wrap(errs.KindInternal, err)
			}
		}
		if sink.collect {
			sink.proof = append(sink.proof, chunk[:int(part)]...)
		}
		_, _ = sink.hash.Write(chunk[:int(part)])
		sink.written += part
		remaining -= part
	}
	return nil
}

func (sink *boundedSink) evidence() postgres16protocol.ConfinementStreamEvidence {
	var digest postgres16protocol.Digest
	copy(digest[:], sink.hash.Sum(nil))
	return postgres16protocol.ConfinementStreamEvidence{Bytes: sink.written, SHA256: digest, EOF: true}
}

// readMultiplexed parses Docker's eight-byte stream frames with a fixed buffer.
// stdcopy.StdCopy allocates the untrusted declared frame length before applying
// output limits, so this port intentionally does not use it.
func readMultiplexed(
	reader *bufio.Reader, policy postgres16protocol.StreamPolicy, artifact io.WriteCloser,
) outputResult {
	limit := policy.OutputLimit
	if limit == 0 {
		if policy.Output == postgres16protocol.OutputArtifact {
			limit = postgres16protocol.MaximumRestoreSourceBytes
		} else {
			limit = postgres16protocol.DiagnosticLimitBytes
		}
	}
	stdout := newBoundedSink(limit, artifact, policy.Output == postgres16protocol.OutputProof)
	stderr := newBoundedSink(policy.StderrLimit, nil, false)
	for {
		var header [8]byte
		n, err := io.ReadFull(reader, header[:])
		if errors.Is(err, io.EOF) && n == 0 {
			return outputResult{stdout: stdout.evidence(), stderr: stderr.evidence(), proof: stdout.proof}
		}
		if err != nil || header[1] != 0 || header[2] != 0 || header[3] != 0 {
			return outputResult{err: errs.New(errs.KindStateConflict, "managed PostgreSQL Exec frame is invalid")}
		}
		length := binary.BigEndian.Uint32(header[4:8])
		switch header[0] {
		case 1:
			err = stdout.accept(reader, length)
		case 2:
			err = stderr.accept(reader, length)
		default:
			err = errs.New(errs.KindStateConflict, "managed PostgreSQL Exec stream type is invalid")
		}
		if err != nil {
			return outputResult{err: err}
		}
	}
}

func writeAll(writer io.Writer, data []byte) error {
	for len(data) != 0 {
		count, err := writer.Write(data)
		if err != nil {
			return err
		}
		if count <= 0 || count > len(data) {
			return io.ErrShortWrite
		}
		data = data[count:]
	}
	return nil
}

func (executor *Executor) inspectExit(
	ctx context.Context, expected Container, execID string,
) (client.ExecInspectResult, postgres16protocol.ExitCode, error) {
	for {
		inspected, err := executor.engine.ExecInspect(ctx, execID, client.ExecInspectOptions{})
		if err != nil || inspected.ID != execID || inspected.ContainerID != expected.ID {
			return client.ExecInspectResult{}, postgres16protocol.ExitRecoveryRequired,
				errs.New(errs.KindStateConflict, "managed PostgreSQL Exec inspection is unavailable")
		}
		if !inspected.Running {
			if inspected.ExitCode < 0 {
				return client.ExecInspectResult{}, postgres16protocol.ExitRecoveryRequired,
					errs.New(errs.KindStateConflict, "managed PostgreSQL Exec termination is ambiguous")
			}
			code, err := postgres16protocol.ClassifyProcessExit(true, uint32(inspected.ExitCode))
			if err != nil || executor.attest(ctx, expected) != nil {
				return client.ExecInspectResult{}, postgres16protocol.ExitRecoveryRequired,
					errs.New(errs.KindStateConflict, "managed PostgreSQL Exec or container proof changed")
			}
			return inspected, code, nil
		}
		select {
		case <-ctx.Done():
			return client.ExecInspectResult{}, postgres16protocol.ExitRecoveryRequired, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// Stop is best effort and never upgrades an unknown result to success. It uses
// the helper's fixed stop operation against the same nonce and container ID.
func (executor *Executor) attemptStop(containerID string, nonce postgres16protocol.Nonce) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	request := postgres16protocol.Request{
		Operation: postgres16protocol.OperationStop,
		Nonce:     nonce, DeadlineUnixNano: uint64(time.Now().Add(5 * time.Second).UnixNano()),
	}
	command, err := request.DockerExecCommand()
	if err != nil {
		return
	}
	created, err := executor.engine.ExecCreate(ctx, containerID, client.ExecCreateOptions{
		User: "0:0", Env: postgres16protocol.Environment(), WorkingDir: "/", Cmd: command,
	})
	if err != nil || !validDockerID(created.ID) {
		return
	}
	_, _ = executor.engine.ExecStart(ctx, created.ID, client.ExecStartOptions{Detach: true})
}
