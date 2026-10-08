package mysql84execution

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash"
	"io"
	"time"

	"github.com/AlanD20/groundplane/internal/common/mysql84protocol"
	"github.com/moby/moby/client"
)

type streamPolicy struct {
	input  bool
	output bool
	proof  bool
	limit  uint64
}

func policyFor(request mysql84protocol.Request) streamPolicy {
	switch request.Operation {
	case mysql84protocol.OperationDump, mysql84protocol.OperationRecoverDump:
		return streamPolicy{output: true, limit: request.MaximumBytes}
	case mysql84protocol.OperationRestoreApply:
		return streamPolicy{input: true, limit: mysql84protocol.DiagnosticLimitBytes}
	case mysql84protocol.OperationServerVersion,
		mysql84protocol.OperationToolVersion,
		mysql84protocol.OperationRestoreToolVersion,
		mysql84protocol.OperationPostRestoreVerify,
		mysql84protocol.OperationRecoverEvidence,
		mysql84protocol.OperationRecoveryInventory:
		return streamPolicy{proof: true, limit: mysql84protocol.DiagnosticLimitBytes}
	default:
		return streamPolicy{limit: mysql84protocol.DiagnosticLimitBytes}
	}
}

type inputResult struct {
	evidence mysql84protocol.StreamEvidence
	err      error
}

type outputResult struct {
	stdout mysql84protocol.StreamEvidence
	stderr mysql84protocol.StreamEvidence
	proof  []byte
	err    error
}

func (executor *Executor) attachAndInspect(ctx context.Context, expected Container,
	request mysql84protocol.Request, source io.ReadCloser, artifact io.WriteCloser, result Result,
) (Result, error) {
	attached, err := executor.engine.ExecAttach(ctx, result.ExecID, client.ExecAttachOptions{TTY: false})
	if err != nil || attached.Conn == nil || attached.Reader == nil {
		if err == nil {
			attached.Close()
		}
		return result, invalidExecution("managed MySQL Exec start outcome is unknown")
	}
	defer attached.Close()
	policy := policyFor(request)
	inputDone, outputDone := make(chan inputResult, 1), make(chan outputResult, 1)
	go func() { inputDone <- pumpInput(&attached.HijackedResponse, source, request, policy) }()
	go func() { outputDone <- readMultiplexed(attached.Reader, policy, artifact) }()
	var input inputResult
	var output outputResult
	inputReceived, outputReceived := false, false
	for !inputReceived || !outputReceived {
		select {
		case input = <-inputDone:
			inputReceived = true
		case output = <-outputDone:
			outputReceived = true
		case <-ctx.Done():
			attached.Close()
			if source != nil {
				_ = source.Close()
			}
			if artifact != nil {
				_ = artifact.Close()
			}
		}
		if input.err != nil || output.err != nil {
			attached.Close()
		}
	}
	result.Stdin, result.Stdout, result.Stderr, result.Proof = input.evidence, output.stdout, output.stderr, output.proof
	if ctx.Err() != nil || input.err != nil || output.err != nil || !input.evidence.EOF ||
		!output.stdout.EOF || !output.stderr.EOF {
		return result, invalidExecution("managed MySQL Exec stream outcome is unknown")
	}
	inspected, err := executor.waitExit(ctx, expected, result.ExecID)
	if err != nil {
		return result, err
	}
	result.Inspect = inspected
	return result, nil
}

func pumpInput(attached *client.HijackedResponse, source io.ReadCloser, request mysql84protocol.Request,
	policy streamPolicy,
) inputResult {
	var evidence mysql84protocol.StreamEvidence
	if !policy.input {
		evidence.SHA256 = sha256.Sum256(nil)
		evidence.EOF = true
		return inputResult{evidence: evidence}
	}
	closer, ok := attached.Conn.(interface{ CloseWrite() error })
	if !ok || source == nil {
		return inputResult{err: invalidExecution("managed MySQL Exec input is unavailable")}
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
		if err != nil || writeAll(attached.Conn, chunk[:n]) != nil {
			return inputResult{err: invalidExecution("managed MySQL input changed")}
		}
		_, _ = hasher.Write(chunk[:n])
		evidence.Bytes += uint64(n)
		remaining -= uint64(n)
	}
	var extra [1]byte
	n, err := source.Read(extra[:])
	if n != 0 || !errors.Is(err, io.EOF) {
		return inputResult{err: invalidExecution("managed MySQL input length changed")}
	}
	copy(evidence.SHA256[:], hasher.Sum(nil))
	if evidence.SHA256 != request.SourceSHA256 || closer.CloseWrite() != nil {
		return inputResult{err: invalidExecution("managed MySQL input digest changed")}
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
}

func (sink *boundedSink) accept(reader io.Reader, length uint32) error {
	if uint64(length) > sink.limit-sink.written {
		return invalidExecution("managed MySQL Exec stream exceeds limit")
	}
	var chunk [streamChunkBytes]byte
	remaining := uint64(length)
	for remaining > 0 {
		part := uint64(len(chunk))
		if remaining < part {
			part = remaining
		}
		if _, err := io.ReadFull(reader, chunk[:int(part)]); err != nil {
			return invalidExecution("managed MySQL Exec stream ended inside a frame")
		}
		if sink.writer != nil && writeAll(sink.writer, chunk[:int(part)]) != nil {
			return invalidExecution("managed MySQL artifact write failed")
		}
		if sink.writer == nil {
			sink.proof = append(sink.proof, chunk[:int(part)]...)
		}
		_, _ = sink.hash.Write(chunk[:int(part)])
		sink.written += part
		remaining -= part
	}
	return nil
}

func (sink *boundedSink) evidence() mysql84protocol.StreamEvidence {
	var digest mysql84protocol.Digest
	copy(digest[:], sink.hash.Sum(nil))
	return mysql84protocol.StreamEvidence{Bytes: sink.written, SHA256: digest, EOF: true}
}

func readMultiplexed(reader *bufio.Reader, policy streamPolicy, artifact io.WriteCloser) outputResult {
	stdout := &boundedSink{limit: policy.limit, hash: sha256.New()}
	if policy.output {
		stdout.writer = artifact
	}
	stderr := &boundedSink{limit: mysql84protocol.DiagnosticLimitBytes, hash: sha256.New(), writer: io.Discard}
	for {
		var header [8]byte
		n, err := io.ReadFull(reader, header[:])
		if errors.Is(err, io.EOF) && n == 0 {
			return outputResult{stdout: stdout.evidence(), stderr: stderr.evidence(), proof: stdout.proof}
		}
		if err != nil || header[1] != 0 || header[2] != 0 || header[3] != 0 {
			return outputResult{err: invalidExecution("managed MySQL Exec frame is invalid")}
		}
		length := binary.BigEndian.Uint32(header[4:8])
		switch header[0] {
		case 1:
			err = stdout.accept(reader, length)
		case 2:
			err = stderr.accept(reader, length)
		default:
			err = invalidExecution("managed MySQL Exec stream type is invalid")
		}
		if err != nil {
			return outputResult{err: err}
		}
	}
}

func (executor *Executor) waitExit(
	ctx context.Context,
	expected Container,
	execID string,
) (client.ExecInspectResult, error) {
	for {
		inspected, err := executor.engine.ExecInspect(ctx, execID, client.ExecInspectOptions{})
		if err != nil || inspected.ID != execID || inspected.ContainerID != expected.ID {
			return client.ExecInspectResult{}, invalidExecution("managed MySQL Exec inspection is unavailable")
		}
		if !inspected.Running {
			if inspected.ExitCode != 0 || executor.attest(ctx, expected) != nil {
				return client.ExecInspectResult{}, invalidExecution("managed MySQL helper failed or container changed")
			}
			return inspected, nil
		}
		select {
		case <-ctx.Done():
			return client.ExecInspectResult{}, invalidExecution("managed MySQL Exec outcome is unknown")
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func writeAll(writer io.Writer, data []byte) error {
	for len(data) != 0 {
		count, err := writer.Write(data)
		if err != nil || count <= 0 || count > len(data) {
			return io.ErrShortWrite
		}
		data = data[count:]
	}
	return nil
}
