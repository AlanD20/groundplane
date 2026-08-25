package entrymaterialization

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: every hop streams bounded plaintext and closes its owned source;
// no full decoded payload is returned or allocated by the protocol.
func TestEncodeDecodeStreamsAndClearsOwnedSource(t *testing.T) {
	t.Parallel()

	input := bytes.Repeat([]byte("sensitive-material-"), (MaxChunkBytes/19)+2)
	spec := validHeaderSpec(OutputSecretFile, "secrets/tls/key.pem", 1000, 1001, ModePrivate)
	spec.Length = uint64(len(input))
	spec.Digest = DigestBytes(input)
	header, err := NewHeader(spec)
	if err != nil {
		t.Fatalf("NewHeader: %v", err)
	}
	limits := Limits{MaxContentBytes: uint64(len(input)), MaxDestinationBytes: 240}

	var frame bytes.Buffer
	if err := Encode(context.Background(), &frame, header, OwnBytes(input), limits); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	assertCleared(t, input)

	var output bytes.Buffer
	decodedHeader, err := Decode(
		context.Background(),
		io.NopCloser(bytes.NewReader(frame.Bytes())),
		limits,
		func(_ context.Context, received Header, content io.Reader) error {
			if received != header {
				t.Fatalf("decoded header = %#v, want %#v", received, header)
			}
			buffer := make([]byte, 1024)
			defer clear(buffer)
			_, copyErr := io.CopyBuffer(&output, content, buffer)
			return copyErr
		},
	)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if decodedHeader != header {
		t.Fatalf("decoded header = %#v, want %#v", decodedHeader, header)
	}
	if output.Len() != int(spec.Length) || DigestBytes(output.Bytes()) != spec.Digest {
		t.Fatal("decoded stream changed")
	}
}

// Rationale: plaintext source ownership ends on every encoder exit, including
// digest mismatch and downstream failure.
func TestEncodeClosesAndClearsSourceOnEveryFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		writer io.Writer
		value  []byte
	}{
		{name: "digest mismatch", writer: io.Discard, value: []byte("wrong material")},
		{name: "writer failure", writer: failingWriter{}, value: []byte("secret")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := test.value
			spec := validHeaderSpec(OutputSecretFile, "secrets/token", 1000, 1001, ModePrivate)
			header, err := NewHeader(spec)
			if err != nil {
				t.Fatalf("NewHeader: %v", err)
			}
			limits := Limits{MaxContentBytes: 64, MaxDestinationBytes: 240}
			if err := Encode(context.Background(), test.writer, header, OwnBytes(input), limits); err == nil {
				t.Fatal("Encode unexpectedly succeeded")
			}
			assertCleared(t, input)
		})
	}
}

// Rationale: cancellation must close an owned input so a blocked protocol read
// cannot leak a goroutine or retain plaintext.
func TestEncodeCancellationClosesBlockedSource(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	source := newBlockingReadCloser()
	spec := validHeaderSpec(OutputSecretFile, "secrets/token", 1000, 1001, ModePrivate)
	spec.Length = 1
	header, err := NewHeader(spec)
	if err != nil {
		t.Fatalf("NewHeader: %v", err)
	}
	result := make(chan error, 1)
	go func() {
		result <- Encode(ctx, io.Discard, header, source, Limits{MaxContentBytes: 1, MaxDestinationBytes: 240})
	}()
	<-source.started
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("Encode error = %v, want context canceled", err)
	}
	if !source.wasClosed() {
		t.Fatal("Encode did not close the blocked source")
	}
}

// Rationale: decoder cancellation owns the same close-to-unblock guarantee as
// encoder cancellation, including before the frame prelude arrives.
func TestDecodeCancellationClosesBlockedSource(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	source := newBlockingReadCloser()
	result := make(chan error, 1)
	go func() {
		_, err := Decode(
			ctx,
			source,
			testLimits(64),
			func(context.Context, Header, io.Reader) error { return nil },
		)
		result <- err
	}()
	<-source.started
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("Decode error = %v, want context canceled", err)
	}
	if !source.wasClosed() {
		t.Fatal("Decode did not close the blocked source")
	}
}

// Rationale: accepting ownership means every return after a non-nil source is
// supplied must close it exactly once, even before ordinary validation starts.
func TestEarlyValidationAlwaysClosesOwnedSourceExactlyOnce(t *testing.T) {
	t.Parallel()

	validSpec := validHeaderSpec(OutputSecretFile, "secrets/token", 1000, 1001, ModePrivate)
	validHeader, err := NewHeader(validSpec)
	if err != nil {
		t.Fatalf("NewHeader: %v", err)
	}
	validLimits := testLimits(64)

	encodeTests := []struct {
		name        string
		ctx         context.Context
		destination io.Writer
		header      Header
		limits      Limits
	}{
		{name: "nil context", ctx: nil, destination: io.Discard, header: validHeader, limits: validLimits},
		{name: "nil destination", ctx: context.Background(), header: validHeader, limits: validLimits},
		{name: "invalid limits", ctx: context.Background(), destination: io.Discard, header: validHeader},
		{name: "invalid header", ctx: context.Background(), destination: io.Discard, limits: validLimits},
	}
	for _, test := range encodeTests {
		t.Run("encode "+test.name, func(t *testing.T) {
			plaintext := []byte("secret")
			source := newTrackingReadCloser(plaintext, nil)
			if err := Encode(test.ctx, test.destination, test.header, source, test.limits); err == nil {
				t.Fatal("Encode unexpectedly succeeded")
			}
			if source.closeCount() != 1 {
				t.Fatalf("Close count = %d, want 1", source.closeCount())
			}
			assertCleared(t, plaintext)
		})
	}

	frame := canonicalFrame(t, []byte("secret"))
	decodeTests := []struct {
		name    string
		ctx     context.Context
		limits  Limits
		consume Consume
	}{
		{
			name:    "nil context",
			limits:  validLimits,
			consume: func(context.Context, Header, io.Reader) error { return nil },
		},
		{
			name: "nil consume",
			ctx:  context.Background(), limits: validLimits,
		},
		{
			name:    "invalid limits",
			ctx:     context.Background(),
			consume: func(context.Context, Header, io.Reader) error { return nil },
		},
	}
	for _, test := range decodeTests {
		t.Run("decode "+test.name, func(t *testing.T) {
			ownedFrame := bytes.Clone(frame)
			source := newTrackingReadCloser(ownedFrame, nil)
			if _, err := Decode(test.ctx, source, test.limits, test.consume); err == nil {
				t.Fatal("Decode unexpectedly succeeded")
			}
			if source.closeCount() != 1 {
				t.Fatalf("Close count = %d, want 1", source.closeCount())
			}
			assertCleared(t, ownedFrame)
		})
	}
}

// Rationale: failure to close a plaintext-bearing source is the cleanup result
// that must replace an earlier validation or callback error.
func TestSourceCloseFailureTakesPrecedence(t *testing.T) {
	t.Parallel()

	source := newTrackingReadCloser([]byte("secret"), io.ErrClosedPipe)
	//lint:ignore SA1012 This test verifies cleanup when the operation context is nil.
	err := Encode(nil, nil, Header{}, source, Limits{})
	if err == nil || !strings.Contains(err.Error(), "content source close failed") {
		t.Fatalf("Encode error = %v, want cleanup failure", err)
	}
	if source.closeCount() != 1 {
		t.Fatalf("Encode Close count = %d, want 1", source.closeCount())
	}

	frame := canonicalFrame(t, []byte("secret"))
	decodeSource := newTrackingReadCloser(bytes.Clone(frame), io.ErrClosedPipe)
	_, err = Decode(
		context.Background(),
		decodeSource,
		testLimits(64),
		func(context.Context, Header, io.Reader) error { return context.Canceled },
	)
	if err == nil || !strings.Contains(err.Error(), "content source close failed") || errors.Is(err, context.Canceled) {
		t.Fatalf("Decode error = %v, want cleanup failure precedence", err)
	}
	if decodeSource.closeCount() != 1 {
		t.Fatalf("Decode Close count = %d, want 1", decodeSource.closeCount())
	}
}

// Rationale: a consumer failure ends streaming but cannot transfer source
// cleanup responsibility back to the caller.
func TestDecodeConsumerFailureStillClosesOwnedSource(t *testing.T) {
	t.Parallel()

	frame := canonicalFrame(t, []byte("secret"))
	source := newTrackingReadCloser(bytes.Clone(frame), nil)
	_, err := Decode(
		context.Background(),
		source,
		testLimits(64),
		func(context.Context, Header, io.Reader) error { return context.Canceled },
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Decode error = %v, want callback error", err)
	}
	if source.closeCount() != 1 {
		t.Fatalf("Close count = %d, want 1", source.closeCount())
	}
}

// Rationale: hostile framing must fail closed before selecting metadata,
// accepting reordered bytes, or smuggling data after the verified stream.
func TestDecodeRejectsMalformedFramesWithSafeInternalErrors(t *testing.T) {
	t.Parallel()

	canonical := canonicalFrame(t, []byte("secret"))
	headerEnd := preludeBytes + recordHeaderBytes + int(binary.BigEndian.Uint32(canonical[preludeBytes+1:]))

	tests := []struct {
		name   string
		frame  func() []byte
		limits Limits
	}{
		{name: "bad magic", frame: func() []byte {
			value := bytes.Clone(canonical)
			value[0] ^= 0xff
			return value
		}, limits: testLimits(64)},
		{name: "unknown record", frame: func() []byte {
			value := bytes.Clone(canonical)
			value[preludeBytes] = 0xff
			return value
		}, limits: testLimits(64)},
		{name: "content before header", frame: func() []byte {
			return appendFramePrelude(nil, recordContent, []byte("x"))
		}, limits: testLimits(64)},
		{name: "duplicate header", frame: func() []byte {
			headerRecord := bytes.Clone(canonical[preludeBytes:headerEnd])
			value := bytes.Clone(canonical[:headerEnd])
			value = append(value, headerRecord...)
			return append(value, canonical[headerEnd:]...)
		}, limits: testLimits(64)},
		{name: "unknown header field", frame: func() []byte {
			return mutateHeaderPayload(canonical, func(payload []byte) []byte {
				return append(payload, 0xff, 0, 0, 0, 0)
			})
		}, limits: testLimits(64)},
		{name: "duplicate header field", frame: func() []byte {
			return mutateHeaderPayload(canonical, func(payload []byte) []byte {
				fieldBytes := 5 + int(binary.BigEndian.Uint32(payload[1:5]))
				return append(payload, payload[:fieldBytes]...)
			})
		}, limits: testLimits(64)},
		{name: "early eof", frame: func() []byte {
			return bytes.Clone(canonical[:len(canonical)-2])
		}, limits: testLimits(64)},
		{name: "extra bytes", frame: func() []byte {
			return append(bytes.Clone(canonical), 0x01)
		}, limits: testLimits(64)},
		{name: "out of order chunk", frame: func() []byte {
			value := bytes.Clone(canonical)
			binary.BigEndian.PutUint32(value[headerEnd+recordHeaderBytes:], 1)
			return value
		}, limits: testLimits(64)},
		{name: "digest mismatch", frame: func() []byte {
			value := bytes.Clone(canonical)
			value[headerEnd+recordHeaderBytes+chunkSequenceBytes] ^= 0xff
			return value
		}, limits: testLimits(64)},
		{name: "content over caller bound", frame: func() []byte {
			return bytes.Clone(canonical)
		}, limits: testLimits(5)},
		{name: "destination over caller bound", frame: func() []byte {
			return bytes.Clone(canonical)
		}, limits: Limits{MaxContentBytes: 64, MaxDestinationBytes: 5}},
		{name: "oversized chunk", frame: func() []byte {
			value := bytes.Clone(canonical)
			binary.BigEndian.PutUint32(value[headerEnd+1:], MaxChunkBytes+1)
			return value
		}, limits: testLimits(MaxChunkBytes + 1)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := Decode(
				context.Background(),
				io.NopCloser(bytes.NewReader(test.frame())),
				test.limits,
				func(_ context.Context, _ Header, content io.Reader) error {
					_, consumeErr := io.Copy(io.Discard, content)
					return consumeErr
				},
			)
			if err == nil {
				t.Fatal("Decode accepted malformed frame")
			}
			if kind, ok := errs.KindOf(err); !ok || kind != errs.KindInternal {
				t.Fatalf("error kind = %v, %t; want internal", kind, ok)
			}
			for _, forbidden := range []string{testTaskID, "secrets/token", "secret"} {
				if strings.Contains(err.Error(), forbidden) {
					t.Fatalf("error disclosed frame data: %v", err)
				}
			}
		})
	}
}

// Rationale: a consumer that does not read through authenticated EOF has not
// verified the frame and cannot report success.
func TestDecodeRejectsIncompleteConsumer(t *testing.T) {
	t.Parallel()

	frame := canonicalFrame(t, []byte("secret"))
	_, err := Decode(
		context.Background(),
		io.NopCloser(bytes.NewReader(frame)),
		testLimits(64),
		func(context.Context, Header, io.Reader) error { return nil },
	)
	if err == nil {
		t.Fatal("Decode accepted a consumer that did not verify EOF")
	}
}

// Rationale: callers may read less than one protocol chunk; bytes already
// copied out of the decoder must be erased before the next Read.
func TestDecodeClearsEachPartiallyReadPlaintextRange(t *testing.T) {
	t.Parallel()

	frame := canonicalFrame(t, []byte("sensitive"))
	_, err := Decode(
		context.Background(),
		OwnBytes(frame),
		testLimits(64),
		func(_ context.Context, _ Header, content io.Reader) error {
			reader, ok := content.(*contentReader)
			if !ok {
				t.Fatal("Decode did not provide the bounded content reader")
			}
			partial := make([]byte, 2)
			defer clear(partial)
			if _, readErr := io.ReadFull(content, partial); readErr != nil {
				return readErr
			}
			if reader.chunk[0] != 0 || reader.chunk[1] != 0 {
				t.Fatal("copied plaintext subrange remained in the decoder chunk")
			}
			if bytes.Count(reader.chunk[2:], []byte{0}) == len(reader.chunk[2:]) {
				t.Fatal("test did not observe the unread plaintext remainder")
			}
			_, copyErr := io.Copy(io.Discard, content)
			return copyErr
		},
	)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
}

// Rationale: a writer that reports failure because its context was canceled
// must preserve caller cancellation instead of becoming a protocol failure.
func TestEncodePreservesCancellationAfterBlockedWriterError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	writer := newCancelWrite(ctx, 5)
	plaintext := []byte("secret")
	source := newTrackingReadCloser(plaintext, nil)
	spec := validHeaderSpec(OutputSecretFile, "secrets/token", 1000, 1001, ModePrivate)
	header, err := NewHeader(spec)
	if err != nil {
		t.Fatalf("NewHeader: %v", err)
	}
	result := make(chan error, 1)
	go func() {
		result <- Encode(ctx, writer, header, source, testLimits(64))
	}()
	<-writer.started
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("Encode error = %v, want context canceled", err)
	}
	if source.closeCount() != 1 {
		t.Fatalf("Close count = %d, want 1", source.closeCount())
	}
	assertCleared(t, plaintext)
}

func canonicalFrame(t *testing.T, content []byte) []byte {
	t.Helper()
	spec := validHeaderSpec(OutputSecretFile, "secrets/token", 1000, 1001, ModePrivate)
	spec.Length = uint64(len(content))
	spec.Digest = DigestBytes(content)
	header, err := NewHeader(spec)
	if err != nil {
		t.Fatalf("NewHeader: %v", err)
	}
	var frame bytes.Buffer
	owned := bytes.Clone(content)
	if err := Encode(
		context.Background(),
		&frame,
		header,
		OwnBytes(owned),
		testLimits(uint64(len(content))),
	); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	return frame.Bytes()
}

func mutateHeaderPayload(frame []byte, mutate func([]byte) []byte) []byte {
	headerLength := int(binary.BigEndian.Uint32(frame[preludeBytes+1:]))
	headerStart := preludeBytes + recordHeaderBytes
	headerEnd := headerStart + headerLength
	payload := mutate(bytes.Clone(frame[headerStart:headerEnd]))
	result := bytes.Clone(frame[:preludeBytes])
	result = append(result, recordHeader)
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(payload)))
	result = append(result, length[:]...)
	result = append(result, payload...)
	return append(result, frame[headerEnd:]...)
}

func appendFramePrelude(target []byte, recordType byte, payload []byte) []byte {
	target = append(target, frameMagic...)
	target = append(target, frameVersion)
	target = append(target, recordType)
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(payload)))
	target = append(target, length[:]...)
	return append(target, payload...)
}

func testLimits(content uint64) Limits {
	return Limits{MaxContentBytes: content, MaxDestinationBytes: 240}
}

func assertCleared(t *testing.T, value []byte) {
	t.Helper()
	for index, current := range value {
		if current != 0 {
			t.Fatalf("byte %d was not cleared", index)
		}
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

type blockingReadCloser struct {
	started   chan struct{}
	closed    chan struct{}
	startOnce sync.Once
	closeOnce sync.Once
}

type trackingReadCloser struct {
	mu       sync.Mutex
	reader   *bytes.Reader
	value    []byte
	closes   int
	closeErr error
}

func newTrackingReadCloser(value []byte, closeErr error) *trackingReadCloser {
	return &trackingReadCloser{reader: bytes.NewReader(value), value: value, closeErr: closeErr}
}

func (source *trackingReadCloser) Read(target []byte) (int, error) {
	source.mu.Lock()
	defer source.mu.Unlock()
	return source.reader.Read(target)
}

func (source *trackingReadCloser) Close() error {
	source.mu.Lock()
	defer source.mu.Unlock()
	source.closes++
	clear(source.value)
	return source.closeErr
}

func (source *trackingReadCloser) closeCount() int {
	source.mu.Lock()
	defer source.mu.Unlock()
	return source.closes
}

func newBlockingReadCloser() *blockingReadCloser {
	return &blockingReadCloser{started: make(chan struct{}), closed: make(chan struct{})}
}

func (source *blockingReadCloser) Read([]byte) (int, error) {
	source.startOnce.Do(func() { close(source.started) })
	<-source.closed
	return 0, io.ErrClosedPipe
}

func (source *blockingReadCloser) Close() error {
	source.closeOnce.Do(func() { close(source.closed) })
	return nil
}

func (source *blockingReadCloser) wasClosed() bool {
	select {
	case <-source.closed:
		return true
	default:
		return false
	}
}
