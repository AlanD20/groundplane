package age

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: Backup artifacts are arbitrary binary streams, including NUL
// bytes, and must round-trip without forcing the source into one buffer.
func TestEncryptStreamRoundTripsLargeBinary(t *testing.T) {
	t.Parallel()
	keypair, err := GenerateKeypair()
	if err != nil {
		t.Fatalf("GenerateKeypair() error = %v", err)
	}
	plaintext := make([]byte, 3<<20+37)
	for index := range plaintext {
		plaintext[index] = byte(index*31 + 7)
	}
	plaintext[0] = 0
	plaintext[len(plaintext)/2] = 0
	plaintext[len(plaintext)-1] = 0
	defer clear(plaintext)

	reader := &boundedReader{reader: bytes.NewReader(plaintext), maxRead: encryptionStreamBufferSize}
	var encrypted bytes.Buffer
	if err := encryptStreamNonClosing(context.Background(), keypair.Recipient, reader, &encrypted); err != nil {
		t.Fatalf("EncryptStream() error = %v", err)
	}
	if reader.largestRead > encryptionStreamBufferSize {
		t.Fatalf("EncryptStream() read %d bytes at once, want <= %d", reader.largestRead, encryptionStreamBufferSize)
	}
	revealed, err := Decrypt(keypair.Identity, encrypted.Bytes())
	if err != nil {
		t.Fatalf("Decrypt() error = %v", err)
	}
	defer clear(revealed)
	if !bytes.Equal(revealed, plaintext) {
		t.Fatal("Decrypt() did not preserve exact binary stream")
	}
}

// Rationale: recipient parsing is the public encryption authority; malformed
// input is validation, and its parser detail must not enter the public error.
func TestEncryptStreamRejectsInvalidRecipientWithoutLeakingData(t *testing.T) {
	t.Parallel()
	const marker = "plaintext-and-identity-marker"
	err := encryptStreamNonClosing(
		context.Background(),
		"not-an-age-recipient-"+marker,
		strings.NewReader(marker),
		io.Discard,
	)
	if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("EncryptStream() error = %v, want validation failure", err)
	}
	var domainError *errs.Error
	if !errors.As(err, &domainError) {
		t.Fatalf("EncryptStream() error type = %T, want *errs.Error", err)
	}
	problem := domainError.ToProblem()
	if strings.Contains(problem.Detail, marker) || strings.Contains(problem.Detail, "not-an-age-recipient") {
		t.Fatalf("public problem leaked input: %#v", problem)
	}
}

// Rationale: a point encrypted for another environment key must not decrypt
// under this environment identity, even though both keys are valid X25519.
func TestEncryptStreamRejectsMismatchedIdentity(t *testing.T) {
	t.Parallel()
	owner, err := GenerateKeypair()
	if err != nil {
		t.Fatalf("GenerateKeypair(owner) error = %v", err)
	}
	other, err := GenerateKeypair()
	if err != nil {
		t.Fatalf("GenerateKeypair(other) error = %v", err)
	}
	var encrypted bytes.Buffer
	if err := encryptStreamNonClosing(context.Background(), owner.Recipient, strings.NewReader("artifact"), &encrypted); err != nil {
		t.Fatalf("EncryptStream() error = %v", err)
	}
	_, err = Decrypt(other.Identity, encrypted.Bytes())
	if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("Decrypt(mismatched identity) error = %v, want validation failure", err)
	}
}

// Rationale: source failures remain typed internal failures while their
// private cause is retained only for errors.Is diagnostics, not public detail.
func TestEncryptStreamPropagatesReadFailureSafely(t *testing.T) {
	t.Parallel()
	keypair, err := GenerateKeypair()
	if err != nil {
		t.Fatalf("GenerateKeypair() error = %v", err)
	}
	readErr := errors.New("reader failure: plaintext-and-identity-marker")
	_, err = streamToBuffer(keypair.Recipient, &failingReader{err: readErr})
	if !errors.Is(err, readErr) {
		t.Fatalf("EncryptStream() error = %v, want source failure", err)
	}
	assertInternalProblemDoesNotContain(t, err, "plaintext-and-identity-marker")
}

// Rationale: destination failures are not validation failures and must not
// cause EncryptStream to close the caller-owned destination.
func TestEncryptStreamPropagatesWriterFailureAndPreservesOwnership(t *testing.T) {
	t.Parallel()
	keypair, err := GenerateKeypair()
	if err != nil {
		t.Fatalf("GenerateKeypair() error = %v", err)
	}
	writeErr := errors.New("writer failure: plaintext-and-identity-marker")
	writer := &failingWriter{err: writeErr}
	err = encryptStreamNonClosing(context.Background(), keypair.Recipient, strings.NewReader("artifact"), writer)
	if !errors.Is(err, writeErr) {
		t.Fatalf("EncryptStream() error = %v, want destination failure", err)
	}
	if writer.closed {
		t.Fatal("EncryptStream() closed caller-owned destination")
	}
	assertInternalProblemDoesNotContain(t, err, "plaintext-and-identity-marker")
}

// Rationale: cancellation must stop a stream before consuming its next input
// chunk and must be returned directly rather than classified as bad input.
func TestEncryptStreamPropagatesCancellation(t *testing.T) {
	t.Parallel()
	keypair, err := GenerateKeypair()
	if err != nil {
		t.Fatalf("GenerateKeypair() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	reader := &countingReader{reader: strings.NewReader("artifact")}
	var encrypted bytes.Buffer
	err = encryptStreamNonClosing(ctx, keypair.Recipient, reader, &encrypted)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("EncryptStream() error = %v, want context.Canceled", err)
	}
	if reader.reads != 0 || encrypted.Len() != 0 {
		t.Fatalf("canceled EncryptStream() consumed or wrote data: reads=%d bytes=%d", reader.reads, encrypted.Len())
	}
}

// Rationale: neither source nor destination lifetime belongs to this helper;
// the caller controls cleanup even after successful finalization.
func TestEncryptStreamDoesNotCloseCallerOwnedInputOrOutput(t *testing.T) {
	t.Parallel()
	keypair, err := GenerateKeypair()
	if err != nil {
		t.Fatalf("GenerateKeypair() error = %v", err)
	}
	reader := &closableReader{reader: strings.NewReader("artifact")}
	writer := &closableWriter{}
	if err := encryptStreamNonClosing(context.Background(), keypair.Recipient, reader, writer); err != nil {
		t.Fatalf("EncryptStream() error = %v", err)
	}
	if reader.closed || writer.closed {
		t.Fatal("EncryptStream() closed caller-owned stream")
	}
}

// Rationale: recipient records are canonical authority values; accepting
// whitespace-normalized variants would make distinct stored bytes equivalent.
func TestEncryptStreamRequiresExactCanonicalRecipient(t *testing.T) {
	t.Parallel()
	keypair, err := GenerateKeypair()
	if err != nil {
		t.Fatalf("GenerateKeypair() error = %v", err)
	}
	err = encryptStreamNonClosing(
		context.Background(), keypair.Recipient+" ", strings.NewReader("artifact"), io.Discard,
	)
	if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("encryptStreamNonClosing(whitespace recipient) error = %v, want validation failure", err)
	}
}

// Rationale: age's encrypt writer ignores destination byte counts, so this
// boundary must convert a nil short write into io.ErrShortWrite itself.
func TestEncryptStreamRejectsNilShortWrite(t *testing.T) {
	t.Parallel()
	keypair, err := GenerateKeypair()
	if err != nil {
		t.Fatalf("GenerateKeypair() error = %v", err)
	}
	err = encryptStreamNonClosing(
		context.Background(), keypair.Recipient, strings.NewReader("artifact"), shortWriter{},
	)
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("encryptStreamNonClosing(short write) error = %v, want io.ErrShortWrite", err)
	}
	assertInternalProblemDoesNotContain(t, err, "artifact")
}

// Rationale: dependency-owned cancellation has unknown provenance and private
// text, so only cancellation present on the caller context may cross directly.
func TestEncryptStreamSanitizesWrappedDependencyCancellation(t *testing.T) {
	t.Parallel()
	keypair, err := GenerateKeypair()
	if err != nil {
		t.Fatalf("GenerateKeypair() error = %v", err)
	}
	const marker = "private-reader-cancellation-marker"
	wrapped := fmt.Errorf("%s: %w", marker, context.Canceled)
	_, err = streamToBuffer(keypair.Recipient, &failingReader{err: wrapped})
	if errors.Is(err, context.Canceled) {
		t.Fatalf("dependency cancellation remained publicly classifiable: %v", err)
	}
	assertInternalProblemDoesNotContain(t, err, marker)
}

// Rationale: an explicit interrupt makes context cancellation honest for an
// otherwise blocked Read and EncryptStream must join its watcher before return.
func TestEncryptStreamInterruptsBlockedReadOnCancellation(t *testing.T) {
	keypair, err := GenerateKeypair()
	if err != nil {
		t.Fatalf("GenerateKeypair() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	reader := newBlockingReader()
	result := make(chan error, 1)
	go func() {
		result <- EncryptStream(ctx, keypair.Recipient, reader, io.Discard, reader.Interrupt)
	}()
	waitSignal(t, reader.started, "EncryptStream did not enter blocked Read")
	cancel()
	if err := waitResult(t, result); !errors.Is(err, context.Canceled) {
		t.Fatalf("EncryptStream(blocked Read) error = %v, want context.Canceled", err)
	}
	if !reader.interrupted {
		t.Fatal("EncryptStream returned before the read interrupt completed")
	}
}

// Rationale: output blockage can begin while age writes its header, before
// plaintext copying, and must still be interruptible without an abandoned goroutine.
func TestEncryptStreamInterruptsBlockedWriteOnCancellation(t *testing.T) {
	keypair, err := GenerateKeypair()
	if err != nil {
		t.Fatalf("GenerateKeypair() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	writer := newBlockingWriter()
	result := make(chan error, 1)
	go func() {
		result <- EncryptStream(ctx, keypair.Recipient, strings.NewReader("artifact"), writer, writer.Interrupt)
	}()
	waitSignal(t, writer.started, "EncryptStream did not enter blocked Write")
	cancel()
	if err := waitResult(t, result); !errors.Is(err, context.Canceled) {
		t.Fatalf("EncryptStream(blocked Write) error = %v, want context.Canceled", err)
	}
	if !writer.interrupted {
		t.Fatal("EncryptStream returned before the write interrupt completed")
	}
}

// Rationale: finalization is best-effort after a copy failure, but the first
// copy failure remains the actionable cause if finalization also fails.
func TestEncryptStreamCopyFailurePrecedesFinalizationFailure(t *testing.T) {
	t.Parallel()
	keypair, err := GenerateKeypair()
	if err != nil {
		t.Fatalf("GenerateKeypair() error = %v", err)
	}
	copyErr := errors.New("private-copy-failure")
	finalizeErr := errors.New("private-finalization-failure")
	output := &finalizationFailWriter{err: finalizeErr}
	input := &armFinalizationFailureReader{output: output, err: copyErr}
	err = encryptStreamNonClosing(context.Background(), keypair.Recipient, input, output)
	if !errors.Is(err, copyErr) || errors.Is(err, finalizeErr) {
		t.Fatalf("combined error = %v, want only copy failure as primary cause", err)
	}
	if !output.failed {
		t.Fatal("EncryptStream did not attempt finalization after copy failure")
	}
	assertInternalProblemDoesNotContain(t, err, "private-copy-failure")
}

func streamToBuffer(recipient string, reader io.Reader) ([]byte, error) {
	var encrypted bytes.Buffer
	err := encryptStreamNonClosing(context.Background(), recipient, reader, &encrypted)
	return encrypted.Bytes(), err
}

func assertInternalProblemDoesNotContain(t *testing.T, err error, marker string) {
	t.Helper()
	kind, ok := errs.KindOf(err)
	if !ok || kind != errs.KindInternal {
		t.Fatalf("KindOf() = %d, %t; error = %v, want internal", kind, ok, err)
	}
	var domainError *errs.Error
	if !errors.As(err, &domainError) {
		t.Fatalf("error type = %T, want *errs.Error", err)
	}
	problem := domainError.ToProblem()
	if problem.Title != "Internal Server Error" || problem.Detail != "Internal Server Error" ||
		strings.Contains(problem.Detail, marker) {
		t.Fatalf("public problem leaked private cause: %#v", problem)
	}
}

type boundedReader struct {
	reader      io.Reader
	maxRead     int
	largestRead int
}

func (reader *boundedReader) Read(p []byte) (int, error) {
	if len(p) > reader.maxRead {
		return 0, errors.New("reader requested an unbounded buffer")
	}
	if len(p) > reader.largestRead {
		reader.largestRead = len(p)
	}
	return reader.reader.Read(p)
}

type failingReader struct{ err error }

func (reader *failingReader) Read([]byte) (int, error) { return 0, reader.err }

type failingWriter struct {
	closed bool
	err    error
}

func (writer *failingWriter) Write([]byte) (int, error) { return 0, writer.err }
func (writer *failingWriter) Close() error {
	writer.closed = true
	return nil
}

type countingReader struct {
	reader io.Reader
	reads  int
}

func (reader *countingReader) Read(p []byte) (int, error) {
	reader.reads++
	return reader.reader.Read(p)
}

type closableReader struct {
	reader io.Reader
	closed bool
}

func (reader *closableReader) Read(p []byte) (int, error) { return reader.reader.Read(p) }
func (reader *closableReader) Close() error {
	reader.closed = true
	return nil
}

type closableWriter struct {
	bytes.Buffer
	closed bool
}

func (writer *closableWriter) Close() error {
	writer.closed = true
	return nil
}

type shortWriter struct{}

func (shortWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	return len(p) - 1, nil
}

type blockingReader struct {
	started     chan struct{}
	release     chan struct{}
	interrupted bool
}

func newBlockingReader() *blockingReader {
	return &blockingReader{started: make(chan struct{}), release: make(chan struct{})}
}

func (reader *blockingReader) Read([]byte) (int, error) {
	select {
	case <-reader.started:
	default:
		close(reader.started)
	}
	<-reader.release
	return 0, io.ErrClosedPipe
}

func (reader *blockingReader) Interrupt() {
	reader.interrupted = true
	close(reader.release)
}

type blockingWriter struct {
	started     chan struct{}
	release     chan struct{}
	interrupted bool
}

func newBlockingWriter() *blockingWriter {
	return &blockingWriter{started: make(chan struct{}), release: make(chan struct{})}
}

func (writer *blockingWriter) Write([]byte) (int, error) {
	select {
	case <-writer.started:
	default:
		close(writer.started)
	}
	<-writer.release
	return 0, io.ErrClosedPipe
}

func (writer *blockingWriter) Interrupt() {
	writer.interrupted = true
	close(writer.release)
}

type finalizationFailWriter struct {
	bytes.Buffer
	armed  bool
	failed bool
	err    error
}

func (writer *finalizationFailWriter) Write(p []byte) (int, error) {
	if writer.armed {
		writer.failed = true
		return 0, writer.err
	}
	return writer.Buffer.Write(p)
}

type armFinalizationFailureReader struct {
	output *finalizationFailWriter
	err    error
	read   bool
}

func (reader *armFinalizationFailureReader) Read(p []byte) (int, error) {
	if !reader.read {
		reader.read = true
		return copy(p, "artifact"), nil
	}
	reader.output.armed = true
	return 0, reader.err
}

func waitSignal(t *testing.T, signal <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatal(message)
	}
}

func waitResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(time.Second):
		t.Fatal("EncryptStream did not return")
		return nil
	}
}
