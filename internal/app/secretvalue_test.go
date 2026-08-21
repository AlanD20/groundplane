package app

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: application composition must preserve caller ownership while the
// Protector clears every transient buffer exchanged with the root age key.
func TestSecretValueProtectorAdaptsControllerKeyWithoutSharingBuffers(t *testing.T) {
	t.Parallel()

	keyCiphertext := []byte("age-ciphertext")
	keyPlaintext := []byte("environment-secret")
	key := &observingSecretValueKey{
		wrapOutput:   keyCiphertext,
		unwrapOutput: keyPlaintext,
	}
	protector := mustSecretValueProtector(t, key)
	callerPlaintext := []byte("environment-secret")

	envelope, err := protector.Seal(context.Background(), callerPlaintext)
	if err != nil {
		t.Fatalf("Seal() error = %v", err)
	}
	if string(callerPlaintext) != "environment-secret" {
		t.Fatalf("caller plaintext = %q, want unchanged", callerPlaintext)
	}
	if !allBytesZero(key.wrapInput) {
		t.Fatal("Seal() did not clear the plaintext passed to ControllerKey.Wrap")
	}
	if !allBytesZero(keyCiphertext) {
		t.Fatal("Seal() did not clear ciphertext returned by ControllerKey.Wrap")
	}
	if got := string(envelope.Ciphertext()); got != "age-ciphertext" {
		t.Fatalf("Envelope ciphertext = %q, want age-ciphertext", got)
	}

	var callbackPlaintext []byte
	var observed string
	err = protector.Open(context.Background(), envelope, func(plaintext []byte) error {
		callbackPlaintext = plaintext
		observed = string(plaintext)
		return nil
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if observed != "environment-secret" {
		t.Fatalf("callback plaintext = %q, want environment-secret", observed)
	}
	if !allBytesZero(key.unwrapInput) {
		t.Fatal("Open() did not clear the ciphertext passed to ControllerKey.Unwrap")
	}
	if !allBytesZero(keyPlaintext) {
		t.Fatal("Open() did not clear plaintext returned by ControllerKey.Unwrap")
	}
	if !allBytesZero(callbackPlaintext) {
		t.Fatal("Open() did not clear callback plaintext")
	}
	if got := string(envelope.Ciphertext()); got != "age-ciphertext" {
		t.Fatalf("Open() mutated envelope ciphertext: %q", got)
	}
}

// Rationale: Sealer and Opener own their contracts independently of Protector;
// the age key must never receive caller storage or retain uncleared output.
func TestSecretValueCipherDefensivelyOwnsAndClearsBuffers(t *testing.T) {
	t.Parallel()

	keyCiphertext := []byte("age-ciphertext")
	keyPlaintext := []byte("environment-secret")
	key := &observingSecretValueKey{
		wrapOutput:   keyCiphertext,
		unwrapOutput: keyPlaintext,
	}
	cipher := &secretValueCipher{key: key}
	callerPlaintext := []byte("environment-secret")

	ciphertext, err := cipher.Seal(context.Background(), callerPlaintext)
	if err != nil {
		t.Fatalf("Seal() error = %v", err)
	}
	if string(callerPlaintext) != "environment-secret" {
		t.Fatalf("Seal() mutated caller plaintext: %q", callerPlaintext)
	}
	if !allBytesZero(key.wrapInput) {
		t.Fatal("Seal() did not clear its private ControllerKey.Wrap input")
	}
	if !allBytesZero(keyCiphertext) {
		t.Fatal("Seal() did not clear ControllerKey.Wrap output")
	}
	if string(ciphertext) != "age-ciphertext" {
		t.Fatalf("Seal() ciphertext = %q, want age-ciphertext", ciphertext)
	}

	callerCiphertext := append([]byte(nil), ciphertext...)
	plaintext, err := cipher.Open(context.Background(), callerCiphertext)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if string(callerCiphertext) != "age-ciphertext" {
		t.Fatalf("Open() mutated caller ciphertext: %q", callerCiphertext)
	}
	if !allBytesZero(key.unwrapInput) {
		t.Fatal("Open() did not clear its private ControllerKey.Unwrap input")
	}
	if !allBytesZero(keyPlaintext) {
		t.Fatal("Open() did not clear ControllerKey.Unwrap output")
	}
	if string(plaintext) != "environment-secret" {
		t.Fatalf("Open() plaintext = %q, want environment-secret", plaintext)
	}
}

// Rationale: cancellation must remain recognizable and must win over a
// simultaneous cryptographic failure without retaining partial key output.
func TestSecretValueProtectorClassifiesCancellation(t *testing.T) {
	t.Parallel()

	key := &observingSecretValueKey{wrapOutput: []byte("must-be-cleared")}
	protector := mustSecretValueProtector(t, key)
	preCanceled, cancelBefore := context.WithCancel(context.Background())
	cancelBefore()
	if _, err := protector.Seal(preCanceled, []byte("ignored")); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled Seal() error = %v, want context.Canceled", err)
	}
	if key.wrapCalls != 0 {
		t.Fatalf("ControllerKey.Wrap calls = %d, want zero", key.wrapCalls)
	}

	duringCall, cancelDuring := context.WithCancel(context.Background())
	key.onWrap = cancelDuring
	key.wrapErr = errors.New("secret-bearing failure")
	_, err := protector.Seal(duringCall, []byte("secret-bearing value"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("in-flight canceled Seal() error = %v, want context.Canceled", err)
	}
	if !allBytesZero(key.wrapOutput) {
		t.Fatal("canceled Seal() did not clear partial ControllerKey.Wrap output")
	}
}

// Rationale: only cancellation owned by the task context crosses this adapter;
// dependency-returned context sentinels under a live context are private faults.
func TestSecretValueCipherUsesOnlyTaskContextCancellation(t *testing.T) {
	t.Parallel()

	key := &observingSecretValueKey{
		wrapOutput:   []byte("partial-ciphertext"),
		wrapErr:      context.Canceled,
		unwrapOutput: []byte("partial-plaintext"),
		unwrapErr:    context.DeadlineExceeded,
	}
	cipher := &secretValueCipher{key: key}
	if _, err := cipher.Seal(context.Background(), []byte("secret")); err == nil ||
		errors.Is(err, context.Canceled) {
		t.Fatalf("Seal() error = %v, want closed internal dependency failure", err)
	} else {
		assertSecretValueInternalWithoutText(t, err, "")
	}
	if !allBytesZero(key.wrapOutput) {
		t.Fatal("Seal() did not clear partial dependency output")
	}

	if _, err := cipher.Open(context.Background(), []byte("ciphertext")); err == nil ||
		errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Open() error = %v, want closed internal dependency failure", err)
	} else {
		assertSecretValueInternalWithoutText(t, err, "")
	}
	if !allBytesZero(key.unwrapOutput) {
		t.Fatal("Open() did not clear partial dependency output")
	}

	key = &observingSecretValueKey{unwrapOutput: []byte("must-be-cleared")}
	cipher = &secretValueCipher{key: key}
	canceled, cancel := context.WithCancel(context.Background())
	key.onUnwrap = cancel
	if _, err := cipher.Open(canceled, []byte("ciphertext")); !errors.Is(err, context.Canceled) {
		t.Fatalf("Open() error = %v, want task context cancellation", err)
	}
	if !allBytesZero(key.unwrapInput) || !allBytesZero(key.unwrapOutput) {
		t.Fatal("canceled Open() retained a transient buffer")
	}
}

// Rationale: corrupt durable ciphertext is a Controller storage fault, and
// neither wrap nor unwrap failures may preserve secret-bearing error text.
func TestSecretValueProtectorSanitizesControllerKeyFailures(t *testing.T) {
	t.Parallel()

	const secret = "secret-bearing key failure"
	key := &observingSecretValueKey{wrapOutput: []byte("age-ciphertext")}
	protector := mustSecretValueProtector(t, key)
	envelope, err := protector.Seal(context.Background(), []byte("environment-secret"))
	if err != nil {
		t.Fatalf("Seal() error = %v", err)
	}

	key.unwrapErr = errs.New(errs.KindValidationFailed, secret)
	err = protector.Open(context.Background(), envelope, func([]byte) error {
		t.Fatal("plaintext callback was called for corrupt durable ciphertext")
		return nil
	})
	assertSecretValueInternalWithoutText(t, err, secret)

	key.wrapErr = errors.New(secret)
	_, err = protector.Seal(context.Background(), []byte("environment-secret"))
	assertSecretValueInternalWithoutText(t, err, secret)
}

// Rationale: a missing root age key is an application wiring defect and must
// fail before constructing a partially usable secret-value boundary.
func TestNewSecretValueProtectorRequiresControllerKey(t *testing.T) {
	t.Parallel()

	protector, err := newSecretValueProtector(nil)
	if protector != nil {
		t.Fatal("newSecretValueProtector(nil) returned a protector")
	}
	assertSecretValueInternalWithoutText(t, err, "")
}

// Rationale: one application-scoped adapter may seal and open concurrently;
// no operation may share another operation's transient buffers.
func TestSecretValueCipherSupportsConcurrentIndependentOperations(t *testing.T) {
	t.Parallel()

	cipher := &secretValueCipher{key: reversibleSecretValueKey{}}
	const operations = 64
	errorsByOperation := make(chan error, operations)
	var workers sync.WaitGroup
	workers.Add(operations)
	for index := range operations {
		go func() {
			defer workers.Done()
			want := []byte{byte(index), byte(index >> 8), 0xa5}
			ciphertext, err := cipher.Seal(context.Background(), want)
			if err != nil {
				errorsByOperation <- err
				return
			}
			plaintext, err := cipher.Open(context.Background(), ciphertext)
			if err != nil {
				errorsByOperation <- err
				return
			}
			if !bytes.Equal(plaintext, want) {
				errorsByOperation <- errors.New("round trip mismatch")
			}
		}()
	}
	workers.Wait()
	close(errorsByOperation)
	for err := range errorsByOperation {
		t.Fatalf("concurrent operation error = %v", err)
	}
}

type observingSecretValueKey struct {
	wrapInput    []byte
	wrapOutput   []byte
	wrapErr      error
	wrapCalls    int
	unwrapInput  []byte
	unwrapOutput []byte
	unwrapErr    error
	onWrap       func()
	onUnwrap     func()
}

func (key *observingSecretValueKey) Wrap(plaintext []byte) ([]byte, error) {
	key.wrapCalls++
	key.wrapInput = plaintext
	if key.onWrap != nil {
		key.onWrap()
	}
	return key.wrapOutput, key.wrapErr
}

func (key *observingSecretValueKey) Unwrap(ciphertext []byte) ([]byte, error) {
	key.unwrapInput = ciphertext
	if key.onUnwrap != nil {
		key.onUnwrap()
	}
	return key.unwrapOutput, key.unwrapErr
}

type reversibleSecretValueKey struct{}

func (reversibleSecretValueKey) Wrap(plaintext []byte) ([]byte, error) {
	return append([]byte{0xa5}, plaintext...), nil
}

func (reversibleSecretValueKey) Unwrap(ciphertext []byte) ([]byte, error) {
	if len(ciphertext) == 0 || ciphertext[0] != 0xa5 {
		return nil, errors.New("invalid test ciphertext")
	}
	return append([]byte(nil), ciphertext[1:]...), nil
}

func mustSecretValueProtector(t *testing.T, key secretValueKey) *secretvalue.Protector {
	t.Helper()
	cipher := &secretValueCipher{key: key}
	protector, err := secretvalue.NewProtector(cipher, cipher)
	if err != nil {
		t.Fatalf("NewProtector() error = %v", err)
	}
	return protector
}

func assertSecretValueInternalWithoutText(t *testing.T, err error, secret string) {
	t.Helper()
	kind, ok := errs.KindOf(err)
	if !ok || kind != errs.KindInternal {
		t.Fatalf("error = %v, want internal kind", err)
	}
	if secret != "" && strings.Contains(err.Error(), secret) {
		t.Fatalf("error retained secret-bearing text: %v", err)
	}
	var domainError *errs.Error
	if !errors.As(err, &domainError) {
		t.Fatalf("error type = %T, want *errs.Error", err)
	}
	if secret != "" && strings.Contains(domainError.ToProblem().Detail, secret) {
		t.Fatalf("problem retained secret-bearing text: %#v", domainError.ToProblem())
	}
}
