package agentcredential

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
)

const testAgentID = "agt_01ARZ3NDEKTSV4RRFFQ69G5FAV"

// Rationale: durable creation must commit encrypted credential material before
// any volatile token or config file can become visible.
func TestGenerateReturnsDurableCredentialWithoutRuntimeArtifact(t *testing.T) {
	t.Parallel()

	raw := bytes.Repeat([]byte{0x2a}, agentprotocol.RawTokenBytes)
	sealer := &capturingSealer{ciphertext: []byte("sealed-token")}
	manager := testManager(t, bytes.NewReader(raw), sealer, &capturingOpener{})

	credential, err := manager.Generate(context.Background(), testAgentID)
	if err != nil {
		t.Fatalf("Generate() error = %v", err)
	}
	if !bytes.Equal(sealer.plaintext, raw) {
		t.Fatal("Sealer plaintext differs from generated raw token")
	}
	if string(credential.EncryptedToken.Ciphertext) != "sealed-token" {
		t.Fatalf("ciphertext = %q, want sealed-token", credential.EncryptedToken.Ciphertext)
	}
	digest := sha256.Sum256(raw)
	wantDigest := base64.RawURLEncoding.EncodeToString(digest[:])
	if string(credential.Digest) != wantDigest {
		t.Fatalf("digest = %q, want %q", credential.Digest, wantDigest)
	}
	if _, err := os.Lstat(manager.testPath("run/groundplane/agents/" + testAgentID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("runtime directory stat error = %v, want not exist", err)
	}
}

// Rationale: failed entropy or sealing must leave no durable-safe credential,
// no filesystem artifact, and no token plaintext in the returned error.
func TestGenerateFailsClosedWithoutCredentialArtifact(t *testing.T) {
	t.Parallel()

	raw := bytes.Repeat([]byte{0x44}, agentprotocol.RawTokenBytes)
	tests := []struct {
		name   string
		random io.Reader
		sealer Sealer
	}{
		{name: "entropy", random: errorReader{}, sealer: &capturingSealer{}},
		{
			name:   "sealer",
			random: bytes.NewReader(raw),
			sealer: &capturingSealer{failWithPlaintext: true},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			manager := testManager(t, test.random, test.sealer, &capturingOpener{})
			credential, err := manager.Generate(context.Background(), testAgentID)
			if err == nil {
				t.Fatal("Generate() error = nil, want failure")
			}
			if len(credential.EncryptedToken.Ciphertext) != 0 || credential.Digest != "" {
				t.Fatalf("Generate() credential = %#v, want empty", credential)
			}
			if strings.Contains(err.Error(), base64.RawURLEncoding.EncodeToString(raw)) ||
				strings.Contains(err.Error(), string(raw)) {
				t.Fatalf("Generate() error leaked token plaintext: %v", err)
			}
			if _, statErr := os.Lstat(
				manager.testPath("run/groundplane/agents/" + testAgentID),
			); !errors.Is(
				statErr,
				os.ErrNotExist,
			) {
				t.Fatalf("runtime directory stat error = %v, want not exist", statErr)
			}
		})
	}
}

// Rationale: invalid durable identity and pre-cancellation must be rejected
// before consuming entropy.
func TestGenerateValidatesBeforeEntropyConsumption(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name    string
		agentID string
		cancel  bool
	}{
		{name: "invalid id", agentID: "agt_../../escape"},
		{name: "cancelled", agentID: testAgentID, cancel: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			reader := &countingReader{reader: bytes.NewReader(bytes.Repeat(
				[]byte{0x66},
				agentprotocol.RawTokenBytes,
			))}
			manager := testManager(t, reader, &capturingSealer{}, &capturingOpener{})
			ctx, cancel := context.WithCancel(context.Background())
			if test.cancel {
				cancel()
			} else {
				defer cancel()
			}
			if _, err := manager.Generate(ctx, test.agentID); err == nil {
				t.Fatal("Generate() error = nil, want refusal")
			}
			if reader.reads != 0 {
				t.Fatalf("entropy reads = %d, want 0", reader.reads)
			}
		})
	}
}

type capturingSealer struct {
	plaintext         []byte
	ciphertext        []byte
	failWithPlaintext bool
}

func (s *capturingSealer) Seal(_ context.Context, plaintext []byte) ([]byte, error) {
	s.plaintext = append([]byte(nil), plaintext...)
	if s.failWithPlaintext {
		return nil, errors.New("cannot seal " + base64.RawURLEncoding.EncodeToString(plaintext))
	}
	return append([]byte(nil), s.ciphertext...), nil
}

type capturingOpener struct {
	plaintext         []byte
	returned          []byte
	failWithPlaintext bool
	calls             int
}

func (o *capturingOpener) Open(_ context.Context, _ []byte) ([]byte, error) {
	o.calls++
	if o.failWithPlaintext {
		return nil, errors.New("cannot open " + string(o.plaintext))
	}
	o.returned = append([]byte(nil), o.plaintext...)
	return o.returned, nil
}

type errorReader struct{}

func (errorReader) Read([]byte) (int, error) { return 0, errors.New("entropy unavailable") }

type countingReader struct {
	reader io.Reader
	reads  int
}

func (r *countingReader) Read(buffer []byte) (int, error) {
	r.reads++
	return r.reader.Read(buffer)
}
