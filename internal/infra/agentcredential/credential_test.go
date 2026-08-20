package agentcredential

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
)

const testAgentID = "agt_01ARZ3NDEKTSV4RRFFQ69G5FAV"

// Rationale: the credential boundary must produce the exact durable metadata
// and runtime representation without returning plaintext.
func TestGenerateAndMaterializeProducesExactCredentialAndModes(t *testing.T) {
	t.Parallel()

	raw := bytes.Repeat([]byte{0xa5}, agentprotocol.RawTokenBytes)
	sealer := &capturingSealer{ciphertext: []byte("encrypted-token")}
	manager := testManager(t, bytes.NewReader(raw), sealer)

	credential, err := manager.GenerateAndMaterialize(context.Background(), testAgentID)
	if err != nil {
		t.Fatalf("GenerateAndMaterialize() error = %v", err)
	}
	if !bytes.Equal(sealer.plaintext, raw) {
		t.Fatalf("sealed plaintext length/content mismatch")
	}
	if !bytes.Equal(credential.EncryptedToken.Ciphertext, sealer.ciphertext) {
		t.Fatalf("ciphertext = %q, want %q", credential.EncryptedToken.Ciphertext, sealer.ciphertext)
	}
	wantDigestBytes := sha256.Sum256(raw)
	wantDigest := base64.RawURLEncoding.EncodeToString(wantDigestBytes[:])
	if string(credential.Digest) != wantDigest || strings.Contains(string(credential.Digest), "=") {
		t.Fatalf("digest = %q, want unpadded %q", credential.Digest, wantDigest)
	}

	tokenPath := manager.testPath("run/groundplane/agents/" + testAgentID + "/token")
	contents, err := os.ReadFile(tokenPath)
	if err != nil {
		t.Fatalf("read token: %v", err)
	}
	wantToken := base64.RawURLEncoding.EncodeToString(raw)
	if len(contents) != 43 || string(contents) != wantToken || strings.Contains(string(contents), "=") {
		t.Fatalf("token file = %q, want 43-char unpadded base64url", contents)
	}
	assertMode(t, filepath.Dir(tokenPath), 0o700)
	assertMode(t, tokenPath, 0o400)
}

// Rationale: failed entropy generation must stop before any credential artifact exists.
func TestGenerateAndMaterializeEntropyFailureCreatesNoArtifact(t *testing.T) {
	t.Parallel()

	manager := testManager(t, errorReader{}, &capturingSealer{})
	_, err := manager.GenerateAndMaterialize(context.Background(), testAgentID)
	if err == nil {
		t.Fatal("GenerateAndMaterialize() error = nil, want entropy failure")
	}
	if _, statErr := os.Lstat(manager.testPath("run/groundplane/agents/" + testAgentID)); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("runtime directory stat error = %v, want not exist", statErr)
	}
}

// Rationale: credential rotation must replace the token without leaving its temporary file.
func TestGenerateAndMaterializeAtomicallyReplacesToken(t *testing.T) {
	t.Parallel()

	manager := testManager(t, bytes.NewReader(bytes.Repeat([]byte{0x11}, agentprotocol.RawTokenBytes)), &capturingSealer{ciphertext: []byte("first")})
	if _, err := manager.GenerateAndMaterialize(context.Background(), testAgentID); err != nil {
		t.Fatalf("first materialization: %v", err)
	}
	manager.random = bytes.NewReader(bytes.Repeat([]byte{0x22}, agentprotocol.RawTokenBytes))
	manager.sealer = &capturingSealer{ciphertext: []byte("second")}
	if _, err := manager.GenerateAndMaterialize(context.Background(), testAgentID); err != nil {
		t.Fatalf("replacement materialization: %v", err)
	}

	tokenPath := manager.testPath("run/groundplane/agents/" + testAgentID + "/token")
	contents, err := os.ReadFile(tokenPath)
	if err != nil {
		t.Fatalf("read replacement token: %v", err)
	}
	want := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{0x22}, agentprotocol.RawTokenBytes))
	if string(contents) != want {
		t.Fatalf("replacement token = %q, want %q", contents, want)
	}
	if _, err := os.Lstat(filepath.Join(filepath.Dir(tokenPath), temporaryTokenName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temporary token stat error = %v, want not exist", err)
	}
}

// Rationale: refusing a token symlink prevents writes outside the validated runtime directory.
func TestGenerateAndMaterializeRefusesTokenSymlink(t *testing.T) {
	t.Parallel()

	manager := testManager(t, bytes.NewReader(bytes.Repeat([]byte{0x33}, agentprotocol.RawTokenBytes)), &capturingSealer{ciphertext: []byte("sealed")})
	directory := manager.testPath("run/groundplane/agents/" + testAgentID)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatalf("create runtime directory: %v", err)
	}
	target := manager.testPath("target")
	if err := os.WriteFile(target, []byte("untouched"), 0o600); err != nil {
		t.Fatalf("write symlink target: %v", err)
	}
	if err := os.Symlink(target, filepath.Join(directory, "token")); err != nil {
		t.Fatalf("create token symlink: %v", err)
	}

	_, err := manager.GenerateAndMaterialize(context.Background(), testAgentID)
	if err == nil {
		t.Fatal("GenerateAndMaterialize() error = nil, want symlink refusal")
	}
	contents, readErr := os.ReadFile(target)
	if readErr != nil || string(contents) != "untouched" {
		t.Fatalf("symlink target = %q, error = %v; want untouched", contents, readErr)
	}
}

// Rationale: a failed atomic replacement must clean up while keeping plaintext out of errors.
func TestGenerateAndMaterializeCleansTemporaryAfterRenameFailureWithoutLeakingPlaintext(t *testing.T) {
	t.Parallel()

	raw := bytes.Repeat([]byte{0x44}, agentprotocol.RawTokenBytes)
	manager := testManager(t, bytes.NewReader(raw), &capturingSealer{ciphertext: []byte("sealed")})
	manager.rename = func(*os.Root, string, string) error { return errors.New("rename blocked") }
	_, err := manager.GenerateAndMaterialize(context.Background(), testAgentID)
	if err == nil {
		t.Fatal("GenerateAndMaterialize() error = nil, want rename failure")
	}
	plaintext := base64.RawURLEncoding.EncodeToString(raw)
	if strings.Contains(err.Error(), plaintext) {
		t.Fatalf("error leaked plaintext token: %v", err)
	}
	directory := manager.testPath("run/groundplane/agents/" + testAgentID)
	if _, statErr := os.Lstat(filepath.Join(directory, temporaryTokenName)); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("temporary token stat error = %v, want not exist", statErr)
	}
}

// Rationale: an untrusted Sealer error must not cross the boundary with token plaintext.
func TestGenerateAndMaterializeDoesNotLeakPlaintextFromSealerError(t *testing.T) {
	t.Parallel()

	raw := bytes.Repeat([]byte{0x55}, agentprotocol.RawTokenBytes)
	sealer := &capturingSealer{failWithPlaintext: true}
	manager := testManager(t, bytes.NewReader(raw), sealer)
	_, err := manager.GenerateAndMaterialize(context.Background(), testAgentID)
	if err == nil {
		t.Fatal("GenerateAndMaterialize() error = nil, want sealing failure")
	}
	if strings.Contains(err.Error(), base64.RawURLEncoding.EncodeToString(raw)) || strings.Contains(err.Error(), string(raw)) {
		t.Fatalf("error leaked plaintext token: %v", err)
	}
}

// Rationale: pre-cancellation must prevent entropy consumption and filesystem effects.
func TestGenerateAndMaterializeHonorsCancellationWithoutArtifact(t *testing.T) {
	t.Parallel()

	reader := &countingReader{reader: bytes.NewReader(bytes.Repeat([]byte{0x66}, agentprotocol.RawTokenBytes))}
	manager := testManager(t, reader, &capturingSealer{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := manager.GenerateAndMaterialize(ctx, testAgentID)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("GenerateAndMaterialize() error = %v, want context.Canceled", err)
	}
	if reader.reads != 0 {
		t.Fatalf("entropy reads = %d, want 0", reader.reads)
	}
}

// Rationale: concurrent mutations must not collide on the fixed temporary token path.
func TestGenerateAndMaterializeSerializesConcurrentReplacements(t *testing.T) {
	t.Parallel()

	first := bytes.Repeat([]byte{0x21}, agentprotocol.RawTokenBytes)
	second := bytes.Repeat([]byte{0x42}, agentprotocol.RawTokenBytes)
	manager := testManager(t, bytes.NewReader(append(first, second...)), &capturingSealer{ciphertext: []byte("sealed")})
	originalRename := manager.rename
	renameEntered := make(chan struct{})
	releaseRename := make(chan struct{})
	renameCalls := 0
	manager.rename = func(root *os.Root, oldName, newName string) error {
		renameCalls++
		if renameCalls == 1 {
			close(renameEntered)
			<-releaseRename
		}
		return originalRename(root, oldName, newName)
	}

	firstResult := make(chan error, 1)
	go func() {
		_, err := manager.GenerateAndMaterialize(context.Background(), testAgentID)
		firstResult <- err
	}()
	<-renameEntered
	if manager.mu.TryLock() {
		manager.mu.Unlock()
		t.Fatal("Manager mutex was not held during materialization")
	}

	secondResult := make(chan error, 1)
	go func() {
		_, err := manager.GenerateAndMaterialize(context.Background(), testAgentID)
		secondResult <- err
	}()
	close(releaseRename)
	if err := <-firstResult; err != nil {
		t.Fatalf("first GenerateAndMaterialize() error = %v", err)
	}
	if err := <-secondResult; err != nil {
		t.Fatalf("second GenerateAndMaterialize() error = %v", err)
	}

	token, err := os.ReadFile(manager.testPath("run/groundplane/agents/" + testAgentID + "/token"))
	if err != nil {
		t.Fatalf("read token: %v", err)
	}
	want := base64.RawURLEncoding.EncodeToString(second)
	if string(token) != want {
		t.Fatalf("token = %q, want second serialized value %q", token, want)
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
