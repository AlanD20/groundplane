package runtime

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/config"
	"github.com/AlanD20/groundplane/internal/controller/localagent"
	"github.com/AlanD20/groundplane/internal/infra/agentcredential"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const runtimeAdapterAgentID = "agt_01ARZ3NDEKTSV4RRFFQ69G5FAV"

// Rationale: the app boundary must translate durable-safe credential values
// without sharing infrastructure-owned backing buffers with the Controller.
func TestLocalAgentRuntimeAdapterGeneratesIndependentCredential(t *testing.T) {
	t.Parallel()

	runtime := &fakeCredentialRuntime{
		credential: agentcredential.Credential{
			EncryptedToken: agentcredential.EncryptedToken{Ciphertext: []byte("encrypted")},
			Digest:         agentcredential.TokenDigest("digest"),
		},
	}
	adapter, err := NewCredentials(
		runtime, config.DefaultAgentConfig().Log, "/srv/groundplane-volumes",
	)
	if err != nil {
		t.Fatalf("NewCredentials() error = %v", err)
	}
	credential, err := adapter.GenerateCredential(context.Background(), runtimeAdapterAgentID)
	if err != nil {
		t.Fatalf("GenerateCredential() error = %v", err)
	}
	if string(credential.EncryptedToken) != "encrypted" || credential.Digest != "digest" {
		t.Fatalf("GenerateCredential() = %#v, want translated credential", credential)
	}
	runtime.credential.EncryptedToken.Ciphertext[0] = 'X'
	if string(credential.EncryptedToken) != "encrypted" {
		t.Fatal("GenerateCredential() shared infrastructure ciphertext backing storage")
	}
}

// Rationale: restart reconciliation must translate one durable material value
// into the exact typed runtime config while clearing its temporary ciphertext.
func TestLocalAgentRuntimeAdapterMaterializesCompleteRuntime(t *testing.T) {
	t.Parallel()

	runtime := &fakeCredentialRuntime{}
	logConfig := config.DefaultAgentConfig().Log
	adapter, err := NewCredentials(runtime, logConfig, "/srv/groundplane-volumes")
	if err != nil {
		t.Fatalf("NewCredentials() error = %v", err)
	}
	labels := map[string]string{"arch": "arm64"}
	material := localagent.RuntimeMaterial{
		AgentID:        runtimeAdapterAgentID,
		Generation:     7,
		Config:         localagent.Config{PullIntervalSeconds: 2, MaxConcurrentTasks: 3, Labels: labels},
		EncryptedToken: []byte("stored-ciphertext"),
	}
	if err := adapter.Materialize(context.Background(), material); err != nil {
		t.Fatalf("Materialize() error = %v", err)
	}
	if runtime.agentID != runtimeAdapterAgentID {
		t.Fatalf("materialized agent id = %q, want %q", runtime.agentID, runtimeAdapterAgentID)
	}
	if runtime.runtimeConfig.AgentID != runtimeAdapterAgentID ||
		runtime.runtimeConfig.Runtime.PullIntervalSeconds != 2 ||
		runtime.runtimeConfig.Runtime.MaxConcurrentTasks != 3 ||
		runtime.runtimeConfig.Runtime.Labels["arch"] != "arm64" ||
		runtime.runtimeConfig.Storage.VolumeRoot != "/srv/groundplane-volumes" ||
		runtime.runtimeConfig.Log != logConfig {
		t.Fatalf("materialized config = %#v, want complete durable runtime", runtime.runtimeConfig)
	}
	labels["arch"] = "changed"
	if runtime.runtimeConfig.Runtime.Labels["arch"] != "arm64" {
		t.Fatal("Materialize() retained caller-owned labels")
	}
	if !allBytesZero(runtime.ciphertext) {
		t.Fatal("Materialize() did not clear adapter-owned ciphertext copy")
	}
}

// Rationale: application-owned key adaptation must honor cancellation before
// invoking crypto and otherwise delegate wrapping and unwrapping exactly once.
func TestCredentialCipherAdaptsControllerKey(t *testing.T) {
	t.Parallel()

	key := &fakeControllerKey{}
	cipher, err := NewCredentialCipher(key)
	if err != nil {
		t.Fatalf("NewCredentialCipher() error = %v", err)
	}
	sealed, err := cipher.Seal(context.Background(), []byte("plain"))
	if err != nil || string(sealed) != "wrapped" {
		t.Fatalf("Seal() = %q, %v; want wrapped, nil", sealed, err)
	}
	opened, err := cipher.Open(context.Background(), []byte("cipher"))
	if err != nil || string(opened) != "unwrapped" {
		t.Fatalf("Open() = %q, %v; want unwrapped, nil", opened, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := cipher.Seal(ctx, []byte("ignored")); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Seal() error = %v, want context.Canceled", err)
	}
	if key.wrapCalls != 1 || key.unwrapCalls != 1 {
		t.Fatalf("key calls = wrap %d, unwrap %d; want 1 each", key.wrapCalls, key.unwrapCalls)
	}
}

// Rationale: persisted Agent ciphertext is Controller-owned durable state, so
// an unwrap failure is internal corruption or key failure and must retain its
// private cause without exposing secret-bearing text through RFC 7807.
func TestCredentialCipherSanitizesControllerKeyUnwrapFailure(t *testing.T) {
	t.Parallel()

	const secret = "ciphertext contains secret material"
	cause := errors.New(secret)
	cipher, err := NewCredentialCipher(&fakeControllerKey{unwrapErr: cause})
	if err != nil {
		t.Fatalf("NewCredentialCipher() error = %v", err)
	}
	_, err = cipher.Open(context.Background(), []byte("cipher"))
	if !errors.Is(err, errs.New(errs.KindInternal, "")) || !errors.Is(err, cause) {
		t.Fatalf("Open() error = %v", err)
	}
	var domainError *errs.Error
	if !errors.As(err, &domainError) {
		t.Fatalf("Open() error type = %T", err)
	}
	if problem := domainError.ToProblem(); strings.Contains(problem.Detail, secret) {
		t.Fatalf("Open() problem leaks cause: %#v", problem)
	}
	if !strings.Contains(err.Error(), secret) {
		t.Fatal("private cause is unavailable to internal logging")
	}
}

type fakeCredentialRuntime struct {
	credential    agentcredential.Credential
	agentID       string
	ciphertext    []byte
	runtimeConfig config.AgentConfig
}

func (runtime *fakeCredentialRuntime) Generate(
	context.Context,
	string,
) (agentcredential.Credential, error) {
	return runtime.credential, nil
}

func (runtime *fakeCredentialRuntime) Materialize(
	_ context.Context,
	agentID string,
	encryptedToken agentcredential.EncryptedToken,
	runtimeConfig config.AgentConfig,
) error {
	runtime.agentID = agentID
	runtime.ciphertext = encryptedToken.Ciphertext
	runtime.runtimeConfig = runtimeConfig
	return nil
}

func (runtime *fakeCredentialRuntime) Remove(context.Context, string) error {
	return nil
}

type fakeControllerKey struct {
	wrapCalls   int
	unwrapCalls int
	unwrapErr   error
}

func (key *fakeControllerKey) Wrap(plaintext []byte) ([]byte, error) {
	key.wrapCalls++
	if !bytes.Equal(plaintext, []byte("plain")) {
		return nil, errors.New("unexpected plaintext")
	}
	return []byte("wrapped"), nil
}

func (key *fakeControllerKey) Unwrap(ciphertext []byte) ([]byte, error) {
	key.unwrapCalls++
	if !bytes.Equal(ciphertext, []byte("cipher")) {
		return nil, errors.New("unexpected ciphertext")
	}
	if key.unwrapErr != nil {
		return nil, key.unwrapErr
	}
	return []byte("unwrapped"), nil
}

func allBytesZero(value []byte) bool {
	for _, item := range value {
		if item != 0 {
			return false
		}
	}
	return true
}
