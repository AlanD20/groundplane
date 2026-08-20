package app

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/config"
	"github.com/AlanD20/groundplane/internal/controller/localagent"
	"github.com/AlanD20/groundplane/internal/infra/agentcredential"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type controllerKey interface {
	Wrap(plaintext []byte) ([]byte, error)
	Unwrap(ciphertext []byte) ([]byte, error)
}

// credentialCipher adapts the application-owned Controller key to the narrow
// sealing and opening ports used by runtime infrastructure.
type credentialCipher struct {
	key controllerKey
}

func newCredentialCipher(key controllerKey) (*credentialCipher, error) {
	if key == nil {
		return nil, errs.New(errs.CodeInternal, "agent runtime controller key is required")
	}
	return &credentialCipher{key: key}, nil
}

func (cipher *credentialCipher) Seal(ctx context.Context, plaintext []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return cipher.key.Wrap(plaintext)
}

func (cipher *credentialCipher) Open(ctx context.Context, ciphertext []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return cipher.key.Unwrap(ciphertext)
}

type credentialRuntime interface {
	Generate(ctx context.Context, agentID string) (agentcredential.Credential, error)
	Materialize(
		ctx context.Context,
		agentID string,
		encryptedToken agentcredential.EncryptedToken,
		runtimeConfig config.AgentConfig,
	) error
	Remove(ctx context.Context, agentID string) error
}

// localAgentRuntimeAdapter is the application boundary between the Controller
// lifecycle model and infrastructure. It is the only place where their types
// are translated, preserving the one-way import matrix.
type localAgentRuntimeAdapter struct {
	runtime credentialRuntime
	log     config.LogConfig
}

func newLocalAgentRuntimeAdapter(
	runtime credentialRuntime,
	logConfig config.LogConfig,
) (*localAgentRuntimeAdapter, error) {
	if runtime == nil {
		return nil, errs.New(errs.CodeInternal, "local agent credential runtime is required")
	}
	return &localAgentRuntimeAdapter{runtime: runtime, log: logConfig}, nil
}

func (adapter *localAgentRuntimeAdapter) GenerateCredential(
	ctx context.Context,
	agentID string,
) (localagent.Credential, error) {
	credential, err := adapter.runtime.Generate(ctx, agentID)
	if err != nil {
		return localagent.Credential{}, err
	}
	ciphertext := append([]byte(nil), credential.EncryptedToken.Ciphertext...)
	clear(credential.EncryptedToken.Ciphertext)
	return localagent.Credential{
		EncryptedToken: ciphertext,
		Digest:         string(credential.Digest),
	}, nil
}

func (adapter *localAgentRuntimeAdapter) Materialize(
	ctx context.Context,
	material localagent.RuntimeMaterial,
) error {
	runtimeConfig := config.DefaultAgentConfig()
	runtimeConfig.AgentID = material.AgentID
	runtimeConfig.Log = adapter.log
	runtimeConfig.Runtime.PullIntervalSeconds = material.Config.PullIntervalSeconds
	runtimeConfig.Runtime.MaxConcurrentTasks = material.Config.MaxConcurrentTasks
	runtimeConfig.Runtime.Labels = cloneRuntimeLabels(material.Config.Labels)

	ciphertext := append([]byte(nil), material.EncryptedToken...)
	defer clear(ciphertext)
	return adapter.runtime.Materialize(
		ctx,
		material.AgentID,
		agentcredential.EncryptedToken{Ciphertext: ciphertext},
		runtimeConfig,
	)
}

func (adapter *localAgentRuntimeAdapter) Remove(ctx context.Context, agentID string) error {
	return adapter.runtime.Remove(ctx, agentID)
}

func cloneRuntimeLabels(labels map[string]string) map[string]string {
	if labels == nil {
		return nil
	}
	cloned := make(map[string]string, len(labels))
	for key, value := range labels {
		cloned[key] = value
	}
	return cloned
}

var _ localagent.Runtime = (*localAgentRuntimeAdapter)(nil)
var _ agentcredential.Sealer = (*credentialCipher)(nil)
var _ agentcredential.Opener = (*credentialCipher)(nil)
