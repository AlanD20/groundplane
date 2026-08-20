// Package agentcredential generates, seals, and materializes the local
// Controller-owned Agent channel credential. It does not store credentials or
// define repository, channel, protocol, or orchestration policy.
package agentcredential

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"io"
	"os"
	"sync"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/infra/docker/agentcontainer"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type TokenDigest string

type EncryptedToken struct {
	Ciphertext []byte
}

// Credential contains only durable-safe encrypted material and lookup
// metadata. It can be written with the Agent record in one transaction.
type Credential struct {
	EncryptedToken EncryptedToken
	Digest         TokenDigest
}

// Sealer is implemented by the application layer that owns the Controller
// encryption key. Seal must consume plaintext synchronously and must not retain
// the provided slice.
type Sealer interface {
	Seal(ctx context.Context, plaintext []byte) ([]byte, error)
}

type Manager struct {
	mu          sync.Mutex
	random      io.Reader
	sealer      Sealer
	hostRoot    string
	expectedUID uint32
	rename      func(*os.Root, string, string) error
}

func New(random io.Reader, sealer Sealer) (*Manager, error) {
	return newManager(random, sealer, "/", 0)
}

func newManager(random io.Reader, sealer Sealer, hostRoot string, expectedUID uint32) (*Manager, error) {
	if random == nil {
		return nil, errs.New(errs.CodeInternal, "agent credential: randomness source is required")
	}
	if sealer == nil {
		return nil, errs.New(errs.CodeInternal, "agent credential: sealer is required")
	}
	if hostRoot == "" {
		return nil, errs.New(errs.CodeInternal, "agent credential: host root is required")
	}
	return &Manager{
		random:      random,
		sealer:      sealer,
		hostRoot:    hostRoot,
		expectedUID: expectedUID,
		rename: func(root *os.Root, oldName, newName string) error {
			return root.Rename(oldName, newName)
		},
	}, nil
}

// GenerateAndMaterialize creates one credential, seals its raw bytes, and
// atomically writes its unpadded base64url encoding to the validated Agent
// runtime token path. No plaintext value is returned.
func (m *Manager) GenerateAndMaterialize(ctx context.Context, agentID string) (Credential, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return Credential{}, err
	}
	if _, err := agentcontainer.RuntimePathsForAgent(agentID); err != nil {
		return Credential{}, err
	}

	var raw [agentprotocol.RawTokenBytes]byte
	defer clear(raw[:])
	if _, err := io.ReadFull(m.random, raw[:]); err != nil {
		return Credential{}, errs.New(errs.CodeInternal, "agent credential: generate token entropy")
	}
	if err := ctx.Err(); err != nil {
		return Credential{}, err
	}

	digest := sha256.Sum256(raw[:])
	digestText := base64.RawURLEncoding.EncodeToString(digest[:])
	ciphertext, err := m.sealer.Seal(ctx, raw[:])
	if err != nil {
		return Credential{}, errs.New(errs.CodeInternal, "agent credential: seal token")
	}
	if len(ciphertext) == 0 {
		return Credential{}, errs.New(errs.CodeInternal, "agent credential: sealer returned empty ciphertext")
	}
	if err := ctx.Err(); err != nil {
		return Credential{}, err
	}

	encoded := make([]byte, base64.RawURLEncoding.EncodedLen(len(raw)))
	defer clear(encoded)
	base64.RawURLEncoding.Encode(encoded, raw[:])
	if err := m.materializeToken(ctx, agentID, encoded); err != nil {
		return Credential{}, err
	}

	return Credential{
		EncryptedToken: EncryptedToken{Ciphertext: append([]byte(nil), ciphertext...)},
		Digest:         TokenDigest(digestText),
	}, nil
}
