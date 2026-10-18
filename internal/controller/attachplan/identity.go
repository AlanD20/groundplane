package attachplan

import (
	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/core"
)

// Identity is opened only for the duration of a captured-input consumer. The
// fact owner clears plaintext after the callback returns; it is never persisted
// as execution history.
type Identity struct {
	Authentication core.BackingAuthentication
	Database       string
	Role           string
	Password       []byte
	Grants         []GrantIdentity
}

type GrantIdentity struct {
	AttachID string
	Database string
}

func (identity *Identity) Clear() {
	if identity != nil {
		clear(identity.Password)
		identity.Password = nil
		identity.Grants = nil
	}
}

type IdentityConsumer func(Identity) error
type HookInputConsumer func(backinghook.Input) error
