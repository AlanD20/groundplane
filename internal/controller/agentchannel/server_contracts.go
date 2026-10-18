package agentchannel

import (
	"context"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const tokenSize = 32

// Token is the decoded, fixed-size Agent credential presented to an Authenticator.
type Token [tokenSize]byte

// Authorization is the non-secret result of successful Agent authentication.
type Authorization struct {
	Generation uint64
	Config     *agentpb.AgentConfig
}

// Authenticator authenticates one Agent generation without retaining or
// returning its credential.
type Authenticator interface {
	Authenticate(context.Context, string, Token) (Authorization, error)
	Configuration(context.Context, string, uint64) (*agentpb.AgentConfig, error)
}

// RestoreIdentityReleaser clears Task-owned transient restore identities.
type RestoreIdentityReleaser interface {
	ReleaseRestoreIdentity(context.Context, string) error
}
