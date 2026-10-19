package agentchannel

import (
	"context"
	"sync"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (a *fakeAuthenticator) Configuration(context.Context, string, uint64) (*agentpb.AgentConfig, error) {
	a.configurationMu.Lock()
	defer a.configurationMu.Unlock()
	if a.configuration != nil {
		return proto.Clone(a.configuration).(*agentpb.AgentConfig), nil
	}
	return proto.Clone(a.authorization.Config).(*agentpb.AgentConfig), nil
}

func (a *fakeAuthenticator) setConfiguration(configuration *agentpb.AgentConfig) {
	a.configurationMu.Lock()
	defer a.configurationMu.Unlock()
	a.configuration = proto.Clone(configuration).(*agentpb.AgentConfig)
}

type fakeAuthenticator struct {
	configurationMu sync.Mutex
	authorization   Authorization
	configuration   *agentpb.AgentConfig
	err             error
	calls           int
	seenID          string
	seenToken       Token
}

type pausedAuthenticator struct {
	authorization Authorization
	entered       chan struct{}
	release       chan struct{}
}

func (a *pausedAuthenticator) Authenticate(ctx context.Context, _ string, _ Token) (Authorization, error) {
	close(a.entered)
	select {
	case <-ctx.Done():
		return Authorization{}, ctx.Err()
	case <-a.release:
		return a.authorization, nil
	}
}

func (a *pausedAuthenticator) Configuration(context.Context, string, uint64) (*agentpb.AgentConfig, error) {
	return proto.Clone(a.authorization.Config).(*agentpb.AgentConfig), nil
}

func (a *fakeAuthenticator) Authenticate(_ context.Context, id string, token Token) (Authorization, error) {
	a.calls++
	a.seenID = id
	a.seenToken = token
	return a.authorization, a.err
}
