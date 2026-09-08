package agentchannel

import (
	"context"

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
