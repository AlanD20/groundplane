package app

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/controller/agentchannel"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type agentChannelCredentialResolver interface {
	ResolveAgentChannel(
		context.Context,
		string,
		[agentprotocol.RawTokenBytes]byte,
	) (etcd.LocalAgentChannelAuthorization, error)
}

type agentChannelAuthenticator struct {
	resolver agentChannelCredentialResolver
}

func newAgentChannelAuthenticator(
	resolver agentChannelCredentialResolver,
) (*agentChannelAuthenticator, error) {
	if resolver == nil {
		return nil, errs.New(errs.KindInternal, "Agent channel credential resolver is required")
	}
	return &agentChannelAuthenticator{resolver: resolver}, nil
}

func (authenticator *agentChannelAuthenticator) Authenticate(
	ctx context.Context,
	agentID string,
	token agentchannel.Token,
) (agentchannel.Authorization, error) {
	var raw [agentprotocol.RawTokenBytes]byte
	copy(raw[:], token[:])
	defer clear(raw[:])
	resolved, err := authenticator.resolver.ResolveAgentChannel(ctx, agentID, raw)
	if err != nil {
		return agentchannel.Authorization{}, err
	}
	if resolved.AgentID != agentID || resolved.Generation == 0 {
		return agentchannel.Authorization{}, errs.New(errs.KindInternal, "Agent channel authorization is inconsistent")
	}
	labels := make(map[string]string, len(resolved.Config.Labels))
	for key, value := range resolved.Config.Labels {
		labels[key] = value
	}
	return agentchannel.Authorization{
		Generation: resolved.Generation,
		Config: &agentpb.AgentConfig{
			PullIntervalSeconds: resolved.Config.PullIntervalSeconds,
			MaxConcurrentTasks:  resolved.Config.MaxConcurrentTasks,
			Labels:              labels,
		},
	}, nil
}

var _ agentchannel.Authenticator = (*agentChannelAuthenticator)(nil)
