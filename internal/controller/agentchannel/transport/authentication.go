package transport

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"

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
	GetSingleton(context.Context) (etcdstore.Versioned[etcd.LocalAgentRecord], error)
}

type agentChannelAuthenticator struct {
	resolver agentChannelCredentialResolver
}

func NewAuthenticator(
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
	return agentchannel.Authorization{
		Generation: resolved.Generation,
		Config:     agentChannelConfig(resolved.Config),
	}, nil
}

func (authenticator *agentChannelAuthenticator) Configuration(
	ctx context.Context,
	agentID string,
	generation uint64,
) (*agentpb.AgentConfig, error) {
	stored, err := authenticator.resolver.GetSingleton(ctx)
	if err != nil {
		return nil, err
	}
	if stored.Record.ID != agentID || stored.Record.Generation != generation ||
		stored.Record.Phase == etcd.LocalAgentPhaseDeleting {
		return nil, errs.New(errs.KindStateConflict, "Agent channel generation is no longer current")
	}
	return agentChannelConfig(stored.Record.Config), nil
}

func agentChannelConfig(config etcd.LocalAgentConfig) *agentpb.AgentConfig {
	labels := make(map[string]string, len(config.Labels))
	for key, value := range config.Labels {
		labels[key] = value
	}
	return &agentpb.AgentConfig{
		PullIntervalSeconds: config.PullIntervalSeconds,
		MaxConcurrentTasks:  config.MaxConcurrentTasks,
		Labels:              labels,
	}
}

var _ agentchannel.Authenticator = (*agentChannelAuthenticator)(nil)
