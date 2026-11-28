package transport

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/config"
	"github.com/AlanD20/groundplane/internal/common/remotehost"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/localagents"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func RemoteRuntimeConfig(runtime config.AgentRuntimeConfig) *agentpb.AgentConfig {
	return agentChannelConfig(localagents.LocalAgentConfig{
		PullIntervalSeconds: runtime.PullIntervalSeconds, MaxConcurrentTasks: runtime.MaxConcurrentTasks,
		MaxParallelOperationsPerTask: runtime.MaxParallelOperationsPerTask, Labels: runtime.Labels,
	})
}

// RemoteWorkResolver exposes only the configured subset of installed
// operation owners. Its authority comes from an enrolled Agent certificate.
type RemoteWorkResolver struct {
	repository *agentregistration.Repository
	config     *agentpb.AgentConfig
	operations []workexecution.Operation
}

func NewRemoteWorkResolver(repository *agentregistration.Repository, config *agentpb.AgentConfig,
	operations []workexecution.Operation) (*RemoteWorkResolver, error) {
	if repository == nil || config == nil || config.PullIntervalSeconds <= 0 || len(operations) == 0 {
		return nil, errs.New(
			errs.KindValidationFailed,
			"Remote Agent work requires configuration and supported operations",
		)
	}
	for index, operation := range operations {
		if !workexecution.ValidOperation(operation) || slices.Contains(operations[:index], operation) {
			return nil, errs.New(errs.KindValidationFailed, "Remote Agent work operation policy is invalid")
		}
	}
	return &RemoteWorkResolver{
		repository: repository,
		config:     proto.CloneOf(config),
		operations: slices.Clone(operations),
	}, nil
}

func (resolver *RemoteWorkResolver) RemoteConfiguration(
	ctx context.Context,
	authority remotehost.Authorization,
) (*agentpb.AgentConfig, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !authority.Identity.Valid() || authority.HostRevision <= 0 {
		return nil, errs.New(errs.KindRequestForbidden, "Remote Agent credential is not authorized")
	}
	return proto.CloneOf(resolver.config), nil
}

func (resolver *RemoteWorkResolver) RegisterRemoteWorkProcess(
	ctx context.Context,
	authority remotehost.Authorization,
	process [16]byte,
	limits workexecution.AgentConcurrency,
	installed []workexecution.Operation,
) (workexecution.Identity, error) {
	for _, operation := range resolver.operations {
		if !slices.Contains(installed, operation) {
			return workexecution.Identity{}, errs.New(errs.KindInternal, "Remote Agent operation owner is unavailable")
		}
	}
	return resolver.repository.RegisterRemoteWorkProcess(ctx, authority, process, limits, resolver.operations)
}

func (resolver *RemoteWorkResolver) ResolveRemoteWorkIdentity(ctx context.Context, authority remotehost.Authorization,
	process [16]byte) (agentregistration.WorkAuthorization, error) {
	return resolver.repository.ResolveRemoteWorkIdentity(ctx, authority, process)
}
