package app

import (
	"context"
	"strconv"

	"github.com/AlanD20/groundplane/internal/controller/agentchannel"
	"github.com/AlanD20/groundplane/internal/controller/localagent"
	"github.com/AlanD20/groundplane/internal/infra/docker/agentcontainer"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// localAgentSessionsAdapter translates the authenticated channel registry into
// the lifecycle module's infrastructure-neutral session contract.
type localAgentSessionsAdapter struct {
	registry *agentchannel.Registry
}

func newLocalAgentSessionsAdapter(registry *agentchannel.Registry) (*localAgentSessionsAdapter, error) {
	if registry == nil {
		return nil, errs.New(errs.KindInternal, "local agent session registry is required")
	}
	return &localAgentSessionsAdapter{registry: registry}, nil
}

func (adapter *localAgentSessionsAdapter) Ready(
	ctx context.Context,
	agentID string,
	generation uint64,
) (<-chan struct{}, error) {
	return adapter.registry.Ready(ctx, agentID, generation)
}

func (adapter *localAgentSessionsAdapter) Snapshot(agentID string) (localagent.SessionSnapshot, bool) {
	snapshot, ok := adapter.registry.Snapshot(agentID)
	if !ok {
		return localagent.SessionSnapshot{}, false
	}
	return localagent.SessionSnapshot{
		Generation: snapshot.Generation,
		Online:     snapshot.Online,
		Revoked:    snapshot.Revoked,
		LastReady:  snapshot.LastReady,
		Capacity:   snapshot.Capacity,
		Version:    snapshot.Version,
	}, true
}

func (adapter *localAgentSessionsAdapter) StopAssignments(
	ctx context.Context,
	agentID string,
	generation uint64,
) error {
	return adapter.registry.StopAssignments(ctx, agentID, generation)
}

func (adapter *localAgentSessionsAdapter) Revoke(
	ctx context.Context,
	agentID string,
	generation uint64,
) error {
	return adapter.registry.Revoke(ctx, agentID, generation)
}

func (adapter *localAgentSessionsAdapter) WaitOffline(
	ctx context.Context,
	agentID string,
	generation uint64,
) error {
	return adapter.registry.WaitOffline(ctx, agentID, generation)
}

type agentContainerLifecycle interface {
	Reconcile(context.Context, agentcontainer.Desired) (agentcontainer.Result, error)
	Remove(context.Context, string, string) error
}

// localAgentContainerAdapter is the only translation between numeric durable
// generations and their canonical Docker label representation.
type localAgentContainerAdapter struct {
	lifecycle agentContainerLifecycle
}

func newLocalAgentContainerAdapter(lifecycle agentContainerLifecycle) (*localAgentContainerAdapter, error) {
	if lifecycle == nil {
		return nil, errs.New(errs.KindInternal, "local agent container lifecycle is required")
	}
	return &localAgentContainerAdapter{lifecycle: lifecycle}, nil
}

func (adapter *localAgentContainerAdapter) Converge(
	ctx context.Context,
	desired localagent.ContainerDesired,
) error {
	_, err := adapter.lifecycle.Reconcile(ctx, agentcontainer.Desired{
		Image:      desired.Image,
		AgentID:    desired.AgentID,
		Generation: strconv.FormatUint(desired.Generation, 10),
	})
	return err
}

func (adapter *localAgentContainerAdapter) Remove(
	ctx context.Context,
	agentID string,
	generation uint64,
) error {
	return adapter.lifecycle.Remove(ctx, agentID, strconv.FormatUint(generation, 10))
}

var _ localagent.Sessions = (*localAgentSessionsAdapter)(nil)
var _ localagent.Container = (*localAgentContainerAdapter)(nil)
