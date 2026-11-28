package agentregistration

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/remotehost"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/localagents"
	"github.com/AlanD20/groundplane/internal/infra/etcd/remotehosts"
)

type workCredential struct {
	key        string
	agentID    string
	generation uint64
	remoteHost string
	revision   int64
}

func localWorkCredential(agentID string, generation uint64) workCredential {
	return workCredential{key: localagents.LocalAgentPrimaryKey(agentID), agentID: agentID, generation: generation}
}

func remoteWorkCredential(authority remotehost.Authorization) workCredential {
	identity := authority.Identity
	if !identity.Valid() || authority.HostRevision <= 0 {
		return workCredential{}
	}
	return workCredential{key: remotehosts.Key(identity.HostID), agentID: identity.AgentID,
		generation: identity.Generation, remoteHost: identity.HostID, revision: authority.HostRevision}
}

func (credential workCredential) valid() bool { return credential.generation > 0 }

func (credential workCredential) acceptsHost(hostID string) bool {
	return credential.remoteHost == "" || credential.remoteHost == hostID
}

func (credential workCredential) check(record *keyvalue.KeyValue) error {
	if record == nil {
		return agentCredentialNotFound()
	}
	if credential.remoteHost != "" {
		if credential.revision != record.ModRevision {
			return agentCredentialNotFound()
		}
		host, err := remotehosts.DecodeHost(credential.remoteHost, record.Value)
		if err != nil {
			return err
		}
		if host.Generation != credential.generation || host.Agent == nil || host.Agent.AgentID != credential.agentID {
			return agentCredentialNotFound()
		}
		return nil
	}
	agent, err := localagents.DecodeLocalAgentPrimary(record.Value)
	if err != nil {
		return err
	}
	if agent.ID != credential.agentID || agent.Generation != credential.generation ||
		agent.Phase == localagents.LocalAgentPhaseDeleting {
		return agentCredentialNotFound()
	}
	return nil
}

func (repository *Repository) RegisterRemoteWorkProcess(
	ctx context.Context,
	authority remotehost.Authorization,
	process [16]byte,
	limits workexecution.AgentConcurrency,
	operations []workexecution.Operation,
) (workexecution.Identity, error) {
	return repository.registerWorkProcess(
		ctx,
		authority.Identity.AgentID,
		remoteWorkCredential(authority),
		process,
		limits,
		operations,
	)
}

func (repository *Repository) ResolveRemoteWorkIdentity(ctx context.Context, authority remotehost.Authorization,
	process [16]byte) (WorkAuthorization, error) {
	return repository.resolveWorkIdentity(ctx, authority.Identity.AgentID, remoteWorkCredential(authority), process)
}
