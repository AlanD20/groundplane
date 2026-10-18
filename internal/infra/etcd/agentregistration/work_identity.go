package agentregistration

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/localagents"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// WorkAuthorization keeps authenticated identity and its publication fences
// together. Only the fixed-revision credential/binding read constructs it.
type WorkAuthorization struct {
	identity   workexecution.Identity
	conditions []keyvalue.Condition
}

func (authorization WorkAuthorization) Identity() workexecution.Identity {
	return authorization.identity
}

func (authorization WorkAuthorization) Conditions() ([]keyvalue.Condition, error) {
	if authorization.identity.Validate() != nil || authorization.identity.Executor != workexecution.Agent || len(authorization.conditions) != 4 {
		return nil, errs.New(errs.KindStateConflict, "authenticated Agent execution authority is missing")
	}
	return append([]keyvalue.Condition(nil), authorization.conditions...), nil
}

// ResolveWorkIdentity checks the authenticated credential generation and resolves
// the independently registered executor process at the same storage revision.
func (repository *Repository) ResolveWorkIdentity(ctx context.Context, agentID string,
	credentialGeneration uint64,
	processGeneration [16]byte,
) (authorization WorkAuthorization, err error) {
	if credentialGeneration == 0 || processGeneration == [16]byte{} {
		return authorization, agentCredentialNotFound()
	}
	binding, err := hostbindings.Read(ctx, repository.store, agentID, 0)
	if err != nil {
		return authorization, err
	}
	identity := workexecution.Identity{Executor: workexecution.Agent, HostID: binding.Binding.HostID}
	keys := []string{localagents.LocalAgentPrimaryKey(agentID), workqueue.ExecutorKey(identity)}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: binding.ReadRevision})
	if err != nil {
		return authorization, err
	}
	if read == nil || read.ReadRevision != binding.ReadRevision || len(read.Values) != len(keys) {
		return authorization, errs.New(errs.KindInternal, "Agent execution identity read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	for index, value := range read.Values {
		if value == nil || value.Key != keys[index] || value.ModRevision <= 0 || value.ModRevision > read.ReadRevision {
			return authorization, errs.New(errs.KindStateConflict, "Agent credential or executor registration is unavailable")
		}
	}
	credential, err := localagents.DecodeLocalAgentPrimary(read.Values[0].Value)
	if err != nil {
		return authorization, err
	}
	if credential.ID != agentID || credential.Generation != credentialGeneration || credential.Phase == localagents.LocalAgentPhaseDeleting {
		return authorization, agentCredentialNotFound()
	}
	executor, err := workqueue.DecodeExecutor(read.Values[1].Value)
	if err != nil {
		return authorization, err
	}
	identity = executor.Identity
	if identity.Executor != workexecution.Agent || identity.HostID != binding.Binding.HostID || identity.AgentID != agentID || executor.ProcessGeneration != processGeneration {
		return authorization, errs.New(errs.KindStateConflict, "Agent no longer owns this host executor")
	}
	conditions := append([]keyvalue.Condition(nil), binding.Conditions...)
	for _, value := range read.Values {
		conditions = append(conditions, keyvalue.Condition{Key: value.Key, ModRevision: value.ModRevision})
	}
	return WorkAuthorization{identity: identity, conditions: conditions}, nil
}
