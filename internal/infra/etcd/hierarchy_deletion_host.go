package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/localagents"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Capture the MVP placement at admission. Later Agent replacement may change
// the process, but cannot silently retarget deletion to a different machine.
func (repository *HierarchyDeletionRepository) captureDeletionHost(
	ctx context.Context,
	revision int64,
) (string, []keyvalue.Condition, error) {
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{localagents.LocalAgentSingletonKey}, Revision: revision},
	)
	if err != nil {
		return "", nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return "", nil, errs.New(errs.KindInternal, "hierarchy host capture is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	if value == nil {
		return "", []keyvalue.Condition{{Key: localagents.LocalAgentSingletonKey}}, nil
	}
	agentID, err := localagents.DecodeLocalAgentReference(value.Value)
	if err != nil {
		return "", nil, err
	}
	binding, err := hostbindings.Read(ctx, repository.store, agentID, revision)
	if err != nil {
		return "", nil, err
	}
	conditions := append(
		binding.Conditions,
		keyvalue.Condition{Key: localagents.LocalAgentSingletonKey, ModRevision: value.ModRevision},
	)
	return binding.Binding.HostID, conditions, nil
}
