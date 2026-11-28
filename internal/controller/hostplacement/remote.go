package hostplacement

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/remotehosts"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ResolveRemote includes the active enrollment in the captured publication
// authority. A local Agent registration never satisfies this request.
func (resolver *Resolver) ResolveRemote(ctx context.Context, hostID string, revision int64,
	operations ...workexecution.Operation,
) (Snapshot, error) {
	selected, err := resolver.Resolve(ctx, hostID, revision, operations...)
	if err != nil {
		return Snapshot{}, err
	}
	key := remotehosts.Key(hostID)
	read, err := resolver.reader.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return Snapshot{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return Snapshot{}, errs.New(errs.KindInternal, "remote host enrollment read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	if value == nil {
		return Snapshot{}, errs.New(errs.KindStateConflict, "selected host has no remote enrollment")
	}
	if value.Key != key || value.ModRevision <= 0 || value.ModRevision > revision {
		return Snapshot{}, errs.New(errs.KindInternal, "remote host enrollment revision is inconsistent")
	}
	host, err := remotehosts.DecodeHost(hostID, value.Value)
	if err != nil {
		return Snapshot{}, err
	}
	if host.Agent == nil || host.Agent.AgentID != selected.Identity.AgentID {
		return Snapshot{}, errs.New(errs.KindStateConflict, "selected host executor is not the active remote Agent")
	}
	selected.Conditions = append(selected.Conditions, keyvalue.Condition{Key: key, ModRevision: value.ModRevision})
	return selected, nil
}
