// Package hostbindings owns stable host-to-Agent ownership, independently of
// credential rotation, executor processes and retained execution history.
package hostbindings

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Binding struct {
	HostID  string `json:"host_id"`
	AgentID string `json:"agent_id"`
}

func AgentKey(agentID string) string { return "/v1/host-bindings/agents/" + agentID }
func HostKey(hostID string) string   { return "/v1/host-bindings/hosts/" + hostID }

func (binding Binding) Validate() error {
	if ids.Validate(ids.KindHost, binding.HostID) != nil || ids.Validate(ids.KindAgent, binding.AgentID) != nil {
		return errs.New(errs.KindValidationFailed, "host binding requires stable host and Agent identities")
	}
	return nil
}

func Decode(value []byte) (Binding, error) {
	binding, err := recordcodec.Decode[Binding](value, "agent-host-binding")
	if err != nil || binding.Validate() != nil {
		return Binding{}, errs.New(errs.KindInternal, "Agent host binding is corrupt")
	}
	return binding, nil
}

// PrepareCreation must join enrollment and credential publication. Neither side
// can be replaced by overwriting a binding: host reassignment requires explicit
// lifecycle ownership and accounting for outstanding execution.
func PrepareCreation(binding Binding) (workqueue.Change, error) {
	if err := binding.Validate(); err != nil {
		return workqueue.Change{}, err
	}
	value, err := recordcodec.Encode("agent-host-binding", binding)
	if err != nil {
		return workqueue.Change{}, err
	}
	return workqueue.Change{
		Conditions: []keyvalue.Condition{{Key: AgentKey(binding.AgentID)}, {Key: HostKey(binding.HostID)}},
		Mutations: []keyvalue.Mutation{
			{Type: keyvalue.MutationPut, Key: AgentKey(binding.AgentID), Value: value},
			{Type: keyvalue.MutationPut, Key: HostKey(binding.HostID), Value: append([]byte(nil), value...)},
		},
	}, nil
}

type Reader interface {
	GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error)
}

type Snapshot struct {
	Binding      Binding
	Conditions   []keyvalue.Condition
	ReadRevision int64
}

// Read resolves both directions at one revision. A forward mapping without its
// matching host owner is invalid, not permission to repair ownership on a read.
func Read(ctx context.Context, store Reader, agentID string, revision int64) (Snapshot, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return Snapshot{}, err
	}
	if store == nil || ids.Validate(ids.KindAgent, agentID) != nil || revision < 0 {
		return Snapshot{}, errs.New(errs.KindValidationFailed, "host binding read is invalid")
	}
	key := AgentKey(agentID)
	read, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return Snapshot{}, err
	}
	if read == nil || read.ReadRevision <= 0 || revision > 0 && read.ReadRevision != revision || len(read.Values) != 1 {
		return Snapshot{}, errs.New(errs.KindInternal, "host binding read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	if value == nil || value.Key != key || value.ModRevision <= 0 || value.ModRevision > read.ReadRevision {
		return Snapshot{}, errs.New(errs.KindStateConflict, "Agent has no durable host binding")
	}
	binding, err := Decode(value.Value)
	if err != nil || binding.AgentID != agentID {
		return Snapshot{}, errs.New(errs.KindInternal, "Agent host binding identity is inconsistent")
	}
	hostKey := HostKey(binding.HostID)
	host, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{hostKey}, Revision: read.ReadRevision})
	if err != nil {
		return Snapshot{}, err
	}
	if host == nil || host.ReadRevision != read.ReadRevision || len(host.Values) != 1 {
		return Snapshot{}, errs.New(errs.KindInternal, "host owner read is incomplete")
	}
	defer keyvalue.ClearValues(host.Values)
	owner := host.Values[0]
	if owner == nil || owner.Key != hostKey || owner.ModRevision != value.ModRevision {
		return Snapshot{}, errs.New(errs.KindStateConflict, "host ownership does not match its Agent binding")
	}
	reverse, err := Decode(owner.Value)
	if err != nil || reverse != binding {
		return Snapshot{}, errs.New(errs.KindInternal, "host ownership identity is inconsistent")
	}
	return Snapshot{Binding: binding, ReadRevision: read.ReadRevision, Conditions: []keyvalue.Condition{
		{Key: key, ModRevision: value.ModRevision}, {Key: hostKey, ModRevision: owner.ModRevision},
	}}, nil
}
