package workqueue

import (
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func readHostEffect(value *keyvalue.KeyValue, item workexecution.Item, required bool) (HostEffectSnapshot, error) {
	if value == nil {
		if required {
			return HostEffectSnapshot{}, errs.New(errs.KindStateConflict, "host work still needs its retained effect protection")
		}
		return HostEffectSnapshot{}, nil
	}
	if value.Key != HostEffectKey(item.Placement.HostID, item.ID) || value.ModRevision <= 0 {
		return HostEffectSnapshot{}, corrupt()
	}
	owner, err := recordcodec.Decode[ClaimOwner](value.Value, "execution_host_effect")
	if err != nil || owner != (ClaimOwner{TaskID: item.TaskID, WorkItemID: item.ID}) {
		return HostEffectSnapshot{}, corrupt()
	}
	return HostEffectSnapshot{Owner: &owner, Revision: value.ModRevision}, nil
}

func assignHostEffect(change *Change, item workexecution.Item, snapshot HostEffectSnapshot, direction workexecution.Direction) error {
	if item.Placement.Executor != workexecution.Agent {
		if snapshot.Revision != 0 || snapshot.Owner != nil {
			return invalid("host-independent work cannot own host-effect membership")
		}
		return nil
	}
	owner := ClaimOwner{TaskID: item.TaskID, WorkItemID: item.ID}
	if direction == workexecution.Recovery {
		if snapshot.Revision <= 0 || snapshot.Owner == nil || *snapshot.Owner != owner {
			return errs.New(errs.KindStateConflict, "recovery lost its host-effect protection")
		}
		change.Conditions = append(change.Conditions, keyvalue.Condition{Key: HostEffectKey(item.Placement.HostID, item.ID), ModRevision: snapshot.Revision})
		return nil
	}
	if snapshot.Revision != 0 || snapshot.Owner != nil {
		return errs.New(errs.KindStateConflict, "forward work still has unresolved host effects")
	}
	raw, err := encode("execution_host_effect", owner)
	if err != nil {
		return err
	}
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: HostEffectKey(item.Placement.HostID, item.ID)},
		keyvalue.Condition{Key: HostForwardFenceKey(item.Placement.HostID)})
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: HostEffectKey(item.Placement.HostID, item.ID), Value: raw})
	return nil
}

func completeHostEffect(change *Change, item workexecution.Item, snapshot HostEffectSnapshot) error {
	if item.Placement.Executor != workexecution.Agent {
		if snapshot.Revision != 0 || snapshot.Owner != nil {
			return invalid("host-independent completion cannot retire host effects")
		}
		return nil
	}
	if snapshot.Revision <= 0 || snapshot.Owner == nil || *snapshot.Owner != (ClaimOwner{TaskID: item.TaskID, WorkItemID: item.ID}) {
		return errs.New(errs.KindStateConflict, "host-effect completion does not own its retained protection")
	}
	key := HostEffectKey(item.Placement.HostID, item.ID)
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: key, ModRevision: snapshot.Revision})
	if item.Phase == workexecution.Settled {
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: key})
	}
	return nil
}
