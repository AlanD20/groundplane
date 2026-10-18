package networkwork

import (
	"bytes"
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// ContinueCascadeRejection settles at most one never-executed graph member.
// A committed batch returns advanced so the caller reloads Task counters. Only
// the final pass returns a graph and fence release for rejected publication.
func (repository *InputRepository) ContinueCascadeRejection(ctx context.Context, task workqueue.TaskSnapshot,
	graph []workexecution.Item, authority workqueue.Change, revision int64, at time.Time,
) (bool, []workexecution.Item, workqueue.Change, error) {
	rejection, evidence, proof, found, err := repository.LoadCascadeRejection(ctx, task, revision)
	if err != nil {
		return false, nil, workqueue.Change{}, err
	}
	if !found || task.Task.PublicationComplete || at.Before(task.Task.UpdatedAt) || len(authority.Conditions) == 0 {
		return false, nil, workqueue.Change{}, conflict("publication cleanup requires its saved rejection and closed graph")
	}
	if err := workexecution.ValidateGraph(task.Task, graph); err != nil {
		return false, nil, workqueue.Change{}, err
	}
	authority.Conditions = append(authority.Conditions, proof.Conditions...)
	saved := make(map[string]workqueue.ItemSnapshot)
	after := ""
	for {
		page, err := repository.transactions.ListWork(ctx, task.Task.ID, after, 100, revision)
		if err != nil {
			return false, nil, workqueue.Change{}, err
		}
		for _, item := range page.Items {
			if _, duplicate := saved[item.Item.ID]; duplicate {
				return false, nil, workqueue.Change{}, conflict("rejected publication has duplicate membership")
			}
			saved[item.Item.ID] = item
		}
		if !page.More {
			break
		}
		if len(page.Items) == 0 || page.LastID <= after {
			return false, nil, workqueue.Change{}, conflict("rejected publication membership did not advance")
		}
		after = page.LastID
	}
	if uint64(len(saved)) != task.Task.Progress.Total() {
		return false, nil, workqueue.Change{}, conflict("rejected publication counters differ from membership")
	}
	completion := workexecution.Completion{Outcome: rejection.Outcome, Evidence: evidence, EffectsSettled: true}
	settled := make([]workexecution.Item, 0, len(graph))
	var pending *workqueue.ItemSnapshot
	var bridge workqueue.ItemSnapshot
	for _, captured := range graph {
		current, exists := saved[captured.ID]
		delete(saved, captured.ID)
		if !exists {
			current = workqueue.ItemSnapshot{Item: captured}
		}
		expected := captured
		if current.Item.Phase == workexecution.Settled {
			expected, err = workexecution.SettleUnstarted(captured, completion, current.Item.UpdatedAt)
			if err != nil {
				return false, nil, workqueue.Change{}, err
			}
		}
		left, leftErr := workqueue.EncodeItem(expected)
		right, rightErr := workqueue.EncodeItem(current.Item)
		same := leftErr == nil && rightErr == nil && bytes.Equal(left, right)
		clear(left)
		clear(right)
		if !same {
			return false, nil, workqueue.Change{}, conflict("rejected publication contains changed or executed work")
		}
		if current.Item.Phase != workexecution.Settled && pending == nil {
			copy := current
			pending = &copy
		}
		settled = append(settled, current.Item)
		if captured.Operation == workexecution.Network {
			bridge = current
		}
	}
	if len(saved) != 0 {
		return false, nil, workqueue.Change{}, conflict("rejected publication has work outside its captured graph")
	}
	if pending != nil {
		view := workqueue.View{Task: task, Item: *pending, ReadRevision: revision}
		var resource workqueue.Change
		if pending.Item.Operation == workexecution.Detach {
			inputs, err := attachremoval.NewInputRepository(repository.store)
			if err != nil {
				return false, nil, workqueue.Change{}, err
			}
			if pending.Revision == 0 {
				done, err := inputs.AbandonCascadePreparation(ctx, view, authority.Conditions)
				if err != nil {
					return false, nil, workqueue.Change{}, err
				}
				if !done {
					return true, nil, workqueue.Change{}, nil
				}
			}
			resource, err = inputs.PrepareCascadePublicationRejection(ctx, view)
			if err != nil {
				return false, nil, workqueue.Change{}, err
			}
		}
		defer keyvalue.ClearMutationValues(resource.Mutations)
		change, err := workqueue.PreparePublicationSettlement(task, *pending, completion, at)
		if err != nil {
			return false, nil, workqueue.Change{}, err
		}
		defer keyvalue.ClearMutationValues(change.Mutations)
		result, err := repository.transactions.Commit(ctx, authority, resource, change)
		keyvalue.ClearValues(result.FailureReads)
		if err != nil {
			return false, nil, workqueue.Change{}, err
		}
		if !result.Succeeded {
			return false, nil, workqueue.Change{}, conflict("rejected publication ownership changed during cleanup")
		}
		return true, nil, workqueue.Change{}, nil
	}
	fenceKey := deletions.TombstoneKey(string(deletions.DeletionTargetZone), task.Task.TargetID)
	authorityKey := RemovalAuthorityKey(bridge.Item.ID)
	read, err := repository.read(ctx, []string{fenceKey, authorityKey}, revision)
	if err != nil {
		return false, nil, workqueue.Change{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil || read.Values[1] == nil {
		return false, nil, workqueue.Change{}, conflict("rejected publication lost its original Zone fence")
	}
	fence, fenceErr := deletions.DecodeDeletionTombstone(read.Values[0].Value)
	original, originalErr := deletions.DecodeDeletionTombstone(read.Values[1].Value)
	view := workqueue.View{Task: task, Item: bridge, ReadRevision: revision}
	if fenceErr != nil || originalErr != nil || !matchesRemovalFence(view, fence) || !matchesRemovalFence(view, original) ||
		fence.TargetRevision != original.TargetRevision {
		return false, nil, workqueue.Change{}, conflict("rejected publication no longer owns the Zone fence")
	}
	authority.Conditions = append(authority.Conditions,
		keyvalue.Condition{Key: fenceKey, ModRevision: read.Values[0].ModRevision},
		keyvalue.Condition{Key: authorityKey, ModRevision: read.Values[1].ModRevision})
	authority.Mutations = append(authority.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: fenceKey})
	return false, settled, authority, nil
}
