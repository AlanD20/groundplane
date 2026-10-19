package workqueue

import (
	"context"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
)

const inputQueuePrefix = root + "waiting-input/"

func inputQueueKey(id string) string { return inputQueuePrefix + id }

func waitingInput(item workexecution.Item) bool {
	return item.Phase == workexecution.Waiting && item.Wait != nil &&
		item.Wait.Reason == workexecution.WaitInput && item.Input == nil && !item.ForwardRevoked
}

// Input preparation has no executor slot. This index follows work state in the
// same transaction, including Abort and unstarted settlement.
func updateInputQueue(change *Change, before, after workexecution.Item) error {
	previous, next := waitingInput(before), waitingInput(after)
	if previous == next {
		return nil
	}
	key := inputQueueKey(after.ID)
	if !next {
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: key})
		return nil
	}
	value, err := encode("execution_owner", ClaimOwner{TaskID: after.TaskID, WorkItemID: after.ID})
	if err != nil {
		return err
	}
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: key})
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: value})
	return nil
}

// ScanInput returns a bounded page. Advance the cursor even when prerequisites
// are not ready, so one waiting branch cannot starve later work.
func (repository *Repository) ScanInput(ctx context.Context, after string) (QueuePage, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return QueuePage{}, err
	}
	if after != "" && ids.Validate(ids.KindWorkItem, after) != nil {
		return QueuePage{}, invalid("input queue cursor is invalid")
	}
	request := keyvalue.RangeRequest{Prefix: inputQueuePrefix, Limit: 64}
	if after != "" {
		request.StartExclusive = inputQueueKey(after)
	}
	read, err := repository.store.Range(ctx, request)
	if err != nil {
		return QueuePage{}, err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) > 64 || read.More && len(read.Values) == 0 {
		return QueuePage{}, corrupt()
	}
	defer keyvalue.ClearRangeValues(read.Values)
	page := QueuePage{More: read.More}
	for _, value := range read.Values {
		if !strings.HasPrefix(value.Key, inputQueuePrefix) || value.ModRevision <= 0 ||
			value.ModRevision > read.ReadRevision {
			return QueuePage{}, corrupt()
		}
		id := strings.TrimPrefix(value.Key, inputQueuePrefix)
		owner, err := recordcodec.Decode[ClaimOwner](value.Value, "execution_owner")
		if err != nil || validateOwner(owner) != nil || owner.WorkItemID != id ||
			ids.Validate(ids.KindWorkItem, id) != nil || id <= after || page.LastID != "" && id <= page.LastID {
			return QueuePage{}, corrupt()
		}
		page.Entries = append(
			page.Entries,
			QueueEntry{TaskID: owner.TaskID, WorkItemID: id, Revision: value.ModRevision},
		)
		page.LastID = id
	}
	return page, nil
}
