package workqueue

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
)

// OccupiedSlots reads compact execution ownership, including older reservations
// on this host. It loads no Task, captured input or retained diagnostic history.
// A slot is neither a live-process observation nor proof of unresolved effects;
// resource claims and helper reconciliation have their own owners.
func (repository *Repository) OccupiedSlots(
	ctx context.Context,
	target workexecution.Placement,
	revision int64,
) (int, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return 0, err
	}
	if target.Validate() != nil || revision < 0 {
		return 0, invalid("executor capacity target is invalid")
	}
	prefix := slotsPrefix(target)
	read, err := repository.store.Range(
		ctx,
		keyvalue.RangeRequest{Prefix: prefix, Limit: maximumExecutorCapacity + 1, Revision: revision},
	)
	if err != nil {
		return 0, err
	}
	if read == nil || read.ReadRevision <= 0 || revision > 0 && read.ReadRevision != revision || read.More ||
		len(read.Values) > maximumExecutorCapacity {
		return 0, corrupt()
	}
	defer keyvalue.ClearRangeValues(read.Values)
	seen := make(map[string]bool, len(read.Values))
	for _, entry := range read.Values {
		index, err := strconv.Atoi(strings.TrimPrefix(entry.Key, prefix))
		if err != nil || index < 0 || index >= maximumExecutorCapacity ||
			entry.Key != prefix+fmt.Sprintf("%03d", index) ||
			entry.ModRevision <= 0 ||
			entry.ModRevision > read.ReadRevision {
			return 0, corrupt()
		}
		slot, err := recordcodec.Decode[Slot](entry.Value, "execution_slot")
		if err != nil || ids.Validate(ids.KindTask, slot.TaskID) != nil ||
			ids.Validate(ids.KindWorkItem, slot.WorkItemID) != nil ||
			ids.Validate(ids.KindAttempt, slot.AttemptID) != nil ||
			seen[slot.WorkItemID] {
			return 0, corrupt()
		}
		seen[slot.WorkItemID] = true
	}
	return len(read.Values), nil
}
