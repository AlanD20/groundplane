package workqueue

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// HostUpgradeWindow distinguishes retained recovery ownership from occupied
// execution capacity. It does not prove that host processes have stopped: the
// caller must also obtain fresh executor evidence while assignments are paused.
// Removal continues to require HostIdle and complete effect settlement.
func (repository *Repository) HostUpgradeWindow(
	ctx context.Context,
	hostID string,
	revision int64,
) (bool, bool, error) {
	if ids.Validate(ids.KindHost, hostID) != nil || revision <= 0 {
		return false, false, invalid("host upgrade read requires a fixed binding revision")
	}
	read, err := repository.store.Range(ctx, keyvalue.RangeRequest{
		Prefix: SlotsPrefix(workexecution.Identity{Executor: workexecution.Agent, HostID: hostID}),
		Limit:  1, Revision: revision,
	})
	if err != nil {
		return false, false, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) > 1 {
		return false, false, corrupt()
	}
	busy := len(read.Values) != 0 || read.More
	keyvalue.ClearRangeValues(read.Values)
	if busy {
		return false, false, nil
	}
	retained, after := false, ""
	for {
		page, err := repository.HostActivity(ctx, hostID, after, 64, revision)
		if err != nil {
			return false, false, err
		}
		for _, view := range page.Work {
			retained = true
			item := view.Item.Item
			if item.Operation != workexecution.EntryConfiguration || item.Phase != workexecution.Attention ||
				item.ActiveAttemptID != "" || item.Input == nil ||
				(item.Cleanup != workexecution.CleanupRequired && item.Cleanup != workexecution.CleanupStopped) {
				return false, retained, nil
			}
			_, latest, err := repository.ReadAttempt(ctx, item.TaskID, item.ID, item.AttemptNumber, revision)
			if err != nil {
				return false, false, err
			}
			attempt := latest.Attempt
			if attempt.FinishedAt == nil || attempt.Result == nil || attempt.Input != *item.Input ||
				attempt.Executor.HostID != hostID {
				return false, retained, nil
			}
		}
		if !page.More {
			return true, retained, nil
		}
		after = page.LastID
	}
}
