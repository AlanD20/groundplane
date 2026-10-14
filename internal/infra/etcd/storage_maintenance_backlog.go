package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

// Backlog counts due candidates, not permission to delete their authority.
// Execution/recovery and source-owner guards remain with each cleanup operation.
func (maintenance *StorageMaintenance) cleanupBacklog(
	ctx context.Context,
	now time.Time,
	revision int64,
) (int64, int64, error) {
	var tasks, diagnostics int64
	cutoff := now.Add(taskjournal.TaskRetention - maintenance.policy.DiagnosticRetention)
	start := ""
	for {
		page, err := maintenance.store.Range(ctx, keyvalue.RangeRequest{
			Prefix: taskjournal.TaskRetentionIndexPrefix, StartExclusive: start, Revision: revision, Limit: 64,
		})
		if err != nil {
			return 0, 0, err
		}
		if page == nil || page.ReadRevision != revision || page.More && len(page.Values) == 0 {
			return 0, 0, recordcodec.CorruptRecord()
		}
		keys := []string{}
		finished := false
		for _, item := range page.Values {
			id, retainUntil, err := taskjournal.ParseTaskRetentionIndexKey(item.Key)
			if err != nil {
				keyvalue.ClearRangeValues(page.Values)
				return 0, 0, err
			}
			start = item.Key
			if retainUntil.After(cutoff) {
				finished = true
				break
			}
			if !retainUntil.After(now) {
				tasks++
			}
			keys = append(keys, taskjournal.TaskDiagnosticExpiryKey(id))
		}
		keyvalue.ClearRangeValues(page.Values)
		if len(keys) != 0 {
			read, err := maintenance.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
			if err != nil {
				return 0, 0, err
			}
			if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
				return 0, 0, recordcodec.CorruptRecord()
			}
			for _, item := range read.Values {
				if item == nil {
					diagnostics++
				}
			}
			keyvalue.ClearValues(read.Values)
		}
		if finished || !page.More {
			return tasks, diagnostics, nil
		}
	}
}
