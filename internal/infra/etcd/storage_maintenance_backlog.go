package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/executiondiagnostics"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// Backlog counts native due candidates, not permission to delete authority.
// Every collection decision remains with its operation owner.
func (maintenance *StorageMaintenance) cleanupBacklog(ctx context.Context, now time.Time, revision int64) (int64, int64, error) {
	queue, err := workqueue.NewRepository(maintenance.store)
	if err != nil {
		return 0, 0, err
	}
	var tasks, diagnostics int64
	after := ""
	for {
		page, err := queue.RetentionPageAt(ctx, after, now.Add(workexecution.TaskHistoryRetention), revision)
		if err != nil {
			return 0, 0, err
		}
		for _, candidate := range page.Candidates {
			task := candidate.Task.Task
			if !task.RetainUntil.After(now) {
				tasks++
			}
			if task.CollectionStartedAt != nil || now.Before(task.FinishedAt.Add(maintenance.policy.DiagnosticRetention)) {
				continue
			}
			expired, err := executiondiagnostics.TaskExpired(ctx, maintenance.store, candidate.Task, revision)
			if err != nil {
				return 0, 0, err
			}
			if !expired {
				diagnostics++
			}
		}
		if page.Next == "" {
			return tasks, diagnostics, nil
		}
		after = page.Next
	}
}
