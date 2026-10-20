package etcd

import (
	"context"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// CheckNewMutation reserves backend space for accepted work to settle. Check the
// live backend, not the maintenance projection or reusable-but-unreclaimed pages.
// This is admission control, not a replacement for etcd's hard quota.
func (maintenance *StorageMaintenance) CheckNewMutation(ctx context.Context) error {
	if maintenance == nil || maintenance.store == nil || maintenance.store.backend == nil {
		return errs.New(errs.KindStorageUnavailable, "Storage admission is unavailable; new changes are paused")
	}
	bounded, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	backend := maintenance.store.backend
	status, err := backend.Status(bounded, backend.Endpoints()[0])
	if err != nil {
		return errs.Wrap(errs.KindStorageUnavailable, err)
	}
	if status.DbSizeQuota <= 0 || status.DbSize < 0 {
		return errs.New(errs.KindStorageUnavailable, "Storage quota could not be determined; new changes are paused")
	}
	// Keep ten percent, bounded to 8–64 MiB. Ordinary transaction size limits
	// remain unchanged; existing work, reports and cleanup never use this gate.
	reserve := min(int64(64<<20), max(int64(8<<20), status.DbSizeQuota/10))
	if status.DbSizeQuota-status.DbSize <= reserve {
		return errs.Newf(
			errs.KindStorageUnavailable,
			"New changes are paused: etcd has %d bytes free and needs more than %d bytes reserved for existing work. Allow retention and compaction to finish, then retry",
			max(int64(0), status.DbSizeQuota-status.DbSize),
			reserve,
		)
	}
	return nil
}

// Also gate publishers that do not enter through HTTP (scheduled work and
// background reconciliation). Positive-revision updates belong to accepted
// Tasks, including Retry, recovery and bounded publication of their work graph.
func (s *store) admitNewTaskPublication(
	ctx context.Context,
	conditions []keyvalue.Condition,
	mutations []keyvalue.Mutation,
) error {
	if s.maintenance == nil {
		return nil // The client-only store has no managed backend quota.
	}
	for _, condition := range conditions {
		if condition.ModRevision != 0 || condition.Prefix ||
			!strings.HasPrefix(condition.Key, "/v1/execution/tasks/") || !strings.HasSuffix(condition.Key, "/record") {
			continue
		}
		for _, mutation := range mutations {
			if mutation.Key != condition.Key || mutation.Type != keyvalue.MutationPut {
				continue
			}
			task, err := workqueue.DecodeTask(mutation.Value)
			if err != nil {
				return err
			}
			if task.Actor == workexecution.SystemActor && task.Action == workexecution.ActionBackupPrune {
				continue // Bounded retention must be able to reclaim storage.
			}
			return s.maintenance.CheckNewMutation(ctx)
		}
	}
	return nil
}
