package etcd

import (
	"context"
	"errors"
	"io/fs"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/AlanD20/groundplane/internal/common/config"
	"github.com/AlanD20/groundplane/internal/common/storagemaintenance"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const storageMaintenanceKey = "/v1/runtime/storage-maintenance"

type historySample struct {
	At       time.Time `json:"at"`
	Revision int64     `json:"revision"`
}
type maintenanceRecord struct {
	Status          storagemaintenance.Status `json:"status"`
	Samples         []historySample           `json:"samples"`
	ReservedThrough int64                     `json:"reserved_through"`
}

type StorageMaintenance struct {
	store        *store
	policy       config.MaintenancePolicy
	walDirectory string
	mu           sync.RWMutex
	projection   atomic.Pointer[storagemaintenance.Status]
	record       maintenanceRecord
}

func (s *store) loadMaintenanceFloor(ctx context.Context) error {
	read, err := s.Get(ctx, storageMaintenanceKey)
	if err != nil {
		return err
	}
	if read.Entry == nil {
		return nil
	}
	record, err := decodeMaintenanceRecord(read.Entry.Value)
	if err != nil {
		return err
	}
	s.compactedThrough.Store(record.ReservedThrough)
	return nil
}

func decodeMaintenanceRecord(value []byte) (maintenanceRecord, error) {
	record, err := recordcodec.Decode[maintenanceRecord](value, "storage-maintenance")
	if err != nil || len(record.Samples) > 128 || record.ReservedThrough < 0 ||
		record.Status.CompactedThrough > record.ReservedThrough {
		return maintenanceRecord{}, recordcodec.CorruptRecord()
	}
	var previous time.Time
	for _, sample := range record.Samples {
		if sample.Revision <= 0 || !sample.At.After(previous) {
			return maintenanceRecord{}, recordcodec.CorruptRecord()
		}
		previous = sample.At
	}
	return record, nil
}

func (maintenance *StorageMaintenance) Status() storagemaintenance.Status {
	current := maintenance.projection.Load()
	if current == nil {
		return storagemaintenance.Status{State: "waiting_for_first_pass"}
	}
	view := *current
	if view.Blocker != nil {
		pin := *view.Blocker
		view.Blocker = &pin
	}
	if view.Cleanup != nil {
		cleanup := *view.Cleanup
		view.Cleanup = &cleanup
	}
	return view
}

func (maintenance *StorageMaintenance) Policy() config.MaintenancePolicy { return maintenance.policy }

func (maintenance *StorageMaintenance) Tick(ctx context.Context, now time.Time) (resultErr error) {
	maintenance.mu.Lock()
	defer maintenance.mu.Unlock()
	bounded, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	ctx = bounded
	now = now.UTC()
	loaded := false
	defer func() {
		if resultErr != nil {
			maintenance.record.Status.State = "failed"
			maintenance.record.Status.LastFailureAt = &now
			maintenance.record.Status.FailureCode = "storage_maintenance_failed"
			maintenance.publishStatus()
			// A timed-out backend operation may have succeeded. Preserve its
			// reserved floor and diagnostic under a separate bounded cleanup.
			if loaded {
				cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
				defer cancel()
				if persistErr := maintenance.persist(cleanup); persistErr != nil {
					resultErr = errors.Join(resultErr, persistErr)
				}
			}
		}
	}()
	read, err := maintenance.store.Get(ctx, storageMaintenanceKey)
	if err != nil {
		return err
	}
	if read.Entry != nil {
		record, decodeErr := decodeMaintenanceRecord(read.Entry.Value)
		if decodeErr != nil {
			return decodeErr
		}
		maintenance.record = record
	}
	loaded = true
	endpoint := maintenance.store.backend.Endpoints()[0]
	status, err := maintenance.store.backend.Status(ctx, endpoint)
	if err != nil {
		return wrap(ctx, err)
	}
	view := &maintenance.record.Status
	view.State, view.ObservedAt = "idle", now
	view.BackendBytes, view.InUseBytes, view.QuotaBytes = status.DbSize, status.DbSizeInUse, status.DbSizeQuota
	view.HeadroomBytes = max(int64(0), view.QuotaBytes-view.BackendBytes)
	view.ReusableBytes = max(int64(0), view.BackendBytes-view.InUseBytes)
	view.WALBytes, err = directoryBytes(maintenance.walDirectory)
	if err != nil {
		return err
	}
	view.CurrentKeys, view.CurrentValueBytes, err = maintenance.currentPayload(ctx, status.Header.Revision)
	if err != nil {
		return err
	}
	view.TaskBacklog, view.DiagnosticBacklog, err = maintenance.cleanupBacklog(ctx, now, status.Header.Revision)
	if err != nil {
		return err
	}
	period := max(maintenance.policy.CleanupInterval, maintenance.policy.HistoryRetention/64)
	if len(maintenance.record.Samples) == 0 ||
		now.Sub(maintenance.record.Samples[len(maintenance.record.Samples)-1].At) >= period {
		maintenance.record.Samples = append(
			maintenance.record.Samples,
			historySample{At: now, Revision: status.Header.Revision},
		)
		if len(maintenance.record.Samples) > 128 {
			maintenance.record.Samples = maintenance.record.Samples[1:]
		}
	}
	var target int64
	for _, sample := range maintenance.record.Samples {
		if !sample.At.After(now.Add(-maintenance.policy.HistoryRetention)) {
			target = sample.Revision
		}
	}
	if maintenance.record.ReservedThrough > view.CompactedThrough {
		target = max(target, maintenance.record.ReservedThrough)
	}
	if target > view.CompactedThrough {
		maintenance.store.historyGate.Lock()
		// Capture the view after excluding new publications. A view from before
		// the barrier could miss a capture published while metrics were read.
		fresh, floorErr := maintenance.store.backend.Status(ctx, endpoint)
		var floor *storagemaintenance.Pin
		if floorErr == nil {
			floor, floorErr = maintenance.store.protectedHistoryFloor(ctx, fresh.Header.Revision)
		}
		if floorErr == nil {
			view.Blocker = floor
			view.ProtectedRevision = 0
			if floor != nil {
				view.ProtectedRevision = floor.Revision
				target = min(target, floor.Revision-1)
			}
			if target > view.CompactedThrough {
				maintenance.record.ReservedThrough = max(maintenance.record.ReservedThrough, target)
				view.State = "compacting"
				// Even a lost reservation response must exclude new old-view
				// captures. The next pass resolves its durable outcome before RPC.
				maintenance.store.compactedThrough.Store(maintenance.record.ReservedThrough)
				floorErr = maintenance.persist(ctx)
			} else {
				view.State = "blocked"
			}
		}
		maintenance.store.historyGate.Unlock()
		if floorErr != nil {
			return floorErr
		}
		if target > view.CompactedThrough {
			_, err := maintenance.store.backend.Compact(ctx, target, clientv3.WithCompactPhysical())
			if err != nil && !errors.Is(err, rpctypes.ErrCompacted) {
				return wrap(ctx, err)
			}
			view.CompactedThrough, view.LastSuccessAt, view.State = target, &now, "idle"
		}
	}
	status, err = maintenance.store.backend.Status(ctx, endpoint)
	if err != nil {
		return wrap(ctx, err)
	}
	view.BackendBytes, view.InUseBytes = status.DbSize, status.DbSizeInUse
	view.HeadroomBytes, view.ReusableBytes = max(
		int64(0),
		view.QuotaBytes-view.BackendBytes,
	), max(
		int64(0),
		view.BackendBytes-view.InUseBytes,
	)
	minute := now.Hour()*60 + now.Minute()
	windowMinute := (minute - maintenance.policy.DefragStartMinuteUTC + 24*60) % (24 * 60)
	inWindow := time.Duration(windowMinute)*time.Minute < maintenance.policy.DefragWindow
	alreadyDefragged := view.LastDefragAt != nil &&
		view.LastDefragAt.UTC().Format("2006-01-02") == now.Format("2006-01-02")
	if inWindow && !alreadyDefragged && view.ReusableBytes >= maintenance.policy.DefragMinimumBytes &&
		view.BackendBytes > 0 && view.ReusableBytes*100/view.BackendBytes >= int64(maintenance.policy.DefragMinimumPercent) {
		view.State = "defragmenting"
		if err := maintenance.persist(ctx); err != nil {
			return err
		}
		before := view.BackendBytes
		if _, err := maintenance.store.backend.Defragment(ctx, endpoint); err != nil {
			return wrap(ctx, err)
		}
		after, err := maintenance.store.backend.Status(ctx, endpoint)
		if err != nil {
			return wrap(ctx, err)
		}
		view.BackendBytes, view.InUseBytes = after.DbSize, after.DbSizeInUse
		view.ReclaimedBytes += max(int64(0), before-after.DbSize)
		view.LastDefragAt, view.LastSuccessAt, view.State = &now, &now, "idle"
		view.ReusableBytes = max(int64(0), after.DbSize-after.DbSizeInUse)
		view.HeadroomBytes = max(int64(0), view.QuotaBytes-view.BackendBytes)
	}
	view.FailureCode = ""
	return maintenance.persist(ctx)
}

func (maintenance *StorageMaintenance) persist(ctx context.Context) error {
	maintenance.publishStatus()
	value, err := recordcodec.Encode("storage-maintenance", maintenance.record)
	if err != nil {
		return err
	}
	defer clear(value)
	physical, err := maintenance.store.physicalKey(storageMaintenanceKey)
	if err != nil {
		return err
	}
	// This one fixed metadata write is the maintenance owner's publication. It
	// cannot introduce captured input and must not reenter its own write barrier.
	_, err = maintenance.store.client.Put(ctx, physical, string(value))
	return wrap(ctx, err)
}

func (maintenance *StorageMaintenance) publishStatus() {
	view := maintenance.record.Status
	if view.Blocker != nil {
		pin := *view.Blocker
		view.Blocker = &pin
	}
	maintenance.projection.Store(&view)
}

func (maintenance *StorageMaintenance) currentPayload(ctx context.Context, revision int64) (int64, int64, error) {
	var count, size int64
	start := ""
	for {
		page, err := maintenance.store.Range(
			ctx,
			keyvalue.RangeRequest{Prefix: "/v1/", StartExclusive: start, Revision: revision, Limit: 64},
		)
		if err != nil {
			return 0, 0, err
		}
		if page == nil || page.ReadRevision != revision || page.More && len(page.Values) == 0 {
			return 0, 0, recordcodec.CorruptRecord()
		}
		for _, item := range page.Values {
			count++
			size += int64(len(item.Key) + len(item.Value))
			start = item.Key
			clear(item.Value)
		}
		if !page.More {
			break
		}
	}
	return count, size, nil
}

func directoryBytes(path string) (int64, error) {
	var size int64
	err := filepath.WalkDir(path, func(_ string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type().IsRegular() {
			info, err := entry.Info()
			if err != nil {
				return err
			}
			size += info.Size()
		}
		return nil
	})
	return size, err
}
