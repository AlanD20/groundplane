package etcd

import (
	"context"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/storagemaintenance"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Sealed captures and immutable runtime manifests read retained bytes at a
// fresh fixed view. Only unfinished capture/planning needs mutable old MVCC
// values. Add a producer here when introducing another durable old-view owner.
func historicalReference(key string, value []byte) (*storagemaintenance.Pin, error) {
	if strings.HasPrefix(key, releaserender.ServiceRolloutPrefix) {
		return releasePreparationHistoryReference(key, value)
	}
	const snapshots = "/v1/runtime/backup-config-snapshots/"
	if strings.HasPrefix(key, snapshots) && !strings.Contains(strings.TrimPrefix(key, snapshots), "/") {
		snapshot, err := backupconfiguration.DecodeBackupConfigSnapshotRecord(value)
		if err != nil {
			return nil, err
		}
		if snapshot.State == backupconfiguration.BackupConfigSnapshotBuilding {
			return &storagemaintenance.Pin{
				Kind:     "backup_config_capture",
				OwnerID:  snapshot.SnapshotID,
				Revision: snapshot.ReadRevision,
			}, nil
		}
	}
	const tombstones = "/v1/runtime/deletions/"
	if strings.HasPrefix(key, tombstones) {
		parts := strings.Split(strings.TrimPrefix(key, tombstones), "/")
		if len(parts) != 2 {
			return nil, recordcodec.CorruptRecord()
		}
		switch parts[0] {
		case "tenant", "project", "environment", "backing-service":
			var tombstone hierarchydeletion.HierarchyDeletionTombstone
			if err := hierarchydeletion.DecodeHierarchyDeletionRecord(value, hierarchydeletion.HierarchyDeletionSmallRecordBytes, &tombstone); err != nil {
				return nil, err
			}
			if tombstone.Schema != 1 || tombstone.TargetID != parts[1] || tombstone.SnapshotRevision <= 0 {
				return nil, recordcodec.CorruptRecord()
			}
			if tombstone.PlanCount == nil {
				return &storagemaintenance.Pin{
					Kind:     "hierarchy_deletion_planning",
					OwnerID:  tombstone.CurrentTaskID,
					Revision: tombstone.SnapshotRevision,
				}, nil
			}
		}
	}
	return blueprintHistoricalReference(key, value)
}

func (s *store) validateHistoricalPublication(mutations []keyvalue.Mutation) error {
	floor := s.compactedThrough.Load()
	for _, mutation := range mutations {
		if mutation.Type != keyvalue.MutationPut {
			continue
		}
		pin, err := historicalReference(mutation.Key, mutation.Value)
		if err != nil {
			return err
		}
		if pin != nil && pin.Revision <= floor {
			return errs.New(
				errs.KindCursorExpired,
				"captured storage revision expired before publication; prepare the operation again",
			)
		}
	}
	return nil
}

func (s *store) protectedHistoryFloor(ctx context.Context, revision int64) (*storagemaintenance.Pin, error) {
	var oldest *storagemaintenance.Pin
	for _, prefix := range []string{"/v1/runtime/backup-config-snapshots/", "/v1/runtime/deletions/", blueprintHistorySourcePrefix, releaserender.ServiceRolloutPrefix} {
		start := ""
		for {
			page, err := s.Range(
				ctx,
				keyvalue.RangeRequest{Prefix: prefix, StartExclusive: start, Revision: revision, Limit: 64},
			)
			if err != nil {
				return nil, err
			}
			if page == nil || page.ReadRevision != revision || page.More && len(page.Values) == 0 {
				return nil, recordcodec.CorruptRecord()
			}
			for _, item := range page.Values {
				start = item.Key
				pin, err := historicalReference(item.Key, item.Value)
				if err != nil {
					keyvalue.ClearRangeValues(page.Values)
					return nil, err
				}
				if pin != nil {
					held, err := s.historicalReferenceHeld(ctx, pin, revision)
					if err != nil {
						keyvalue.ClearRangeValues(page.Values)
						return nil, err
					}
					if held && (oldest == nil || pin.Revision < oldest.Revision) {
						oldest = pin
					}
				}
			}
			keyvalue.ClearRangeValues(page.Values)
			if !page.More {
				break
			}
		}
	}
	return oldest, nil
}
