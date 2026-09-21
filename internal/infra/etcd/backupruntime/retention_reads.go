package backupruntime

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *Reader) GetBackupRetentionSweep(
	ctx context.Context,
	sourceID string,
	triggerRecoveryPointID string,
) (etcdstore.Versioned[BackupRetentionSweepRecord], bool, error) {
	if err := recordcodec.ValidateID(ids.KindBackupSource, sourceID); err != nil {
		return etcdstore.Versioned[BackupRetentionSweepRecord]{}, false, err
	}
	if err := recordcodec.ValidateID(ids.KindRecoveryPoint, triggerRecoveryPointID); err != nil {
		return etcdstore.Versioned[BackupRetentionSweepRecord]{}, false, err
	}
	record, found, err := getOptionalBackupRuntimeRecord(
		ctx,
		repository.store,
		BackupRetentionKey(sourceID, triggerRecoveryPointID),
		triggerRecoveryPointID,
		DecodeBackupRetentionSweepRecord,
		func(record BackupRetentionSweepRecord) string {
			if record.SourceID != sourceID {
				return ""
			}
			return record.TriggerRecoveryPointID
		},
	)
	if err != nil || !found {
		return record, found, err
	}
	authority, err := repository.ReadFixedKeys(
		ctx,
		[]string{
			BackupRetentionKey(sourceID, triggerRecoveryPointID),
			BackupRecoveryPointKey(triggerRecoveryPointID),
		},
		record.ReadRevision,
	)
	if err != nil {
		return etcdstore.Versioned[BackupRetentionSweepRecord]{}, false, err
	}
	defer etcdstore.ClearValues(authority.Values)
	if authority.Values[0] == nil || authority.Values[1] == nil ||
		authority.Values[0].ModRevision != record.Revision || authority.Values[1].Version != 1 {
		return etcdstore.Versioned[BackupRetentionSweepRecord]{}, false, CorruptBackupRuntimeRecord()
	}
	point, err := DecodeBackupRecoveryPointRecord(authority.Values[1].Value)
	if err != nil || point.ID != triggerRecoveryPointID || point.SourceID != sourceID {
		return etcdstore.Versioned[BackupRetentionSweepRecord]{}, false, CorruptBackupRuntimeRecord()
	}
	if record.Record.State == BackupRetentionPending {
		if authority.Values[0].Version != 1 || authority.Values[0].ModRevision != authority.Values[1].ModRevision {
			return etcdstore.Versioned[BackupRetentionSweepRecord]{}, false, CorruptBackupRuntimeRecord()
		}
	} else if authority.Values[0].Version < 2 ||
		authority.Values[0].ModRevision <= authority.Values[1].ModRevision {
		return etcdstore.Versioned[BackupRetentionSweepRecord]{}, false, CorruptBackupRuntimeRecord()
	}
	return record, true, nil
}

func (repository *Reader) ListBackupRetentionSweepsBySource(
	ctx context.Context,
	sourceID string,
	request BackupRuntimeListRequest,
) (BackupRuntimePage[BackupRetentionSweepRecord], error) {
	if err := recordcodec.ValidateID(ids.KindBackupSource, sourceID); err != nil {
		return BackupRuntimePage[BackupRetentionSweepRecord]{}, err
	}
	prefix := BackupRetentionPrefix + sourceID + "/"
	if err := ValidateBackupRuntimeListRequest(prefix, request); err != nil {
		return BackupRuntimePage[BackupRetentionSweepRecord]{}, err
	}
	result, err := repository.store.Range(ctx, etcdstore.RangeRequest{
		Prefix: prefix, StartExclusive: request.StartExclusive,
		Limit: int64(request.Limit), Revision: request.Revision,
	})
	if err != nil {
		return BackupRuntimePage[BackupRetentionSweepRecord]{}, err
	}
	if result == nil || result.ReadRevision <= 0 {
		return BackupRuntimePage[BackupRetentionSweepRecord]{}, errs.New(
			errs.KindInternal,
			"backup retention page is incomplete",
		)
	}
	defer etcdstore.ClearRangeValues(result.Values)
	page := BackupRuntimePage[BackupRetentionSweepRecord]{
		Items:    make([]etcdstore.Versioned[BackupRetentionSweepRecord], len(result.Values)),
		Revision: result.ReadRevision,
	}
	for index, item := range result.Values {
		record, decodeErr := DecodeBackupRetentionSweepRecord(item.Value)
		if decodeErr != nil || record.SourceID != sourceID ||
			item.Key != BackupRetentionKey(sourceID, record.TriggerRecoveryPointID) {
			return BackupRuntimePage[BackupRetentionSweepRecord]{}, CorruptBackupRuntimeRecord()
		}
		page.Items[index] = etcdstore.Versioned[BackupRetentionSweepRecord]{
			Record: record, Revision: item.ModRevision, ReadRevision: result.ReadRevision,
		}
	}
	if result.More {
		page.Next = result.Values[len(result.Values)-1].Key
	}
	return page, nil
}
