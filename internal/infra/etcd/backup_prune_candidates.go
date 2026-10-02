package etcd

import (
	"context"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const backupPruneCandidatePageSize = 64

// BackupPruneCandidatePage is a bounded global scan. A caller carries Next to
// the following tick; an empty Next starts again at the first tombstone.
type BackupPruneCandidatePage struct {
	Pending []etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord]
	Next    string
}

func (repository *BackupRuntimeRepository) ListPendingBackupPruneCandidates(
	ctx context.Context, startExclusive string,
) (BackupPruneCandidatePage, error) {
	if repository == nil || repository.store == nil ||
		(startExclusive != "" && (!strings.HasPrefix(startExclusive, backupruntime.BackupRecoveryPointPrunePrefix) ||
			ids.Validate(ids.KindRecoveryPoint, strings.TrimPrefix(startExclusive, backupruntime.BackupRecoveryPointPrunePrefix)) != nil)) {
		return BackupPruneCandidatePage{}, errs.New(errs.KindValidationFailed, "backup prune continuation is invalid")
	}
	read, err := repository.store.Range(ctx, etcdstore.RangeRequest{
		Prefix:         backupruntime.BackupRecoveryPointPrunePrefix,
		StartExclusive: startExclusive, Limit: backupPruneCandidatePageSize,
	})
	if err != nil {
		return BackupPruneCandidatePage{}, err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) > backupPruneCandidatePageSize {
		return BackupPruneCandidatePage{}, errs.New(errs.KindInternal, "backup prune candidate page is incomplete")
	}
	defer etcdstore.ClearRangeValues(read.Values)
	page := BackupPruneCandidatePage{
		Pending: make(
			[]etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord],
			0,
			backupruntime.MaximumBackupPruneDispatchPoints,
		),
	}
	for _, item := range read.Values {
		pointID := strings.TrimPrefix(item.Key, backupruntime.BackupRecoveryPointPrunePrefix)
		if item.Key != backupruntime.BackupRecoveryPointPruneKey(pointID) ||
			ids.Validate(ids.KindRecoveryPoint, pointID) != nil || item.ModRevision <= 0 {
			return BackupPruneCandidatePage{}, backupruntime.CorruptBackupRuntimeRecord()
		}
		prune, decodeErr := backupruntime.DecodeBackupRecoveryPointPruneRecord(item.Value)
		if decodeErr != nil || prune.Point.ID != pointID {
			return BackupPruneCandidatePage{}, backupruntime.CorruptBackupRuntimeRecord()
		}
		if prune.State == backupruntime.BackupPrunePending &&
			prune.DispatchAttempts < backupruntime.MaximumBackupPruneDispatchAttempts {
			page.Pending = append(page.Pending, etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord]{
				Record: prune, Revision: item.ModRevision, ReadRevision: read.ReadRevision,
			})
		}
	}
	if read.More {
		if len(read.Values) == 0 {
			return BackupPruneCandidatePage{}, errs.New(errs.KindInternal, "backup prune continuation is missing")
		}
		page.Next = read.Values[len(read.Values)-1].Key
	}
	return page, nil
}
