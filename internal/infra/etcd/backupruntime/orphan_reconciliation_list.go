package backupruntime

import (
	"context"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// NextReconciledBackupOrphan walks primary records, so the scheduler does not
// require a surviving Environment or Task to discover an orphan. The returned
// version is re-read with both native membership indexes before use.
func (repository *Reader) NextReconciledBackupOrphan(ctx context.Context, afterID string) (
	etcdstore.Versioned[BackupOrphanRecord], string, bool, error,
) {
	if afterID != "" && ids.Validate(ids.KindRecoveryPoint, afterID) != nil {
		return etcdstore.Versioned[BackupOrphanRecord]{}, afterID, false,
			errs.New(errs.KindValidationFailed, "backup orphan cursor is invalid")
	}
	start := ""
	if afterID != "" {
		start = backupOrphanPrefix + afterID
	}
	page, err := repository.store.Range(ctx, etcdstore.RangeRequest{
		Prefix: backupOrphanPrefix, StartExclusive: start, Limit: 1,
	})
	if err != nil {
		return etcdstore.Versioned[BackupOrphanRecord]{}, afterID, false, err
	}
	if page == nil || page.ReadRevision <= 0 || len(page.Values) > 1 {
		return etcdstore.Versioned[BackupOrphanRecord]{}, afterID, false, CorruptBackupRuntimeRecord()
	}
	defer etcdstore.ClearRangeValues(page.Values)
	if len(page.Values) == 0 {
		return etcdstore.Versioned[BackupOrphanRecord]{}, "", false, nil
	}
	entry := page.Values[0]
	id := strings.TrimPrefix(entry.Key, backupOrphanPrefix)
	if entry.Key != backupOrphanPrefix+id || ids.Validate(ids.KindRecoveryPoint, id) != nil {
		return etcdstore.Versioned[BackupOrphanRecord]{}, afterID, false, CorruptBackupRuntimeRecord()
	}
	current, found, err := repository.GetBackupOrphan(ctx, id)
	if err != nil || !found {
		if err == nil {
			err = errs.New(errs.KindStateConflict, "backup orphan changed during reconciliation scan")
		}
		return etcdstore.Versioned[BackupOrphanRecord]{}, afterID, false, err
	}
	return current, id, true, nil
}
