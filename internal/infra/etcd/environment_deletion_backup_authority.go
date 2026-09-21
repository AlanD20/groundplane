package etcd

import (
	"context"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	connectors "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	deletions "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func requireEnvironmentDeletionBackupStateEmpty(
	ctx context.Context,
	store hierarchyStore,
	environmentID string,
	operationID string,
	revision int64,
) error {
	present, err := environmentDeletionBackupAuthorityPresent(
		ctx, store, environmentID, operationID, revision,
	)
	if err != nil {
		return err
	}
	if present {
		return errs.New(
			errs.KindStateConflict,
			"environment deletion retained backup authority",
		)
	}
	return nil
}

func initialEnvironmentDeletionCleanupPhase(
	ctx context.Context,
	store hierarchyStore,
	environmentID string,
	operationID string,
	revision int64,
) (deletions.EnvironmentDeletionCleanupPhase, error) {
	present, err := environmentDeletionBackupAuthorityPresent(
		ctx, store, environmentID, operationID, revision,
	)
	if err != nil {
		return "", err
	}
	if present {
		return deletions.EnvironmentDeletionCleanupEnumerating, nil
	}
	return deletions.EnvironmentDeletionCleanupComplete, nil
}

func environmentDeletionBackupAuthorityPresent(
	ctx context.Context,
	store hierarchyStore,
	environmentID string,
	operationID string,
	revision int64,
) (bool, error) {
	direct, err := store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			backuppolicy.BackupPolicyKey(environmentID),
			backuppolicy.BackupKeyKey(environmentID),
			backuppolicy.BackupKeyValueKey(environmentID),
		},
		Revision: revision,
	})
	if err != nil {
		return false, err
	}
	if direct == nil {
		return false, errs.New(
			errs.KindInternal,
			"environment deletion backup authority evidence is incomplete",
		)
	}
	if direct.ReadRevision != revision || len(direct.Values) != 3 {
		etcdstore.ClearValues(direct.Values)
		return false, errs.New(
			errs.KindInternal,
			"environment deletion backup authority evidence is incomplete",
		)
	}
	directPresent := false
	for _, value := range direct.Values {
		directPresent = directPresent || value != nil
	}
	etcdstore.ClearValues(direct.Values)
	if directPresent {
		return true, nil
	}
	prefixes := []string{
		backuppolicy.BackupSourceEnvironmentPrefix(environmentID),
		connectors.ConnectorEnvironmentPrefix(environmentID),
		backupruntime.BackupScheduleCursorPrefix + environmentID + "/",
		backupruntime.BackupDueOutcomePrefix + environmentID + "/",
		backupruntime.BackupRecoveryPointEnvironmentPrefix + environmentID + "/",
		backupruntime.BackupRunEnvironmentPrefix + environmentID + "/",
		backupruntime.BackupOrphanEnvironmentPrefix + environmentID + "/",
		backupruntime.BackupRestoreEnvironmentPrefix + environmentID + "/",
		backupruntime.BackupKeyRotationEnvironmentPrefix + environmentID + "/",
		deletions.EnvironmentDeletionWorkOperationPrefix(operationID),
	}
	for _, prefix := range prefixes {
		page, err := store.Range(ctx, etcdstore.RangeRequest{Prefix: prefix, Limit: 1, Revision: revision})
		if err != nil {
			return false, err
		}
		if page == nil || page.ReadRevision != revision || len(page.Values) > 1 {
			if page != nil {
				etcdstore.ClearRangeValues(page.Values)
			}
			return false, errs.New(
				errs.KindInternal,
				"environment deletion backup authority evidence is incomplete",
			)
		}
		found := len(page.Values) != 0
		etcdstore.ClearRangeValues(page.Values)
		if found {
			return true, nil
		}
	}
	return false, nil
}
