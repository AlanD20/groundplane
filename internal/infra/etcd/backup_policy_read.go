package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupqueries"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
)

func (repository *BackupPolicyRepository) GetBackupPolicyProjection(ctx context.Context, environmentID string) (backupqueries.BackupPolicyProjection, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return backupqueries.BackupPolicyProjection{}, err
	}
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return backupqueries.BackupPolicyProjection{}, err
	}
	return backupqueries.ReadPolicySnapshot(ctx, repository.store, environmentID)
}
