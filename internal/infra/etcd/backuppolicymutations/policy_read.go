package backuppolicymutations

import (
	"context"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *Repository) GetBackupPolicy(
	ctx context.Context,
	environmentID string,
) (etcdstore.Versioned[backuppolicy.BackupPolicyRecord], bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[backuppolicy.BackupPolicyRecord]{}, false, err
	}
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return etcdstore.Versioned[backuppolicy.BackupPolicyRecord]{}, false, err
	}
	result, err := repository.store.Get(ctx, backuppolicy.BackupPolicyKey(environmentID))
	if err != nil {
		return etcdstore.Versioned[backuppolicy.BackupPolicyRecord]{}, false, err
	}
	if result == nil {
		return etcdstore.Versioned[backuppolicy.BackupPolicyRecord]{}, false, errs.New(
			errs.KindInternal,
			"backup policy read is empty",
		)
	}
	if result.Entry == nil {
		return etcdstore.Versioned[backuppolicy.BackupPolicyRecord]{ReadRevision: result.ReadRevision}, false, nil
	}
	record, err := backuppolicy.DecodeBackupPolicyRecord(result.Entry.Value)
	if err != nil || record.EnvironmentID != environmentID {
		return etcdstore.Versioned[backuppolicy.BackupPolicyRecord]{}, false, recordcodec.CorruptRecord()
	}
	return etcdstore.Versioned[backuppolicy.BackupPolicyRecord]{
		Record: record, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, true, nil
}
