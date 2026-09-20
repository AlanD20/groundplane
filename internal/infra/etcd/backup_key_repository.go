package etcd

import (
	"context"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// VersionedBackupKey keeps public metadata and its encrypted private identity
// at one fixed MVCC view while preserving their independent compare revisions.
// The caller owns Encrypted.Ciphertext and must clear it.
type VersionedBackupKey struct {
	Record            backuppolicy.BackupKeyRecord
	Encrypted         backuppolicy.BackupKeyEncryptedValue
	RecordRevision    int64
	EncryptedRevision int64
	ReadRevision      int64
}

func (repository *BackupPolicyRepository) GetBackupKey(
	ctx context.Context,
	environmentID string,
) (VersionedBackupKey, bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return VersionedBackupKey{}, false, err
	}
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return VersionedBackupKey{}, false, err
	}
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		backuppolicy.BackupKeyKey(environmentID),
		backuppolicy.BackupKeyValueKey(environmentID),
	}})
	if err != nil {
		return VersionedBackupKey{}, false, err
	}
	if result == nil || len(result.Values) != 2 {
		return VersionedBackupKey{}, false, errs.New(
			errs.KindInternal,
			"Backup key read is incomplete",
		)
	}
	if result.Values[0] == nil && result.Values[1] == nil {
		return VersionedBackupKey{ReadRevision: result.ReadRevision}, false, nil
	}
	if result.Values[0] == nil || result.Values[1] == nil {
		return VersionedBackupKey{}, false, corruptBackupKey()
	}
	record, err := backuppolicy.DecodeBackupKeyRecord(result.Values[0].Value)
	if err != nil {
		return VersionedBackupKey{}, false, corruptBackupKey()
	}
	encrypted, err := backuppolicy.DecodeBackupKeyEncryptedValue(result.Values[1].Value)
	if err != nil {
		return VersionedBackupKey{}, false, corruptBackupKey()
	}
	if record.EnvironmentID != environmentID || encrypted.EnvironmentID != environmentID ||
		record.KeyEra != encrypted.KeyEra {
		clear(encrypted.Ciphertext)
		return VersionedBackupKey{}, false, corruptBackupKey()
	}
	return VersionedBackupKey{
		Record: record, Encrypted: encrypted,
		RecordRevision: result.Values[0].ModRevision, EncryptedRevision: result.Values[1].ModRevision,
		ReadRevision: result.ReadRevision,
	}, true, nil
}

func validateVersionedBackupKey(key VersionedBackupKey) error {
	if err := backuppolicy.ValidateBackupKeyRecord(key.Record); err != nil {
		return err
	}
	if err := backuppolicy.ValidateBackupKeyEncryptedValue(key.Encrypted); err != nil {
		return err
	}
	if key.Record.EnvironmentID != key.Encrypted.EnvironmentID || key.Record.KeyEra != key.Encrypted.KeyEra ||
		key.RecordRevision <= 0 || key.EncryptedRevision <= 0 ||
		key.ReadRevision < key.RecordRevision || key.ReadRevision < key.EncryptedRevision {
		return errs.New(errs.KindValidationFailed, "Backup key version is invalid")
	}
	return nil
}

func corruptBackupKey() error {
	return errs.New(errs.KindInternal, "Backup key is corrupt")
}
