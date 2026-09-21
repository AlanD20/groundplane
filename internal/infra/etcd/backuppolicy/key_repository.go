package backuppolicy

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// KeyRepository reads encrypted key material and metadata at one MVCC revision.
type KeyRepository struct {
	store keyReader
}

type keyReader interface {
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
}

func NewKeyRepository(store etcdstore.Store) (*KeyRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "backup key store is required")
	}
	return &KeyRepository{store: store}, nil
}

// VersionedBackupKey keeps public metadata and its encrypted private identity
// at one fixed MVCC view while preserving their independent compare revisions.
// The caller owns Encrypted.Ciphertext and must clear it.
type VersionedBackupKey struct {
	Record            BackupKeyRecord
	Encrypted         BackupKeyEncryptedValue
	RecordRevision    int64
	EncryptedRevision int64
	ReadRevision      int64
}

func (repository *KeyRepository) GetBackupKey(
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
		BackupKeyKey(environmentID),
		BackupKeyValueKey(environmentID),
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
		return VersionedBackupKey{}, false, CorruptBackupKey()
	}
	record, err := DecodeBackupKeyRecord(result.Values[0].Value)
	if err != nil {
		return VersionedBackupKey{}, false, CorruptBackupKey()
	}
	encrypted, err := DecodeBackupKeyEncryptedValue(result.Values[1].Value)
	if err != nil {
		return VersionedBackupKey{}, false, CorruptBackupKey()
	}
	if record.EnvironmentID != environmentID || encrypted.EnvironmentID != environmentID ||
		record.KeyEra != encrypted.KeyEra {
		clear(encrypted.Ciphertext)
		return VersionedBackupKey{}, false, CorruptBackupKey()
	}
	return VersionedBackupKey{
		Record: record, Encrypted: encrypted,
		RecordRevision: result.Values[0].ModRevision, EncryptedRevision: result.Values[1].ModRevision,
		ReadRevision: result.ReadRevision,
	}, true, nil
}

func ValidateVersionedBackupKey(key VersionedBackupKey) error {
	if err := ValidateBackupKeyRecord(key.Record); err != nil {
		return err
	}
	if err := ValidateBackupKeyEncryptedValue(key.Encrypted); err != nil {
		return err
	}
	if key.Record.EnvironmentID != key.Encrypted.EnvironmentID || key.Record.KeyEra != key.Encrypted.KeyEra ||
		key.RecordRevision <= 0 || key.EncryptedRevision <= 0 ||
		key.ReadRevision < key.RecordRevision || key.ReadRevision < key.EncryptedRevision {
		return errs.New(errs.KindValidationFailed, "Backup key version is invalid")
	}
	return nil
}

func CorruptBackupKey() error {
	return errs.New(errs.KindInternal, "Backup key is corrupt")
}
