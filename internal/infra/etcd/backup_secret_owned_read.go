package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// getBackupSecretManyOwned takes ownership of every returned value buffer.
// On every rejection path it clears those buffers before returning. A valid
// result transfers that ownership to the caller without another ciphertext
// copy; the caller must clear it after use.
func getBackupSecretManyOwned(
	ctx context.Context,
	store backupSecretResolutionStore,
	keys []string,
	revision int64,
) (*etcdstore.GetManyResult, error) {
	result, err := store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		if result != nil {
			etcdstore.ClearValues(result.Values)
		}
		return nil, err
	}
	if result == nil || result.ReadRevision != revision || len(result.Values) != len(keys) {
		if result != nil {
			etcdstore.ClearValues(result.Values)
		}
		return nil, errs.New(errs.KindInternal, "backup secret fixed read is incomplete")
	}
	for index, value := range result.Values {
		if value != nil && value.Key != keys[index] {
			etcdstore.ClearValues(result.Values)
			return nil, errs.New(errs.KindInternal, "backup secret fixed read is corrupt")
		}
	}
	return result, nil
}
