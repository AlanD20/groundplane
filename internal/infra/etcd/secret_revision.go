package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// getSecretAtRevision observes the primary and retirement fence together. A
// prepared Script uses its existing snapshot; ordinary reads request latest.
func (repository *SecretRepository) getSecretAtRevision(
	ctx context.Context, id string, revision int64,
) (Versioned[SecretRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[SecretRecord]{}, err
	}
	if err := validateID(ids.KindSecret, id); err != nil {
		return Versioned[SecretRecord]{}, err
	}
	if revision < 0 {
		return Versioned[SecretRecord]{}, errs.New(errs.KindValidationFailed, "Secret read revision is invalid")
	}
	read, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys:     []string{secretRecordKey(id), deletionTombstoneKey(string(DeletionTargetSecret), id)},
		Revision: revision,
	})
	if err != nil {
		return Versioned[SecretRecord]{}, err
	}
	if read == nil || len(read.Values) != 2 || read.ReadRevision <= 0 ||
		(revision > 0 && read.ReadRevision != revision) {
		return Versioned[SecretRecord]{}, errs.New(errs.KindInternal, "Secret revision read is incomplete")
	}
	if read.Values[0] == nil {
		return Versioned[SecretRecord]{}, errs.New(errs.KindSecretNotFound, "Secret was not found")
	}
	record, err := decodeSecretRecord(read.Values[0].Value)
	if err != nil || record.Secret.ID != id {
		return Versioned[SecretRecord]{}, corruptSecretRecord()
	}
	if read.Values[1] != nil {
		if err := validateSecretDeletionFence(read.Values[1], id); err != nil {
			return Versioned[SecretRecord]{}, err
		}
		return Versioned[SecretRecord]{}, errs.New(errs.KindSecretNotFound, "Secret was not found")
	}
	return Versioned[SecretRecord]{
		Record:       record,
		Revision:     read.Values[0].ModRevision,
		ReadRevision: read.ReadRevision,
	}, nil
}
