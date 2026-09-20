package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	secretrecord "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// getSecretAtRevision observes the primary and retirement fence together. A
// prepared Script uses its existing snapshot; ordinary reads request latest.
func (repository *SecretRepository) getSecretAtRevision(
	ctx context.Context, id string, revision int64,
) (etcdstore.Versioned[secretrecord.Record], error) {
	if err := validateContext(ctx); err != nil {
		return etcdstore.Versioned[secretrecord.Record]{}, err
	}
	if err := recordcodec.ValidateID(ids.KindSecret, id); err != nil {
		return etcdstore.Versioned[secretrecord.Record]{}, err
	}
	if revision < 0 {
		return etcdstore.Versioned[secretrecord.Record]{}, errs.New(errs.KindValidationFailed, "Secret read revision is invalid")
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys:     []string{secretrecord.RecordKey(id), deletionTombstoneKey(string(DeletionTargetSecret), id)},
		Revision: revision,
	})
	if err != nil {
		return etcdstore.Versioned[secretrecord.Record]{}, err
	}
	if read == nil || len(read.Values) != 2 || read.ReadRevision <= 0 ||
		(revision > 0 && read.ReadRevision != revision) {
		return etcdstore.Versioned[secretrecord.Record]{}, errs.New(errs.KindInternal, "Secret revision read is incomplete")
	}
	if read.Values[0] == nil {
		return etcdstore.Versioned[secretrecord.Record]{}, errs.New(errs.KindSecretNotFound, "Secret was not found")
	}
	record, err := secretrecord.DecodeRecord(read.Values[0].Value)
	if err != nil || record.Secret.ID != id {
		return etcdstore.Versioned[secretrecord.Record]{}, secretrecord.CorruptRecord()
	}
	if read.Values[1] != nil {
		if err := validateSecretDeletionFence(read.Values[1], id); err != nil {
			return etcdstore.Versioned[secretrecord.Record]{}, err
		}
		return etcdstore.Versioned[secretrecord.Record]{}, errs.New(errs.KindSecretNotFound, "Secret was not found")
	}
	return etcdstore.Versioned[secretrecord.Record]{
		Record:       record,
		Revision:     read.Values[0].ModRevision,
		ReadRevision: read.ReadRevision,
	}, nil
}
