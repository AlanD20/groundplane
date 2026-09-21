package secrets

import (
	"context"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// getSecretAtRevision observes the primary and retirement fence together. A
// prepared Script uses its existing snapshot; ordinary reads request latest.
func (repository *Reader) getSecretAtRevision(
	ctx context.Context, id string, revision int64,
) (etcdstore.Versioned[Record], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	if err := recordcodec.ValidateID(ids.KindSecret, id); err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	if revision < 0 {
		return etcdstore.Versioned[Record]{}, errs.New(errs.KindValidationFailed, "Secret read revision is invalid")
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys:     []string{RecordKey(id), deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetSecret), id)},
		Revision: revision,
	})
	if err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	if read == nil || len(read.Values) != 2 || read.ReadRevision <= 0 ||
		(revision > 0 && read.ReadRevision != revision) {
		return etcdstore.Versioned[Record]{}, errs.New(errs.KindInternal, "Secret revision read is incomplete")
	}
	if read.Values[0] == nil {
		return etcdstore.Versioned[Record]{}, errs.New(errs.KindSecretNotFound, "Secret was not found")
	}
	record, err := DecodeRecord(read.Values[0].Value)
	if err != nil || record.Secret.ID != id {
		return etcdstore.Versioned[Record]{}, CorruptRecord()
	}
	if read.Values[1] != nil {
		if err := ValidateSecretDeletionFence(read.Values[1], id); err != nil {
			return etcdstore.Versioned[Record]{}, err
		}
		return etcdstore.Versioned[Record]{}, errs.New(errs.KindSecretNotFound, "Secret was not found")
	}
	return etcdstore.Versioned[Record]{
		Record:       record,
		Revision:     read.Values[0].ModRevision,
		ReadRevision: read.ReadRevision,
	}, nil
}
