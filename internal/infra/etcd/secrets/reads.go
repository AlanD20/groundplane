package secrets

import (
	"context"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	recordquery "github.com/AlanD20/groundplane/internal/infra/etcd/recordquery"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *Reader) GetSecret(
	ctx context.Context,
	id string,
) (etcdstore.Versioned[Record], error) {
	return repository.getSecretAtRevision(ctx, id, 0)
}

func (repository *Reader) GetSecretValue(
	ctx context.Context,
	current etcdstore.Versioned[Record],
) (EncryptedValue, error) {
	if err := ValidateSecretVersion(current); err != nil {
		return EncryptedValue{}, err
	}
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{ValueKey(current.Record.Secret.ID)}, Revision: current.ReadRevision,
	})
	if err != nil {
		return EncryptedValue{}, err
	}
	if result == nil || len(result.Values) != 1 || result.Values[0] == nil {
		return EncryptedValue{}, errs.New(errs.KindInternal, "Secret encrypted value is missing")
	}
	value, err := DecodeEncryptedValue(result.Values[0].Value)
	if err != nil || value.SecretID != current.Record.Secret.ID {
		clear(value.Ciphertext)
		return EncryptedValue{}, CorruptRecord()
	}
	return value, nil
}

func (repository *Reader) ListSecrets(
	ctx context.Context,
	scope core.SecretScope,
	projectID string,
	request etcdstore.PageRequest,
) (etcdstore.Page[Record], error) {
	if err := ValidateSecretListScope(scope, projectID); err != nil {
		return etcdstore.Page[Record]{}, err
	}
	ownerKind, ownerID := SecretScopeKey(scope, projectID)
	page, err := recordquery.ListIndex(
		ctx,
		repository.store,
		"secrets",
		ownerKind,
		ownerID,
		SecretOwnerCollectionPrefix(scope, projectID),
		RecordKey,
		ids.KindSecret,
		request,
		DecodeRecord,
		func(record Record) string { return record.Secret.ID },
		func(record Record) bool {
			return record.Secret.Scope == scope && record.Secret.ProjectID == projectID
		},
	)
	if err != nil || len(page.Items) == 0 {
		return page, err
	}
	keys := make([]string, len(page.Items))
	for index, item := range page.Items {
		keys[index] = deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetSecret), item.Record.Secret.ID)
	}
	tombstones, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: page.Revision})
	if err != nil {
		return etcdstore.Page[Record]{}, err
	}
	if tombstones == nil || tombstones.ReadRevision != page.Revision || len(tombstones.Values) != len(keys) {
		return etcdstore.Page[Record]{}, errs.New(errs.KindInternal, "Secret deletion fence page is incomplete")
	}
	visible := make([]etcdstore.Versioned[Record], 0, len(page.Items))
	for index, item := range page.Items {
		if tombstones.Values[index] == nil {
			visible = append(visible, item)
			continue
		}
		if err := ValidateSecretDeletionFence(tombstones.Values[index], item.Record.Secret.ID); err != nil {
			return etcdstore.Page[Record]{}, err
		}
	}
	page.Items = visible
	return page, nil
}

// ResolveSecret accepts either an in-scope stable id or a key. Key lookup is
// project-first with platform fallback at one fixed etcd revision.
func (repository *Reader) ResolveSecret(
	ctx context.Context,
	projectID string,
	reference string,
) (etcdstore.Versioned[Record], error) {
	return repository.ResolveSecretAtRevision(ctx, projectID, reference, 0)
}

func (repository *Reader) ResolveSecretAtRevision(
	ctx context.Context, projectID, reference string, revision int64,
) (etcdstore.Versioned[Record], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	if err := recordcodec.ValidateID(ids.KindProject, projectID); err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	if reference == "" || revision < 0 {
		return etcdstore.Versioned[Record]{}, errs.New(errs.KindValidationFailed, "Secret reference is required")
	}
	if ids.Validate(ids.KindSecret, reference) == nil {
		record, err := repository.getSecretAtRevision(ctx, reference, revision)
		if err != nil {
			return etcdstore.Versioned[Record]{}, err
		}
		if record.Record.Secret.Scope == core.SecretScopePlatform ||
			record.Record.Secret.ProjectID == projectID {
			return record, nil
		}
		return etcdstore.Versioned[Record]{}, errs.New(errs.KindSecretNotFound, "Secret was not found in scope")
	}

	indexes, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		SecretKeyIndexKey(core.SecretScopeProject, projectID, reference),
		SecretKeyIndexKey(core.SecretScopePlatform, "", reference),
	}, Revision: revision})
	if err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	if indexes == nil || len(indexes.Values) != 2 || revision > 0 && indexes.ReadRevision != revision {
		return etcdstore.Versioned[Record]{}, errs.New(errs.KindInternal, "Secret fallback index read is incomplete")
	}
	for index, selected := range indexes.Values {
		if selected == nil {
			continue
		}
		id := string(selected.Value)
		if ids.Validate(ids.KindSecret, id) != nil {
			return etcdstore.Versioned[Record]{}, errs.New(errs.KindInternal, "Secret key index is corrupt")
		}
		stored, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
			Keys: []string{
				RecordKey(id),
				deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetSecret), id),
			},
			Revision: indexes.ReadRevision,
		})
		if err != nil {
			return etcdstore.Versioned[Record]{}, err
		}
		if stored == nil || stored.ReadRevision != indexes.ReadRevision || len(stored.Values) != 2 ||
			stored.Values[0] == nil {
			return etcdstore.Versioned[Record]{}, errs.New(
				errs.KindInternal,
				"Secret key index references a missing record",
			)
		}
		if stored.Values[1] != nil {
			if err := ValidateSecretDeletionFence(stored.Values[1], id); err != nil {
				return etcdstore.Versioned[Record]{}, err
			}
			continue
		}
		record, err := DecodeRecord(stored.Values[0].Value)
		projectMatch := index == 0 && record.Secret.Scope == core.SecretScopeProject &&
			record.Secret.ProjectID == projectID
		platformMatch := index == 1 && record.Secret.Scope == core.SecretScopePlatform &&
			record.Secret.ProjectID == ""
		if err != nil || record.Secret.ID != id || record.Secret.Key != reference ||
			(!projectMatch && !platformMatch) {
			return etcdstore.Versioned[Record]{}, CorruptRecord()
		}
		return etcdstore.Versioned[Record]{
			Record: record, Revision: stored.Values[0].ModRevision, ReadRevision: stored.ReadRevision,
		}, nil
	}
	return etcdstore.Versioned[Record]{}, errs.New(errs.KindSecretNotFound, "Secret was not found in scope")
}
