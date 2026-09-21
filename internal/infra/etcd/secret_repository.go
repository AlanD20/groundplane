package etcd

import (
	"context"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	recordquery "github.com/AlanD20/groundplane/internal/infra/etcd/recordquery"
	secretrecord "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type SecretRepository struct {
	store hierarchyStore
}

func NewSecretRepository(store etcdstore.Store) (*SecretRepository, error) {
	return newSecretRepository(store)
}

func newSecretRepository(store hierarchyStore) (*SecretRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "Secret store is required")
	}
	return &SecretRepository{store: store}, nil
}

func (repository *SecretRepository) CreateSecret(
	ctx context.Context,
	owner secretrecord.Owner,
	record secretrecord.Record,
	value secretrecord.EncryptedValue,
) (etcdstore.Versioned[secretrecord.Record], error) {
	if err := secretrecord.ValidateSecretOwnership(ctx, owner, record); err != nil {
		return etcdstore.Versioned[secretrecord.Record]{}, err
	}
	if err := secretrecord.ValidateSecretValueBinding(record, value); err != nil {
		return etcdstore.Versioned[secretrecord.Record]{}, err
	}
	primaryValue, err := secretrecord.EncodeRecord(record)
	if err != nil {
		return etcdstore.Versioned[secretrecord.Record]{}, err
	}
	defer clear(primaryValue)
	encryptedValue, err := secretrecord.EncodeEncryptedValue(value)
	if err != nil {
		return etcdstore.Versioned[secretrecord.Record]{}, err
	}
	defer clear(encryptedValue)

	conditions := secretrecord.SecretCreateConditions(owner, record)
	result, err := repository.store.Transact(ctx, conditions, []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: secretrecord.RecordKey(record.Secret.ID), Value: primaryValue},
		{Type: etcdstore.MutationPut, Key: secretrecord.SecretOwnerKey(record.Secret), Value: []byte(record.Secret.ID)},
		{Type: etcdstore.MutationPut, Key: secretrecord.SecretScopedKey(record.Secret), Value: []byte(record.Secret.ID)},
		{Type: etcdstore.MutationPut, Key: secretrecord.ValueKey(record.Secret.ID), Value: encryptedValue},
	})
	if err != nil {
		return etcdstore.Versioned[secretrecord.Record]{}, err
	}
	if !result.Succeeded {
		return etcdstore.Versioned[secretrecord.Record]{}, secretrecord.ClassifySecretCreateConflict(
			result.FailureReads, owner, record,
		)
	}
	return etcdstore.Versioned[secretrecord.Record]{
		Record: record, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}

// CreateSecretIdempotent atomically commits the redacted metadata, ownership
// indexes, encrypted value, and exact completed replay marker.
func (repository *SecretRepository) CreateSecretIdempotent(
	ctx context.Context,
	owner secretrecord.Owner,
	record secretrecord.Record,
	value secretrecord.EncryptedValue,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := secretrecord.ValidateSecretOwnership(ctx, owner, record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := secretrecord.ValidateSecretValueBinding(record, value); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if marker.Kind != idempotencyrecord.IdempotencyMarkerDirect || marker.State != idempotencyrecord.IdempotencyMarkerCompleted {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Secret creation marker must be a completed direct mutation",
		)
	}
	if err := idempotencyrecord.ValidateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	primaryValue, err := secretrecord.EncodeRecord(record)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(primaryValue)
	encryptedValue, err := secretrecord.EncodeEncryptedValue(value)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(encryptedValue)
	plan, err := NewIdempotencyMutationPlan(
		secretrecord.SecretCreateConditions(owner, record),
		[]etcdstore.Mutation{
			{Type: etcdstore.MutationPut, Key: secretrecord.RecordKey(record.Secret.ID), Value: primaryValue},
			{Type: etcdstore.MutationPut, Key: secretrecord.SecretOwnerKey(record.Secret), Value: []byte(record.Secret.ID)},
			{Type: etcdstore.MutationPut, Key: secretrecord.SecretScopedKey(record.Secret), Value: []byte(record.Secret.ID)},
			{Type: etcdstore.MutationPut, Key: secretrecord.ValueKey(record.Secret.ID), Value: encryptedValue},
		},
		func(_ int64, values []*etcdstore.KeyValue) error {
			return secretrecord.ClassifySecretCreateConflict(values, owner, record)
		},
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := NewIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
}

func (repository *SecretRepository) GetSecret(
	ctx context.Context,
	id string,
) (etcdstore.Versioned[secretrecord.Record], error) {
	return repository.getSecretAtRevision(ctx, id, 0)
}

func (repository *SecretRepository) GetSecretValue(
	ctx context.Context,
	current etcdstore.Versioned[secretrecord.Record],
) (secretrecord.EncryptedValue, error) {
	if err := secretrecord.ValidateSecretVersion(current); err != nil {
		return secretrecord.EncryptedValue{}, err
	}
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{secretrecord.ValueKey(current.Record.Secret.ID)}, Revision: current.ReadRevision,
	})
	if err != nil {
		return secretrecord.EncryptedValue{}, err
	}
	if result == nil || len(result.Values) != 1 || result.Values[0] == nil {
		return secretrecord.EncryptedValue{}, errs.New(errs.KindInternal, "Secret encrypted value is missing")
	}
	value, err := secretrecord.DecodeEncryptedValue(result.Values[0].Value)
	if err != nil || value.SecretID != current.Record.Secret.ID {
		clear(value.Ciphertext)
		return secretrecord.EncryptedValue{}, secretrecord.CorruptRecord()
	}
	return value, nil
}

func (repository *SecretRepository) ListSecrets(
	ctx context.Context,
	scope core.SecretScope,
	projectID string,
	request etcdstore.PageRequest,
) (etcdstore.Page[secretrecord.Record], error) {
	if err := secretrecord.ValidateSecretListScope(scope, projectID); err != nil {
		return etcdstore.Page[secretrecord.Record]{}, err
	}
	ownerKind, ownerID := secretrecord.SecretScopeKey(scope, projectID)
	page, err := recordquery.ListIndex(
		ctx,
		repository.store,
		"secrets",
		ownerKind,
		ownerID,
		secretrecord.SecretOwnerCollectionPrefix(scope, projectID),
		secretrecord.RecordKey,
		ids.KindSecret,
		request,
		secretrecord.DecodeRecord,
		func(record secretrecord.Record) string { return record.Secret.ID },
		func(record secretrecord.Record) bool {
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
		return etcdstore.Page[secretrecord.Record]{}, err
	}
	if tombstones == nil || tombstones.ReadRevision != page.Revision || len(tombstones.Values) != len(keys) {
		return etcdstore.Page[secretrecord.Record]{}, errs.New(errs.KindInternal, "Secret deletion fence page is incomplete")
	}
	visible := make([]etcdstore.Versioned[secretrecord.Record], 0, len(page.Items))
	for index, item := range page.Items {
		if tombstones.Values[index] == nil {
			visible = append(visible, item)
			continue
		}
		if err := secretrecord.ValidateSecretDeletionFence(tombstones.Values[index], item.Record.Secret.ID); err != nil {
			return etcdstore.Page[secretrecord.Record]{}, err
		}
	}
	page.Items = visible
	return page, nil
}

// ResolveSecret accepts either an in-scope stable id or a key. Key lookup is
// project-first with platform fallback at one fixed etcd revision.
func (repository *SecretRepository) ResolveSecret(
	ctx context.Context,
	projectID string,
	reference string,
) (etcdstore.Versioned[secretrecord.Record], error) {
	return repository.resolveSecretAtRevision(ctx, projectID, reference, 0)
}

func (repository *SecretRepository) resolveSecretAtRevision(
	ctx context.Context, projectID, reference string, revision int64,
) (etcdstore.Versioned[secretrecord.Record], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[secretrecord.Record]{}, err
	}
	if err := recordcodec.ValidateID(ids.KindProject, projectID); err != nil {
		return etcdstore.Versioned[secretrecord.Record]{}, err
	}
	if reference == "" || revision < 0 {
		return etcdstore.Versioned[secretrecord.Record]{}, errs.New(errs.KindValidationFailed, "Secret reference is required")
	}
	if ids.Validate(ids.KindSecret, reference) == nil {
		record, err := repository.getSecretAtRevision(ctx, reference, revision)
		if err != nil {
			return etcdstore.Versioned[secretrecord.Record]{}, err
		}
		if record.Record.Secret.Scope == core.SecretScopePlatform ||
			record.Record.Secret.ProjectID == projectID {
			return record, nil
		}
		return etcdstore.Versioned[secretrecord.Record]{}, errs.New(errs.KindSecretNotFound, "Secret was not found in scope")
	}

	indexes, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		secretrecord.SecretKeyIndexKey(core.SecretScopeProject, projectID, reference),
		secretrecord.SecretKeyIndexKey(core.SecretScopePlatform, "", reference),
	}, Revision: revision})
	if err != nil {
		return etcdstore.Versioned[secretrecord.Record]{}, err
	}
	if indexes == nil || len(indexes.Values) != 2 || revision > 0 && indexes.ReadRevision != revision {
		return etcdstore.Versioned[secretrecord.Record]{}, errs.New(errs.KindInternal, "Secret fallback index read is incomplete")
	}
	for index, selected := range indexes.Values {
		if selected == nil {
			continue
		}
		id := string(selected.Value)
		if ids.Validate(ids.KindSecret, id) != nil {
			return etcdstore.Versioned[secretrecord.Record]{}, errs.New(errs.KindInternal, "Secret key index is corrupt")
		}
		stored, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
			Keys: []string{
				secretrecord.RecordKey(id),
				deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetSecret), id),
			},
			Revision: indexes.ReadRevision,
		})
		if err != nil {
			return etcdstore.Versioned[secretrecord.Record]{}, err
		}
		if stored == nil || stored.ReadRevision != indexes.ReadRevision || len(stored.Values) != 2 ||
			stored.Values[0] == nil {
			return etcdstore.Versioned[secretrecord.Record]{}, errs.New(
				errs.KindInternal,
				"Secret key index references a missing record",
			)
		}
		if stored.Values[1] != nil {
			if err := secretrecord.ValidateSecretDeletionFence(stored.Values[1], id); err != nil {
				return etcdstore.Versioned[secretrecord.Record]{}, err
			}
			continue
		}
		record, err := secretrecord.DecodeRecord(stored.Values[0].Value)
		projectMatch := index == 0 && record.Secret.Scope == core.SecretScopeProject &&
			record.Secret.ProjectID == projectID
		platformMatch := index == 1 && record.Secret.Scope == core.SecretScopePlatform &&
			record.Secret.ProjectID == ""
		if err != nil || record.Secret.ID != id || record.Secret.Key != reference ||
			(!projectMatch && !platformMatch) {
			return etcdstore.Versioned[secretrecord.Record]{}, secretrecord.CorruptRecord()
		}
		return etcdstore.Versioned[secretrecord.Record]{
			Record: record, Revision: stored.Values[0].ModRevision, ReadRevision: stored.ReadRevision,
		}, nil
	}
	return etcdstore.Versioned[secretrecord.Record]{}, errs.New(errs.KindSecretNotFound, "Secret was not found in scope")
}

func (repository *SecretRepository) DeleteSecret(
	ctx context.Context,
	owner secretrecord.Owner,
	current etcdstore.Versioned[secretrecord.Record],
) (int64, error) {
	if err := secretrecord.ValidateSecretOwnership(ctx, owner, current.Record); err != nil {
		return 0, err
	}
	if err := secretrecord.ValidateSecretVersion(current); err != nil {
		return 0, err
	}
	dependencies, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			secretrecord.SecretOwnerKey(current.Record.Secret),
			secretrecord.SecretScopedKey(current.Record.Secret),
			secretrecord.ValueKey(current.Record.Secret.ID),
		},
		Revision: current.ReadRevision,
	})
	if err != nil {
		return 0, err
	}
	if dependencies == nil || len(dependencies.Values) != 3 ||
		dependencies.Values[0] == nil || dependencies.Values[1] == nil || dependencies.Values[2] == nil ||
		string(dependencies.Values[0].Value) != current.Record.Secret.ID ||
		string(dependencies.Values[1].Value) != current.Record.Secret.ID {
		return 0, errs.New(errs.KindInternal, "Secret indexes or encrypted value are missing")
	}
	conditions := secretrecord.SecretDeleteConditions(owner, current, dependencies.Values)
	fences, err := prepareSecretScriptAbsence(ctx, repository.store, current.Record.Secret.ID)
	if err != nil {
		return 0, err
	}
	conditions = append(conditions, fences...)
	result, err := repository.store.Transact(ctx, conditions, []etcdstore.Mutation{
		{Type: etcdstore.MutationDelete, Key: secretrecord.RecordKey(current.Record.Secret.ID)},
		{Type: etcdstore.MutationDelete, Key: secretrecord.SecretOwnerKey(current.Record.Secret)},
		{Type: etcdstore.MutationDelete, Key: secretrecord.SecretScopedKey(current.Record.Secret)},
		{Type: etcdstore.MutationDelete, Key: secretrecord.ValueKey(current.Record.Secret.ID)},
	})
	if err != nil {
		return 0, err
	}
	if !result.Succeeded {
		return 0, errs.New(errs.KindStateConflict, "Secret changed while deletion was requested")
	}
	return result.Revision, nil
}
