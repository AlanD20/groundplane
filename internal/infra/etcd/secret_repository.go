package etcd

import (
	"context"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	secretmutations "github.com/AlanD20/groundplane/internal/infra/etcd/secretmutations"
	secretrecord "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type SecretRepository struct {
	*secretmutations.Repository
	store hierarchyStore
}

func NewSecretRepository(store etcdstore.Store) (*SecretRepository, error) {
	return newSecretRepository(store)
}

func newSecretRepository(store hierarchyStore) (*SecretRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "Secret store is required")
	}
	return &SecretRepository{store: store, Repository: secretmutations.NewRepository(store)}, nil
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
