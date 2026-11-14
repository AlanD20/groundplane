package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/secretmutations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ReplaceSecretValueIdempotent preserves the stable metadata/index identity.
// Exact execution pins and Script sources fence replacement just as deletion;
// an edit cannot destroy ciphertext still required by captured execution.
func (repository *SecretRepository) ReplaceSecretValueIdempotent(
	ctx context.Context,
	owner secrets.Owner,
	current keyvalue.Versioned[secrets.Record],
	replacement secrets.Record,
	value secrets.EncryptedValue,
	marker idempotency.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := secrets.ValidateSecretOwnership(ctx, owner, current.Record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if secrets.ValidateSecretValueBinding(replacement, value) != nil ||
		replacement.Secret.ID != current.Record.Secret.ID ||
		replacement.Secret.Scope != current.Record.Secret.Scope ||
		replacement.Secret.ProjectID != current.Record.Secret.ProjectID ||
		replacement.Secret.Key != current.Record.Secret.Key ||
		replacement.Secret.Kind != current.Record.Secret.Kind ||
		replacement.Secret.Ref != current.Record.Secret.Ref ||
		current.Revision <= 0 {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Secret value edit cannot change its identity, owner, key or kind",
		)
	}
	if marker.Kind != idempotency.IdempotencyMarkerDirect || marker.State != idempotency.IdempotencyMarkerCompleted ||
		marker.Locator.Method != "PATCH" ||
		marker.Locator.Route != "/secrets/{id}" ||
		marker.ReplayTarget == nil ||
		marker.ReplayTarget.Kind != idempotency.IdempotencyReplayTargetSecret ||
		marker.ReplayTarget.ID != current.Record.Secret.ID ||
		idempotency.ValidateIdempotencyMarker(marker) != nil {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Secret edit replay authority is invalid",
		)
	}
	expectedKind, expectedID := idempotency.IdempotencyScopePlatform, "-"
	if current.Record.Secret.ProjectID != "" {
		expectedKind = idempotency.IdempotencyScopeProject
		expectedID = current.Record.Secret.ProjectID
	}
	if marker.Locator.ScopeKind != expectedKind || marker.Locator.ScopeID != expectedID {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Secret edit scope differs from its captured owner",
		)
	}
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{
			Keys: []string{
				secrets.SecretOwnerKey(current.Record.Secret),
				secrets.SecretScopedKey(current.Record.Secret),
				secrets.ValueKey(current.Record.Secret.ID),
			},
			Revision: current.ReadRevision,
		},
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if read == nil || len(read.Values) != 3 || read.ReadRevision != current.ReadRevision {
		return IdempotencyTransactionResult{}, errs.New(errs.KindStateConflict, "Secret edit source is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	for i := range read.Values {
		if read.Values[i] == nil {
			return IdempotencyTransactionResult{}, errs.New(errs.KindStateConflict, "Secret edit source is incomplete")
		}
	}
	if string(read.Values[0].Value) != current.Record.Secret.ID ||
		string(read.Values[1].Value) != current.Record.Secret.ID {
		return IdempotencyTransactionResult{}, secrets.CorruptRecord()
	}
	conditions := secrets.SecretDeleteConditions(owner, current, read.Values)
	guards, err := secretmutations.PrepareSecretScriptAbsence(ctx, repository.store, current.Record.Secret.ID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	conditions = append(conditions, guards...)
	metadata, err := secrets.EncodeRecord(replacement)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(metadata)
	ciphertext, err := secrets.EncodeEncryptedValue(value)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(ciphertext)
	plan, err := NewIdempotencyMutationPlan(
		conditions,
		[]keyvalue.Mutation{
			{Type: keyvalue.MutationPut, Key: secrets.RecordKey(current.Record.Secret.ID), Value: metadata},
			{Type: keyvalue.MutationPut, Key: secrets.ValueKey(current.Record.Secret.ID), Value: ciphertext},
		},
		func(_ int64, _ []*keyvalue.KeyValue) error {
			return errs.New(errs.KindStateConflict, "Secret changed or was pinned while editing; refresh and retry")
		},
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	store, err := NewIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return store.Apply(ctx, marker, plan)
}
