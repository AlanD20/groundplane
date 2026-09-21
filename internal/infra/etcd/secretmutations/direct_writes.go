package secretmutations

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	secretrecord "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *Repository) CreateSecret(
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

func (repository *Repository) DeleteSecret(
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
	fences, err := PrepareSecretScriptAbsence(ctx, repository.store, current.Record.Secret.ID)
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
