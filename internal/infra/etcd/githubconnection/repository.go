package githubconnection

import (
	"context"
	"math"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordquery"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type store interface {
	Get(context.Context, string) (*keyvalue.GetResult, error)
	GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error)
	Range(context.Context, keyvalue.RangeRequest) (*keyvalue.RangeResult, error)
	Transact(context.Context, []keyvalue.Condition, []keyvalue.Mutation) (keyvalue.TransactionResult, error)
}

// Cipher seals and opens Controller-owned values. Implementations consume inputs
// synchronously and return caller-owned byte slices.
type Cipher interface {
	Seal(context.Context, []byte) ([]byte, error)
	Open(context.Context, []byte) ([]byte, error)
}

// Publication supplies additional conditions and writes for the connection
// transaction, such as a replay receipt built from public metadata.
type Publication func(Connection) ([]keyvalue.Condition, []keyvalue.Mutation, error)

type Repository struct {
	store  store
	cipher Cipher
}

func NewRepository(store store, cipher Cipher) (*Repository, error) {
	if store == nil || cipher == nil {
		return nil, errs.New(errs.KindInternal, "GitHub connection storage and cipher are required")
	}
	return &Repository{store: store, cipher: cipher}, nil
}

// Create binds immutable provider IDs to a scoped connection and publishes
// metadata and credentials in the same transaction. Additional conditions can
// protect the Controller's captured owner or acceptance evidence.
func (repository *Repository) Create(
	ctx context.Context,
	input CreateInput,
	conditions []keyvalue.Condition,
	publications ...Publication,
) (keyvalue.Versioned[Connection], error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return keyvalue.Versioned[Connection]{}, err
	}
	id := input.ID
	if id == "" {
		id = ids.New(IDKind)
	}
	record := Connection{
		ID: id, Scope: input.Scope, Name: input.Name,
		AppID: input.AppID, InstallationID: input.InstallationID, Generation: 1,
		CreatedAt: input.CreatedAt, UpdatedAt: input.CreatedAt,
	}
	if err := validateConnection(record); err != nil {
		return keyvalue.Versioned[Connection]{}, err
	}
	metadataKey, privateKey := connectionKey(input.Scope, record.ID), credentialKey(record.ID)
	conditions = append(append([]keyvalue.Condition(nil), conditions...),
		keyvalue.Condition{Key: metadataKey}, keyvalue.Condition{Key: privateKey})
	owner, err := repository.ownerGuard(ctx, input.Scope, 0)
	if err != nil {
		return keyvalue.Versioned[Connection]{}, err
	}
	return repository.write(ctx, record, append(conditions, owner...), Credentials{
		PrivateKeyPEM: input.PrivateKeyPEM, WebhookSecret: input.WebhookSecret,
	}, publications)
}

// Get returns only metadata under the explicitly selected owner.
func (repository *Repository) Get(ctx context.Context, scope Scope, id string) (keyvalue.Versioned[Connection], error) {
	if err := verifyLookup(ctx, scope, id); err != nil {
		return keyvalue.Versioned[Connection]{}, err
	}
	result, err := repository.store.Get(ctx, connectionKey(scope, id))
	if err != nil {
		return keyvalue.Versioned[Connection]{}, err
	}
	if result == nil || result.ReadRevision <= 0 {
		return keyvalue.Versioned[Connection]{}, corruption()
	}
	defer keyvalue.ClearValues([]*keyvalue.KeyValue{result.Entry})
	return readMetadata(result.Entry, scope, id, result.ReadRevision)
}

// List returns metadata in a bounded, fixed-revision page. Revoked records
// stay visible as historical evidence and are never eligible credentials.
func (repository *Repository) List(
	ctx context.Context,
	scope Scope,
	request keyvalue.PageRequest,
) (keyvalue.Page[Connection], error) {
	if err := scope.validate(); err != nil {
		return keyvalue.Page[Connection]{}, err
	}
	return recordquery.ListPrimary(ctx, repository.store, "github-connections", string(scope.Kind), scope.ID,
		connectionPrefix(scope), IDKind, request, decodeConnection, func(record Connection) string { return record.ID },
		func(record Connection) bool { return record.Scope == scope })
}

// Rotate advances the credential generation without changing provider IDs.
func (repository *Repository) Rotate(
	ctx context.Context,
	scope Scope,
	id string,
	expectedRevision int64,
	expectedGeneration uint64,
	replacement Credentials,
	at time.Time,
	publications ...Publication,
) (keyvalue.Versioned[Connection], error) {
	return repository.replace(
		ctx,
		scope,
		id,
		expectedRevision,
		expectedGeneration,
		at,
		false,
		replacement,
		publications,
	)
}

// Revoke removes the live credential and leaves an immutable revocation record.
func (repository *Repository) Revoke(
	ctx context.Context,
	scope Scope,
	id string,
	expectedRevision int64,
	expectedGeneration uint64,
	at time.Time,
	publications ...Publication,
) (keyvalue.Versioned[Connection], error) {
	return repository.replace(
		ctx,
		scope,
		id,
		expectedRevision,
		expectedGeneration,
		at,
		true,
		Credentials{},
		publications,
	)
}

func (repository *Repository) replace(
	ctx context.Context,
	scope Scope,
	id string,
	expectedRevision int64,
	expectedGeneration uint64,
	at time.Time,
	revoke bool,
	replacement Credentials,
	publications []Publication,
) (keyvalue.Versioned[Connection], error) {
	if expectedRevision <= 0 || expectedGeneration == 0 {
		return keyvalue.Versioned[Connection]{}, errs.New(
			errs.KindValidationFailed,
			"GitHub connection revision and generation are required",
		)
	}
	current, sealed, err := repository.readPair(ctx, scope, id)
	if err != nil {
		return keyvalue.Versioned[Connection]{}, err
	}
	defer clear(sealed.ciphertext)
	record := current.Record
	if current.Revision != expectedRevision || record.Generation != expectedGeneration ||
		record.RevokedAt != nil || record.Generation == math.MaxUint64 {
		return keyvalue.Versioned[Connection]{}, changed()
	}
	if !recordcodec.IsCanonicalUTC(at) || at.Before(record.UpdatedAt) {
		return keyvalue.Versioned[Connection]{}, errs.New(
			errs.KindValidationFailed,
			"GitHub connection update timestamp is invalid",
		)
	}
	conditions := comparison(current, sealed)
	if revoke {
		record.RevokedAt = &at
	} else {
		owner, err := repository.ownerGuard(ctx, scope, current.ReadRevision)
		if err != nil {
			return keyvalue.Versioned[Connection]{}, err
		}
		conditions = append(conditions, owner...)
	}
	record.Generation++
	record.UpdatedAt = at
	return repository.write(ctx, record, conditions, replacement, publications)
}

func (repository *Repository) write(
	ctx context.Context,
	record Connection,
	conditions []keyvalue.Condition,
	credentials Credentials,
	publications []Publication,
) (keyvalue.Versioned[Connection], error) {
	metadata, err := recordcodec.Encode(connectionKind, record)
	if err != nil {
		return keyvalue.Versioned[Connection]{}, err
	}
	defer clear(metadata)
	private := keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: credentialKey(record.ID)}
	if record.RevokedAt == nil {
		value, err := repository.seal(ctx, record, credentials)
		if err != nil {
			return keyvalue.Versioned[Connection]{}, err
		}
		defer clear(value)
		private.Type, private.Value = keyvalue.MutationPut, value
	}
	mutations := []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: connectionKey(record.Scope, record.ID), Value: metadata},
		private,
	}
	for _, publish := range publications {
		if publish == nil {
			return keyvalue.Versioned[Connection]{}, errs.New(
				errs.KindInternal,
				"GitHub connection publication is required",
			)
		}
		publicRecord := record
		if record.RevokedAt != nil {
			revoked := *record.RevokedAt
			publicRecord.RevokedAt = &revoked
		}
		compare, writes, err := publish(publicRecord)
		if err != nil {
			return keyvalue.Versioned[Connection]{}, err
		}
		conditions = append(conditions, compare...)
		mutations = append(mutations, writes...)
	}
	result, err := repository.store.Transact(ctx, conditions, mutations)
	defer keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return keyvalue.Versioned[Connection]{}, err
	}
	if !result.Succeeded {
		return keyvalue.Versioned[Connection]{}, changed()
	}
	if result.Revision <= 0 {
		return keyvalue.Versioned[Connection]{}, corruption()
	}
	return keyvalue.Versioned[Connection]{Record: record, Revision: result.Revision, ReadRevision: result.Revision}, nil
}

func verifyLookup(ctx context.Context, scope Scope, id string) error {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return err
	}
	if err := scope.validate(); err != nil {
		return err
	}
	return recordcodec.ValidateID(IDKind, id)
}

func readMetadata(
	entry *keyvalue.KeyValue,
	scope Scope,
	id string,
	at int64,
) (keyvalue.Versioned[Connection], error) {
	if entry == nil {
		return keyvalue.Versioned[Connection]{}, errs.New(errs.KindRequestNotFound, "GitHub connection was not found")
	}
	if entry.Key != connectionKey(scope, id) || entry.ModRevision <= 0 || entry.ModRevision > at {
		return keyvalue.Versioned[Connection]{}, corruption()
	}
	record, err := decodeConnection(entry.Value)
	if err != nil || record.Scope != scope || record.ID != id {
		return keyvalue.Versioned[Connection]{}, corruption()
	}
	return keyvalue.Versioned[Connection]{Record: record, Revision: entry.ModRevision, ReadRevision: at}, nil
}

func changed() error {
	return errs.New(errs.KindStateConflict, "GitHub connection or credential generation changed")
}
