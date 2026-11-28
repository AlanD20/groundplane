package githubconnection

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type credentialSnapshot struct {
	ciphertext []byte
	revision   int64
}

// ActiveFence captures current ownership and credential-generation comparisons
// for a Controller transaction accepting new work.
func (repository *Repository) ActiveFence(
	ctx context.Context,
	scope Scope,
	id string,
	expectedGeneration uint64,
) ([]keyvalue.Condition, error) {
	current, sealed, err := repository.readActive(ctx, scope, id, expectedGeneration)
	if err != nil {
		return nil, err
	}
	defer clear(sealed.ciphertext)
	owner, err := repository.ownerGuard(ctx, scope, current.ReadRevision)
	if err != nil {
		return nil, err
	}
	return append(comparison(current, sealed), owner...), nil
}

// WithCredentials borrows credentials for one GitHub operation. The consumer
// must use the values synchronously and must not retain or expose them. Callers
// persist their operation intent separately and handle any external request
// that was already in flight when revocation committed.
func (repository *Repository) WithCredentials(
	ctx context.Context,
	scope Scope,
	id string,
	expectedGeneration uint64,
	consume func(Connection, Credentials) error,
) error {
	if consume == nil {
		return errs.New(errs.KindInternal, "GitHub connection credential consumer is required")
	}
	current, sealed, err := repository.readActive(ctx, scope, id, expectedGeneration)
	if err != nil {
		return err
	}
	defer clear(sealed.ciphertext)
	plaintext, err := repository.cipher.Open(ctx, sealed.ciphertext)
	defer clear(plaintext)
	if err != nil {
		return cipherError(ctx, "GitHub connection credential could not be opened")
	}
	if len(plaintext) == 0 || len(plaintext) > maxCiphertext {
		return corruption()
	}
	material, err := recordcodec.Decode[credentialMaterial](plaintext, plaintextKind)
	defer material.Credentials.clear()
	if err != nil || material.Identity != identityOf(current.Record) || !material.Credentials.valid() {
		return corruption()
	}
	latest, err := repository.Get(ctx, scope, id)
	if err != nil {
		return err
	}
	if latest.Revision != current.Revision || latest.Record.Generation != expectedGeneration {
		return changed()
	}
	if _, err := repository.ownerGuard(ctx, scope, latest.ReadRevision); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return consume(latest.Record, material.Credentials)
}

func (repository *Repository) readActive(
	ctx context.Context,
	scope Scope,
	id string,
	expectedGeneration uint64,
) (keyvalue.Versioned[Connection], credentialSnapshot, error) {
	if expectedGeneration == 0 {
		return keyvalue.Versioned[Connection]{}, credentialSnapshot{},
			errs.New(errs.KindValidationFailed, "GitHub connection credential generation is required")
	}
	current, sealed, err := repository.readPair(ctx, scope, id)
	if err != nil {
		return keyvalue.Versioned[Connection]{}, credentialSnapshot{}, err
	}
	if current.Record.RevokedAt != nil || current.Record.Generation != expectedGeneration {
		clear(sealed.ciphertext)
		return keyvalue.Versioned[Connection]{}, credentialSnapshot{}, changed()
	}
	return current, sealed, nil
}

func (repository *Repository) readPair(
	ctx context.Context,
	scope Scope,
	id string,
) (keyvalue.Versioned[Connection], credentialSnapshot, error) {
	if err := verifyLookup(ctx, scope, id); err != nil {
		return keyvalue.Versioned[Connection]{}, credentialSnapshot{}, err
	}
	privateKey := credentialKey(id)
	result, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{connectionKey(scope, id), privateKey},
	})
	if err != nil {
		return keyvalue.Versioned[Connection]{}, credentialSnapshot{}, err
	}
	if result == nil {
		return keyvalue.Versioned[Connection]{}, credentialSnapshot{}, corruption()
	}
	defer keyvalue.ClearValues(result.Values)
	if len(result.Values) != 2 || result.ReadRevision <= 0 {
		return keyvalue.Versioned[Connection]{}, credentialSnapshot{}, corruption()
	}
	current, err := readMetadata(result.Values[0], scope, id, result.ReadRevision)
	if err != nil {
		return keyvalue.Versioned[Connection]{}, credentialSnapshot{}, err
	}
	entry := result.Values[1]
	if current.Record.RevokedAt != nil && entry == nil {
		return current, credentialSnapshot{}, nil
	}
	if current.Record.RevokedAt != nil || entry == nil || entry.Key != privateKey ||
		entry.ModRevision != current.Revision {
		return keyvalue.Versioned[Connection]{}, credentialSnapshot{}, corruption()
	}
	if len(entry.Value) > recordcodec.MaximumValueBytes {
		return keyvalue.Versioned[Connection]{}, credentialSnapshot{}, corruption()
	}
	sealed, err := recordcodec.Decode[sealedCredential](entry.Value, sealedKind)
	if err != nil || sealed.Identity != identityOf(current.Record) ||
		len(sealed.Ciphertext) == 0 || len(sealed.Ciphertext) > maxCiphertext {
		clear(sealed.Ciphertext)
		return keyvalue.Versioned[Connection]{}, credentialSnapshot{}, corruption()
	}
	return current, credentialSnapshot{ciphertext: sealed.Ciphertext, revision: entry.ModRevision}, nil
}

func (repository *Repository) seal(ctx context.Context, record Connection, credentials Credentials) ([]byte, error) {
	if !credentials.valid() {
		return nil, errs.New(
			errs.KindValidationFailed,
			"GitHub connection private key or webhook secret exceeds the allowed bounds",
		)
	}
	identity := identityOf(record)
	plaintext, err := recordcodec.Encode(plaintextKind, credentialMaterial{
		Identity: identity, Credentials: credentials,
	})
	defer clear(plaintext)
	if err != nil {
		return nil, err
	}
	ciphertext, err := repository.cipher.Seal(ctx, plaintext)
	defer clear(ciphertext)
	if err != nil {
		return nil, cipherError(ctx, "GitHub connection credential could not be sealed")
	}
	if len(ciphertext) == 0 || len(ciphertext) > maxCiphertext {
		return nil, corruption()
	}
	return recordcodec.Encode(sealedKind, sealedCredential{Identity: identity, Ciphertext: ciphertext})
}

func comparison(current keyvalue.Versioned[Connection], sealed credentialSnapshot) []keyvalue.Condition {
	return []keyvalue.Condition{
		{Key: connectionKey(current.Record.Scope, current.Record.ID), ModRevision: current.Revision},
		{Key: credentialKey(current.Record.ID), ModRevision: sealed.revision},
	}
}

func cipherError(ctx context.Context, message string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return errs.New(errs.KindInternal, message)
}
