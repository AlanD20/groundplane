package etcd

import (
	"context"
	connectormutations "github.com/AlanD20/groundplane/internal/infra/etcd/connectormutations"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *ConnectorRepository) EditConnectorIdempotent(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	current etcdstore.Versioned[connectorrecord.Record],
	credentialDigest string,
	record connectorrecord.Record,
	credentials connectorrecord.EncryptedCredentials,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := connectormutations.ValidateEditMarker(record, marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if existing, found, err := existingIdempotencyTransaction(ctx, repository.store, marker); err != nil || found {
		return existing, err
	}
	publication, err := repository.PrepareConnectorEdit(
		ctx,
		environment,
		project,
		current,
		credentialDigest,
		record,
		credentials,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer etcdstore.ClearMutationValues(publication.Mutations)
	plan, err := NewIdempotencyMutationPlan(
		publication.Conditions,
		publication.Mutations,
		func(_ int64, reads []*etcdstore.KeyValue) error {
			return errs.New(
				errs.KindStateConflict,
				"connector or its backup, owner or credential authority changed; reload before editing",
			)
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
