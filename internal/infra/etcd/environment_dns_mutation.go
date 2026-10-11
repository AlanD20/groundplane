package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/desiredauthoring"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// MutateEnvironmentDNS atomically publishes a single DNS intention, its hostname
// reservation and replay response. No other Blueprint unit is executed. The
// platform resolver reconciler observes the new head through the shared name
// generation and still requires acknowledgement before retiring old claims.
func (repository *EnvironmentBlueprintRepository) MutateEnvironmentDNS(ctx context.Context,
	environmentID string, request desiredauthoring.DNSMutation, marker idempotency.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := idempotency.ValidateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if existing, found, err := existingIdempotencyTransaction(ctx, repository.store, marker); err != nil || found {
		return existing, err
	}
	publication, err := desiredauthoring.PrepareDNS(ctx, repository.store, environmentID, marker, request)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer keyvalue.ClearMutationValues(publication.Mutations)
	plan, err := NewIdempotencyMutationPlan(publication.Conditions, publication.Mutations,
		func(_ int64, values []*keyvalue.KeyValue) error {
			if len(values) != len(publication.Conditions) {
				return errs.New(errs.KindInternal, "DNS mutation compare evidence is incomplete")
			}
			return errs.New(errs.KindStateConflict, "DNS or its Environment changed while saving; refresh and retry")
		})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	replay, err := NewIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return replay.Apply(ctx, marker, plan)
}
