package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/resolverexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/resolverwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PublishSystemResolverWork is the internal periodic reconciliation publisher.
// Operator requests instead compile after their whole-Task prerequisites and
// join PrepareResolverAuthority to promotion of their existing Task identity.
func (repository *TaskRepository) PublishSystemResolverWork(ctx context.Context, input resolverexecution.Input,
	capture resolverwork.Capture, marker idempotency.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if capture.Actor != workexecution.SystemActor || capture.Validate(input) != nil ||
		marker.Locator.ScopeKind != idempotency.IdempotencyScopePlatform || marker.Locator.ScopeID != "-" {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"automatic CoreDNS work requires System authority and Platform replay",
		)
	}
	if existing, found, err := existingIdempotencyTransaction(ctx, repository.store, marker); err != nil || found {
		return existing, err
	}
	task, item, err := resolverwork.NewWork(input, capture)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	native, err := workqueue.PrepareSingleTask(task, item, marker)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer keyvalue.ClearMutationValues(native.Mutations)
	authority, err := repository.PrepareResolverAuthority(ctx, input, capture)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer keyvalue.ClearMutationValues(authority.Mutations)
	change, err := workqueue.JoinChanges(native, authority)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer keyvalue.ClearMutationValues(change.Mutations)
	plan, err := newIdempotencyMutationPlanForMarker(
		idempotency.IdempotencyMarkerTask,
		change.Conditions,
		change.Mutations,
		func(_ int64, _ []*keyvalue.KeyValue) error {
			return errs.New(errs.KindStateConflict, "CoreDNS automatic reconciliation sources changed")
		},
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	replay, err := NewIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return replay.Apply(ctx, marker, plan)
}
