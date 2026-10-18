package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ApplyExecutionRetry joins an operation-authorized exact-attempt retry with
// request replay. It never allocates a replacement Task or retries a failed CAS.
func (repository *IdempotencyRepository) ApplyExecutionRetry(ctx context.Context,
	change workqueue.Change, marker idempotency.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if marker.Kind != idempotency.IdempotencyMarkerDirect || marker.State != idempotency.IdempotencyMarkerCompleted {
		return IdempotencyTransactionResult{}, errs.New(errs.KindInternal, "execution retry requires a completed request receipt")
	}
	plan, err := newIdempotencyMutationPlanForMarker(idempotency.IdempotencyMarkerDirect, change.Conditions, change.Mutations,
		func(_ int64, _ []*keyvalue.KeyValue) error {
			return errs.New(errs.KindStateConflict, "The selected execution changed before Retry was accepted; refresh its attempt")
		})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := plan.enforceTransactionBounds(func(conditions []keyvalue.Condition, mutations []keyvalue.Mutation) error {
		compares := len(keyvalue.ImageSelectionConditions(ctx, conditions))
		if compares+len(mutations) > keyvalue.MaximumOperations || 2*compares+len(mutations) > 128 {
			return errs.New(errs.KindValidationFailed, "execution retry exceeds the atomic request limit")
		}
		return nil
	}); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return repository.Apply(ctx, marker, plan)
}
