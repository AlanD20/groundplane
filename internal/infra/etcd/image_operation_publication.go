package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/imageoperations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// CreateImageOperation commits the captured operation, its work and tag selection
// with request replay in one transaction.
func (repository *TaskRepository) CreateImageOperation(ctx context.Context,
	input imageoperations.Input, marker idempotency.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	change, err := imageoperations.PreparePublication(input, marker)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer keyvalue.ClearMutationValues(change.Mutations)
	plan, err := newIdempotencyMutationPlanForMarker(idempotency.IdempotencyMarkerTask,
		change.Conditions, change.Mutations, func(_ int64, values []*keyvalue.KeyValue) error {
			if len(values) != len(change.Conditions) {
				return errs.New(errs.KindInternal, "image operation publication evidence is incomplete")
			}
			return errs.New(errs.KindStateConflict, "Image operation or an unfinished tag write changed before acceptance; refresh its Task before retrying")
		})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	markers, err := NewIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return markers.Apply(ctx, marker, plan)
}
