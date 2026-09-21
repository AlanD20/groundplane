package etcd

import (
	"context"
	backuppolicymutations "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicymutations"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ReplaceBackupPolicyProtected atomically replaces the Environment singleton,
// swaps Connector reverse references, creates an optional sealed era-1 key,
// and commits exact completed-direct replay evidence.
func (repository *BackupPolicyRepository) ReplaceBackupPolicyProtected(
	ctx context.Context,
	prepared backuppolicymutations.PreparedBackupPolicyReplacement,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	plan, err := prepared.PreparePublication(ctx, marker)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer plan.Clear()
	if plan.OperationCount(marker) > etcdstore.MaximumOperations {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"backup policy replacement exceeds the atomic transaction limit",
		)
	}
	mutationPlan, err := NewIdempotencyMutationPlan(
		plan.Conditions(),
		plan.Mutations(),
		func(_ int64, values []*etcdstore.KeyValue) error {
			return plan.ClassifyConflict(values)
		},
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := NewIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, mutationPlan)
}
