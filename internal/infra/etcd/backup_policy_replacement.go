package etcd

import (
	"context"
	backuppolicymutations "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicymutations"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// replaceBackupPolicyProtected atomically replaces the Environment singleton,
// swaps Connector reverse references, creates an optional sealed era-1 key,
// and commits exact completed-direct replay evidence.
func (repository *BackupPolicyRepository) replaceBackupPolicyProtected(
	ctx context.Context,
	candidate backuppolicymutations.ReplacementCandidate,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := backuppolicymutations.ValidateBackupPolicyReplacement(ctx, candidate, marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	plan, err := backuppolicymutations.PrepareBackupPolicyReplacement(candidate)
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
