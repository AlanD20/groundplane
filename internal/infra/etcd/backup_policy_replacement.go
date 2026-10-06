package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/core"
	backuppolicymutations "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicymutations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/desiredauthoring"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
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
	idempotency, err := NewIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	state, matches, err := desiredauthoring.CheckConditions(ctx, repository.store, plan.Conditions())
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	evidence, err := idempotency.ReadAtRevision(ctx, marker.Locator, state.ReadRevision)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if evidence != nil {
		existing, err := evidence.Marker()
		return IdempotencyTransactionResult{kind: idempotencyTransactionExisting, revision: state.ReadRevision, marker: existing}, err
	}
	if !matches {
		return IdempotencyTransactionResult{kind: idempotencyTransactionConflict, revision: state.ReadRevision, conflict: plan.ClassifyConflict(state.Values)}, nil
	}
	if plan.OperationCount(marker) > etcdstore.MaximumOperations {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"backup policy replacement exceeds the atomic transaction limit",
		)
	}
	policy := prepared.Projection()
	publication, err := prepareDirectDesiredProjectionPublication(ctx, repository.store, policy.EnvironmentID, marker,
		func(input *core.BlueprintDesiredInput, projection *projectionrecord.EnvironmentComposeProjection) error {
			return desiredauthoring.SetBackup(ctx, repository.store, input, projection, policy)
		})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clearRouteHeadPublication(publication)
	conditions, mutations, classify, err := publication.bindDirectDesired(plan.Conditions(), plan.Mutations(),
		func(_ int64, values []*etcdstore.KeyValue) error { return plan.ClassifyConflict(values) })
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	mutationPlan, err := NewIdempotencyMutationPlan(conditions, mutations, classify)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, mutationPlan)
}
