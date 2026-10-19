package runtime

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// Apply joins the captured Agent update and the exact protected replay response
// in one transaction. Registration storage does not depend on its parent package.
func (service *durableLocalAgentConfigIdempotency) Apply(
	ctx context.Context,
	update agentregistration.ConfigurationUpdate,
	marker idempotency.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	conditions, mutations, err := update.Changes()
	if err != nil {
		return etcd.IdempotencyTransactionResult{}, err
	}
	defer keyvalue.ClearMutationValues(mutations)
	plan, err := etcd.NewIdempotencyMutationPlan(conditions, mutations, update.ClassifyConflict)
	if err != nil {
		return etcd.IdempotencyTransactionResult{}, err
	}
	return service.repository.Apply(ctx, marker, plan)
}
