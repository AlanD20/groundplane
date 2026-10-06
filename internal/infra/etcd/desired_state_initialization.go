package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/infra/etcd/desiredauthoring"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"time"
)

// InitializeEnvironmentDesiredState durably initializes first Entry/Zone authoring without host work.
func (repository *HierarchyRepository) InitializeEnvironmentDesiredState(ctx context.Context, environmentID string,
	locator idempotencyrecord.IdempotencyLocator, intent idempotencyrecord.ProtectedIntentRecord, now time.Time,
) error {
	return desiredauthoring.Initialize(ctx, repository.store, environmentID, locator, intent, now)
}
