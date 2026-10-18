package attachremoval

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/desiredauthoring"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// prepareCascadeDesired removes only this captured binding from the consumer's
// authored input. It preserves the current rendered artifact and every unrelated
// declaration. The caller joins its head CAS with binding ownership and work
// publication; staging alone never changes the consumer Environment.
func (repository *InputRepository) prepareCascadeDesired(ctx context.Context, task workqueue.TaskSnapshot,
	item workexecution.Item, source attachments.Record, revision int64,
) (workqueue.Change, error) {
	marker, guards, err := repository.transactions.ReadTaskReplay(ctx, task, revision)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	if marker.State != idempotency.IdempotencyMarkerPending || task.Task.PublicationComplete ||
		item.TaskID != task.Task.ID || item.TargetID != source.ID || item.Operation != workexecution.Detach {
		return workqueue.Change{}, checkpointConflict("consumer desired removal requires its accepted unpublished Task")
	}
	// Metadata staging has its own Environment-scoped identity. This locator is
	// not a second public request or child Task: no replay marker is written for
	// it. The original protected intent and native replay comparisons remain the
	// authority, while the work ID identifies this one consumer mutation.
	marker.Locator = idempotency.IdempotencyLocator{
		ScopeKind: idempotency.IdempotencyScopeEnvironment, ScopeID: source.EnvironmentID,
		Method: "POST", Route: "/tasks/{id}/work/{work_id}/consumer-removal", Key: item.ID,
	}
	desired, err := desiredauthoring.Prepare(ctx, repository.store, source.EnvironmentID, marker,
		desiredauthoring.AttachmentMutation(ctx, repository.store, source, "", true))
	if err != nil {
		return workqueue.Change{}, err
	}
	return workqueue.Change{Conditions: append(guards, desired.Conditions...), Mutations: desired.Mutations}, nil
}
