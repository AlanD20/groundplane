package etcd

import (
	"context"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/desiredauthoring"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
)

func prepareAttachDesiredAuthoring(ctx context.Context, store hierarchyStore, record attachrecord.Record,
	oldName string, remove bool, marker idempotencyrecord.IdempotencyMarker,
) (routeHeadPublication, error) {
	return prepareDirectDesiredProjectionPublication(
		ctx,
		store,
		record.EnvironmentID,
		marker,
		desiredauthoring.AttachmentMutation(ctx, store, record, oldName, remove),
	)
}
