package workqueue

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
)

// ReadTaskReplay returns the accepted request and comparisons binding it to the
// native Task. Callers own and must clear the returned encrypted intent/body.
func (repository *Repository) ReadTaskReplay(ctx context.Context, task TaskSnapshot, revision int64,
) (idempotency.IdempotencyMarker, []keyvalue.Condition, error) {
	if task.Revision <= 0 || revision < task.Revision || task.Task.Validate() != nil {
		return idempotency.IdempotencyMarker{}, nil, invalid("request replay requires a captured Task revision")
	}
	bindingKey := replayBindingKey(task.Task.ID)
	read, err := repository.read(ctx, []string{bindingKey}, revision)
	if err != nil {
		return idempotency.IdempotencyMarker{}, nil, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return idempotency.IdempotencyMarker{}, nil, corrupt()
	}
	locator, err := recordcodec.Decode[idempotency.IdempotencyLocator](read.Values[0].Value, "execution_replay")
	if err != nil {
		return idempotency.IdempotencyMarker{}, nil, err
	}
	markerKey, err := idempotency.IdempotencyMarkerKey(locator)
	if err != nil {
		return idempotency.IdempotencyMarker{}, nil, corrupt()
	}
	markers, err := repository.read(ctx, []string{markerKey}, revision)
	if err != nil {
		return idempotency.IdempotencyMarker{}, nil, err
	}
	defer keyvalue.ClearValues(markers.Values)
	if markers.Values[0] == nil {
		return idempotency.IdempotencyMarker{}, nil, corrupt()
	}
	marker, err := idempotency.DecodeIdempotencyMarker(markers.Values[0].Value, locator)
	if err != nil {
		return idempotency.IdempotencyMarker{}, nil, err
	}
	if marker.Kind != idempotency.IdempotencyMarkerTask || marker.TaskID != task.Task.ID ||
		!marker.CreatedAt.Equal(task.Task.CreatedAt) || marker.Intent.CiphertextDigest != task.Task.Intent.SHA256 {
		clear(marker.Intent.Ciphertext)
		clear(marker.Response.Body)
		return idempotency.IdempotencyMarker{}, nil, corrupt()
	}
	return marker, []keyvalue.Condition{
		{Key: TaskKey(task.Task.ID), ModRevision: task.Revision},
		{Key: bindingKey, ModRevision: read.Values[0].ModRevision},
		{Key: markerKey, ModRevision: markers.Values[0].ModRevision},
	}, nil
}
