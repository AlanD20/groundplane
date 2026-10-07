package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	removalrecord "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
)

type volumeRemovalIdempotencyPruneGuard struct {
	store idempotencyRepositoryStore
}

// NewVolumeRemovalAwareIdempotencyRetention composes the idempotency-owned
// collector with the Volume-removal authority that can outlive one Task.
func NewVolumeRemovalAwareIdempotencyRetention(
	store idempotencyRepositoryStore,
) (*idempotencyrecord.RetentionRepository, error) {
	guard := &volumeRemovalIdempotencyPruneGuard{store: store}
	return idempotencyrecord.NewRetentionRepository(store, guard.evaluate)
}

func (guard *volumeRemovalIdempotencyPruneGuard) evaluate(
	ctx context.Context,
	input idempotencyrecord.PruneGuardInput,
) (idempotencyrecord.PruneGuardResult, error) {
	if input.MarkerKind != idempotencyrecord.IdempotencyMarkerTask {
		return idempotencyrecord.PruneGuardResult{}, nil
	}
	taskRead, err := guard.store.GetMany(
		ctx,
		etcdstore.GetManyRequest{Keys: []string{taskjournal.TaskStorageKey(input.TaskID)}, Revision: input.Revision},
	)
	if err != nil {
		return idempotencyrecord.PruneGuardResult{}, err
	}
	if taskRead == nil || taskRead.ReadRevision != input.Revision || len(taskRead.Values) != 1 ||
		taskRead.Values[0] == nil || taskRead.Values[0].Key != taskjournal.TaskStorageKey(input.TaskID) ||
		taskRead.Values[0].ModRevision <= 0 {
		return idempotencyrecord.PruneGuardResult{}, idempotencyrecord.CorruptIdempotencyMarker()
	}
	defer etcdstore.ClearValues(taskRead.Values)
	task, err := DecodeTaskRecord(taskRead.Values[0].Value)
	if err != nil || task.ID != input.TaskID {
		return idempotencyrecord.PruneGuardResult{}, idempotencyrecord.CorruptIdempotencyMarker()
	}
	if task.Type != taskjournal.TaskRemove ||
		task.Params[taskjournal.TaskResourceKindParam] != taskjournal.TaskResourceVolume {
		return idempotencyrecord.PruneGuardResult{}, nil
	}
	root := removalrecord.Root(task.OperationID)
	operation, err := guard.store.Range(
		ctx,
		etcdstore.RangeRequest{Prefix: root, Limit: 1, Revision: input.Revision},
	)
	if err != nil {
		return idempotencyrecord.PruneGuardResult{}, err
	}
	if operation == nil || operation.ReadRevision != input.Revision || len(operation.Values) > 1 {
		return idempotencyrecord.PruneGuardResult{}, idempotencyrecord.CorruptIdempotencyMarker()
	}
	defer etcdstore.ClearRangeValues(operation.Values)
	if len(operation.Values) != 0 {
		return idempotencyrecord.PruneGuardResult{Retain: true}, nil
	}
	rootFences, rootPending, err := guard.rootFences(ctx, task, input.Revision)
	if err != nil || rootPending {
		return idempotencyrecord.PruneGuardResult{Retain: rootPending}, err
	}
	keys := []string{removalrecord.OwnerKey(task.Target), removalrecord.EnvironmentLockKey(task.Owner.EnvironmentID)}
	owners, err := guard.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: input.Revision})
	if err != nil {
		return idempotencyrecord.PruneGuardResult{}, err
	}
	if owners == nil || owners.ReadRevision != input.Revision || len(owners.Values) != len(keys) {
		return idempotencyrecord.PruneGuardResult{}, idempotencyrecord.CorruptIdempotencyMarker()
	}
	defer etcdstore.ClearValues(owners.Values)
	fences := []etcdstore.Condition{
		{Key: taskjournal.TaskStorageKey(task.ID), ModRevision: taskRead.Values[0].ModRevision},
		{Key: root, Prefix: true},
	}
	fences = append(fences, rootFences...)
	for index, value := range owners.Values {
		if value != nil {
			owner, err := removalrecord.DecodeOwner(value.Value)
			if err != nil || value.Key != keys[index] || value.ModRevision <= 0 ||
				index == 0 && owner.VolumeID != task.Target ||
				index == 1 && owner.EnvironmentID != task.Owner.EnvironmentID {
				return idempotencyrecord.PruneGuardResult{}, idempotencyrecord.CorruptIdempotencyMarker()
			}
			if owner.OperationID == task.OperationID {
				return idempotencyrecord.PruneGuardResult{Retain: true}, nil
			}
		}
		fences = append(fences, etcdstore.Condition{Key: keys[index], ModRevision: etcdstore.RevisionOf(value)})
	}
	return idempotencyrecord.PruneGuardResult{Conditions: fences}, nil
}

func (guard *volumeRemovalIdempotencyPruneGuard) rootFences(
	ctx context.Context,
	task TaskRecord,
	revision int64,
) ([]etcdstore.Condition, bool, error) {
	originID := task.Params[removalrecord.OriginTaskParam]
	if originID == task.ID {
		return nil, false, nil // The candidate's own terminal marker is the root.
	}
	if recordcodec.ValidateID(ids.KindTask, originID) != nil {
		return nil, false, idempotencyrecord.CorruptIdempotencyMarker()
	}
	read, err := guard.store.GetMany(
		ctx,
		etcdstore.GetManyRequest{Keys: []string{taskjournal.TaskStorageKey(originID)}, Revision: revision},
	)
	if err != nil {
		return nil, false, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return nil, false, idempotencyrecord.CorruptIdempotencyMarker()
	}
	defer etcdstore.ClearValues(read.Values)
	fences := []etcdstore.Condition{
		{Key: taskjournal.TaskStorageKey(originID), ModRevision: etcdstore.RevisionOf(read.Values[0])},
	}
	if read.Values[0] == nil {
		return fences, false, nil // Marker-first GC already removed the original Task.
	}
	origin, err := DecodeTaskRecord(read.Values[0].Value)
	if err != nil || origin.ID != originID || origin.OperationID != task.OperationID || origin.Owner != task.Owner ||
		origin.idempotencyMarker == nil || read.Values[0].Key != taskjournal.TaskStorageKey(originID) ||
		read.Values[0].ModRevision <= 0 {
		return nil, false, idempotencyrecord.CorruptIdempotencyMarker()
	}
	key, err := idempotencyrecord.IdempotencyMarkerKey(*origin.idempotencyMarker)
	if err != nil {
		return nil, false, err
	}
	markerRead, err := guard.store.GetMany(
		ctx,
		etcdstore.GetManyRequest{Keys: []string{key}, Revision: revision},
	)
	if err != nil {
		return nil, false, err
	}
	if markerRead == nil || markerRead.ReadRevision != revision || len(markerRead.Values) != 1 {
		return nil, false, idempotencyrecord.CorruptIdempotencyMarker()
	}
	defer etcdstore.ClearValues(markerRead.Values)
	if value := markerRead.Values[0]; value != nil {
		marker, err := idempotencyrecord.DecodeIdempotencyMarker(value.Value, *origin.idempotencyMarker)
		if err != nil || value.Key != key || value.ModRevision <= 0 || marker.TaskID != originID {
			return nil, false, idempotencyrecord.CorruptIdempotencyMarker()
		}
		defer clear(marker.Intent.Ciphertext)
		defer clear(marker.Response.Body)
		if marker.State == idempotencyrecord.IdempotencyMarkerPending {
			return nil, true, nil
		}
	}
	return append(
		fences,
		etcdstore.Condition{Key: key, ModRevision: etcdstore.RevisionOf(markerRead.Values[0])},
	), false, nil
}
