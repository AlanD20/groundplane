package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) releaseNetworkPruneAuthority(ctx context.Context, task TaskRecord,
	revision int64,
) (bool, []keyvalue.Condition, error) {
	if taskjournal.IsReleaseNetworkChild(task.Params) {
		key := taskjournal.TaskStorageKey(task.Params[taskjournal.TaskReleaseNetworkParentParam])
		read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
		if err != nil {
			return false, nil, err
		}
		if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
			return false, nil, errs.New(errs.KindInternal, "release network retention parent read is incomplete")
		}
		defer keyvalue.ClearValues(read.Values)
		if read.Values[0] != nil {
			return true, nil, nil
		} // Parent may still need its terminal dependency outcome.
		return false, []keyvalue.Condition{{Key: key}}, nil
	}
	publicationID := task.Params[releaserender.TaskReleasePublicationParam]
	if publicationID == "" || task.RetryOf != "" ||
		task.Type != taskjournal.TaskDeploy && task.Type != taskjournal.TaskRollback {
		return false, nil, nil
	}
	key := releases.ReleasePublicationKey(publicationID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return false, nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return false, nil, releases.CorruptReleaseRecord()
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return false, []keyvalue.Condition{{Key: key}}, nil
	}
	marker, err := releases.DecodeReleaseRecord[releases.ReleasePublicationMarker](
		read.Values[0].Value,
		"release-publication",
	)
	if err != nil {
		return false, nil, err
	}
	if marker.NetworkPreparationTaskID == "" {
		return false, nil, nil
	}
	if marker.OperationID != task.OperationID || marker.PublicationID != publicationID {
		return false, nil, releases.CorruptReleaseRecord()
	}
	childKey := taskjournal.TaskStorageKey(marker.NetworkPreparationTaskID)
	childRead, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{childKey}, Revision: revision},
	)
	if err != nil {
		return false, nil, err
	}
	if childRead == nil || childRead.ReadRevision != revision || len(childRead.Values) != 1 ||
		childRead.Values[0] == nil {
		return false, nil, releases.CorruptReleaseRecord()
	}
	defer keyvalue.ClearValues(childRead.Values)
	child, err := DecodeTaskRecord(childRead.Values[0].Value)
	if err != nil {
		return false, nil, err
	}
	if child.ID != marker.NetworkPreparationTaskID ||
		child.Params[taskjournal.TaskReleaseNetworkParentParam] != task.ID ||
		child.Owner != task.Owner {
		return false, nil, releases.CorruptReleaseRecord()
	}
	if !releaseNetworkEffectsSettled(child) {
		return true, nil, nil
	}
	fenceKey := releases.ReleaseFenceSetKey(task.Owner.EnvironmentID)
	fenceRead, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{fenceKey}, Revision: revision},
	)
	if err != nil {
		return false, nil, err
	}
	if fenceRead == nil || fenceRead.ReadRevision != revision || len(fenceRead.Values) != 1 {
		return false, nil, releases.CorruptReleaseRecord()
	}
	defer keyvalue.ClearValues(fenceRead.Values)
	fenceCondition := keyvalue.Condition{Key: fenceKey}
	if fenceRead.Values[0] != nil {
		fence, err := releases.DecodeReleaseRecord[releases.ReleaseFenceSet](
			fenceRead.Values[0].Value,
			"release-fence-set",
		)
		if err != nil {
			return false, nil, err
		}
		if fence.OperationID == task.OperationID {
			return true, nil, nil
		}
		fenceCondition.ModRevision = fenceRead.Values[0].ModRevision
	}
	return false, []keyvalue.Condition{{Key: key, ModRevision: read.Values[0].ModRevision},
		{Key: childKey, ModRevision: childRead.Values[0].ModRevision}, fenceCondition}, nil
}
