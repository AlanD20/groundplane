package etcd

import (
	"context"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	groupstore "github.com/AlanD20/groundplane/internal/infra/etcd/releasegroups"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type releaseGroupTaskChange struct {
	applies    bool
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
	values     [][]byte
}

func (repository *TaskRepository) prepareReleaseGroupTaskRetry(
	ctx context.Context,
	source, retry TaskRecord,
	revision int64,
) (releaseGroupTaskChange, error) {
	applies, err := taskOwnsReleaseGroupRemoval(source)
	if err != nil || !applies {
		return releaseGroupTaskChange{}, err
	}
	if retry.Type != taskjournal.TaskRemove || retry.Target != source.Target ||
		retry.Params[taskjournal.TaskResourceKindParam] != taskjournal.TaskResourceReleaseGroup {
		return releaseGroupTaskChange{}, errs.New(errs.KindInternal, "release group retry changed its durable target")
	}
	stored, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		groupstore.ReleaseGroupRecordKey(
			source.Target,
		), deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetReleaseGroup), source.Target),
	}, Revision: revision})
	if err != nil {
		return releaseGroupTaskChange{}, err
	}
	if stored == nil || len(stored.Values) != 2 || stored.Values[0] == nil || stored.Values[1] != nil {
		return releaseGroupTaskChange{}, errs.New(
			errs.KindStateConflict,
			"release group is not available for deletion retry",
		)
	}
	group, err := groupstore.DecodeReleaseGroupStored(stored.Values[0].Value)
	if err != nil || group.ID != source.Target {
		return releaseGroupTaskChange{}, recordcodec.CorruptRecord()
	}
	indexes, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		groupstore.ReleaseGroupOwnerKey(
			group.EnvironmentID,
			group.ID,
		), groupstore.ReleaseGroupNameKey(group.EnvironmentID, group.Name),
		hierarchyrecord.EnvironmentKey(
			group.EnvironmentID,
		), hierarchyrecord.EnvironmentMutationEpochKey(group.EnvironmentID),
		deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetEnvironment), group.EnvironmentID),
		deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetProject), source.Owner.ProjectID),
		deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetTenant), source.Owner.TenantID),
	}, Revision: revision})
	if err != nil {
		return releaseGroupTaskChange{}, err
	}
	if indexes == nil || len(indexes.Values) != 7 || indexes.Values[0] == nil || indexes.Values[1] == nil ||
		indexes.Values[2] == nil || indexes.Values[3] == nil || indexes.Values[4] != nil ||
		indexes.Values[5] != nil || indexes.Values[6] != nil || source.Owner.EnvironmentID != group.EnvironmentID ||
		string(indexes.Values[0].Value) != group.ID || string(indexes.Values[1].Value) != group.ID {
		return releaseGroupTaskChange{}, errs.New(errs.KindResourceInUse, "release group owner is unavailable")
	}
	epoch, err := backupruntime.DecodeEnvironmentMutationEpochRecord(indexes.Values[3].Value)
	if err != nil || epoch.EnvironmentID != group.EnvironmentID {
		return releaseGroupTaskChange{}, recordcodec.CorruptRecord()
	}
	tombstone := deletionrecord.DeletionTombstoneRecord{
		TargetKind: deletionrecord.DeletionTargetReleaseGroup, TargetID: group.ID, TargetRevision: stored.Values[0].ModRevision,
		TaskID: retry.ID, Phase: deletionrecord.DeletionPhaseFinalizing, CreatedAt: retry.CreatedAt, UpdatedAt: retry.CreatedAt,
	}
	value, err := deletionrecord.EncodeDeletionTombstone(tombstone)
	if err != nil {
		return releaseGroupTaskChange{}, err
	}
	return releaseGroupTaskChange{
		applies: true,
		conditions: []etcdstore.Condition{
			{Key: groupstore.ReleaseGroupRecordKey(group.ID), ModRevision: stored.Values[0].ModRevision},
			{
				Key:         groupstore.ReleaseGroupOwnerKey(group.EnvironmentID, group.ID),
				ModRevision: indexes.Values[0].ModRevision,
			},
			{
				Key:         groupstore.ReleaseGroupNameKey(group.EnvironmentID, group.Name),
				ModRevision: indexes.Values[1].ModRevision,
			},
			{Key: hierarchyrecord.EnvironmentKey(group.EnvironmentID), ModRevision: indexes.Values[2].ModRevision},
			{
				Key:         hierarchyrecord.EnvironmentMutationEpochKey(group.EnvironmentID),
				ModRevision: indexes.Values[3].ModRevision,
			},
			{Key: deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetEnvironment), group.EnvironmentID)},
			{Key: deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetProject), source.Owner.ProjectID)},
			{Key: deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetTenant), source.Owner.TenantID)},
			{Key: deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetReleaseGroup), group.ID)},
		},
		mutations: []etcdstore.Mutation{
			{
				Type:  etcdstore.MutationPut,
				Key:   deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetReleaseGroup), group.ID),
				Value: value,
			},
		},
		values: [][]byte{value},
	}, nil
}

func (repository *TaskRepository) prepareReleaseGroupTaskAcknowledgement(
	ctx context.Context,
	task TaskRecord,
	terminal taskjournal.TaskStatus,
	revision int64,
) (releaseGroupTaskChange, error) {
	applies, err := taskOwnsReleaseGroupRemoval(task)
	if err != nil || !applies {
		return releaseGroupTaskChange{}, err
	}
	stored, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		groupstore.ReleaseGroupRecordKey(
			task.Target,
		), deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetReleaseGroup), task.Target),
	}, Revision: revision})
	if err != nil {
		return releaseGroupTaskChange{}, err
	}
	if stored == nil || len(stored.Values) != 2 || stored.Values[0] == nil || stored.Values[1] == nil {
		return releaseGroupTaskChange{}, errs.New(errs.KindInternal, "release group deletion state is inconsistent")
	}
	group, err := groupstore.DecodeReleaseGroupStored(stored.Values[0].Value)
	if err != nil || group.ID != task.Target {
		return releaseGroupTaskChange{}, recordcodec.CorruptRecord()
	}
	tombstone, err := deletionrecord.DecodeDeletionTombstone(stored.Values[1].Value)
	if err != nil || tombstone.TargetKind != deletionrecord.DeletionTargetReleaseGroup ||
		tombstone.TargetID != task.Target ||
		tombstone.TargetRevision != stored.Values[0].ModRevision ||
		tombstone.TaskID != task.ID ||
		tombstone.Phase != deletionrecord.DeletionPhaseFinalizing {
		return releaseGroupTaskChange{}, errs.New(
			errs.KindStateConflict,
			"release group deletion tombstone does not match its task",
		)
	}
	indexes, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		groupstore.ReleaseGroupOwnerKey(
			group.EnvironmentID,
			group.ID,
		), groupstore.ReleaseGroupNameKey(group.EnvironmentID, group.Name),
		hierarchyrecord.EnvironmentMutationEpochKey(
			group.EnvironmentID,
		), hierarchyrecord.EnvironmentKey(group.EnvironmentID),
		deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetEnvironment), group.EnvironmentID),
		deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetProject), task.Owner.ProjectID),
		deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetTenant), task.Owner.TenantID),
	}, Revision: revision})
	if err != nil {
		return releaseGroupTaskChange{}, err
	}
	if indexes == nil || len(indexes.Values) != 7 || indexes.Values[0] == nil || indexes.Values[1] == nil ||
		indexes.Values[2] == nil ||
		indexes.Values[3] == nil ||
		indexes.Values[4] != nil ||
		indexes.Values[5] != nil ||
		indexes.Values[6] != nil ||
		task.Owner.EnvironmentID != group.EnvironmentID ||
		string(indexes.Values[0].Value) != group.ID ||
		string(indexes.Values[1].Value) != group.ID {
		return releaseGroupTaskChange{}, errs.New(errs.KindInternal, "release group deletion indexes are corrupt")
	}
	epoch, err := backupruntime.DecodeEnvironmentMutationEpochRecord(indexes.Values[2].Value)
	if err != nil || epoch.EnvironmentID != group.EnvironmentID {
		return releaseGroupTaskChange{}, recordcodec.CorruptRecord()
	}
	epochValue, err := backupruntime.EncodeEnvironmentMutationEpochRecord(epoch)
	if err != nil {
		return releaseGroupTaskChange{}, err
	}
	change := releaseGroupTaskChange{
		applies: true,
		conditions: []etcdstore.Condition{
			{Key: groupstore.ReleaseGroupRecordKey(group.ID), ModRevision: stored.Values[0].ModRevision},
			{
				Key:         deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetReleaseGroup), group.ID),
				ModRevision: stored.Values[1].ModRevision,
			},
			{
				Key:         groupstore.ReleaseGroupOwnerKey(group.EnvironmentID, group.ID),
				ModRevision: indexes.Values[0].ModRevision,
			},
			{
				Key:         groupstore.ReleaseGroupNameKey(group.EnvironmentID, group.Name),
				ModRevision: indexes.Values[1].ModRevision,
			},
			{Key: hierarchyrecord.EnvironmentKey(group.EnvironmentID), ModRevision: indexes.Values[3].ModRevision},
			{Key: deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetEnvironment), group.EnvironmentID)},
			{Key: deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetProject), task.Owner.ProjectID)},
			{Key: deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetTenant), task.Owner.TenantID)},
		},
		mutations: []etcdstore.Mutation{
			{
				Type: etcdstore.MutationDelete,
				Key:  deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetReleaseGroup), group.ID),
			},
		},
	}
	if terminal == taskjournal.TaskStatusCompleted {
		collectionCondition, collectionMutation, collectionErr := groupstore.LoadReleaseGroupCollectionEpoch(
			ctx,
			repository.store,
			group.EnvironmentID,
			revision,
		)
		if collectionErr != nil {
			clear(epochValue)
			return releaseGroupTaskChange{}, collectionErr
		}
		change.conditions = append(
			change.conditions,
			etcdstore.Condition{
				Key:         hierarchyrecord.EnvironmentMutationEpochKey(group.EnvironmentID),
				ModRevision: indexes.Values[2].ModRevision,
			},
			collectionCondition,
		)
		change.mutations = append(
			change.mutations,
			etcdstore.Mutation{
				Type: etcdstore.MutationDelete,
				Key:  groupstore.ReleaseGroupOwnerKey(group.EnvironmentID, group.ID),
			},
			etcdstore.Mutation{
				Type: etcdstore.MutationDelete,
				Key:  groupstore.ReleaseGroupNameKey(group.EnvironmentID, group.Name),
			},
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: groupstore.ReleaseGroupRecordKey(group.ID)},
			etcdstore.Mutation{
				Type:  etcdstore.MutationPut,
				Key:   hierarchyrecord.EnvironmentMutationEpochKey(group.EnvironmentID),
				Value: epochValue,
			},
			collectionMutation,
		)
		change.values = append(change.values, epochValue, collectionMutation.Value)
	} else {
		clear(epochValue)
	}
	return change, nil
}

func (repository *TaskRepository) validateReleaseGroupTaskAcknowledgementReplay(
	ctx context.Context,
	task TaskRecord,
	terminal taskjournal.TaskStatus,
	revision int64,
) error {
	applies, err := taskOwnsReleaseGroupRemoval(task)
	if err != nil || !applies {
		return err
	}
	stored, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		groupstore.ReleaseGroupRecordKey(
			task.Target,
		), deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetReleaseGroup), task.Target),
	}, Revision: revision})
	if err != nil {
		return err
	}
	if stored == nil || len(stored.Values) != 2 || stored.Values[1] != nil {
		return errs.New(errs.KindStateConflict, "release group deletion terminal state does not match its task")
	}
	if terminal == taskjournal.TaskStatusCompleted && stored.Values[0] != nil {
		return errs.New(errs.KindStateConflict, "completed release group deletion retained its target")
	}
	if terminal != taskjournal.TaskStatusCompleted && stored.Values[0] == nil {
		return errs.New(errs.KindStateConflict, "failed release group deletion lost its target")
	}
	return nil
}

func taskOwnsReleaseGroupRemoval(task TaskRecord) (bool, error) {
	if task.Executor != taskjournal.TaskExecutorController ||
		task.Params[taskjournal.TaskResourceKindParam] != taskjournal.TaskResourceReleaseGroup {
		return false, nil
	}
	if task.Type != taskjournal.TaskRemove || len(task.Params) != 1 ||
		ids.Validate(ids.KindReleaseGroup, task.Target) != nil {
		return false, errs.New(errs.KindInternal, "release group deletion task has invalid durable input")
	}
	return true, nil
}

func clearReleaseGroupTaskChange(change releaseGroupTaskChange) {
	for _, value := range change.values {
		clear(value)
	}
}
