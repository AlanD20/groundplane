package etcd

import (
	"context"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/releasegroup"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	releaseGroupRecordPrefix = "/v1/records/release-groups/"
	releaseGroupOwnerPrefix  = "/v1/indexes/release-groups/by-owner/environment/"
	releaseGroupNamePrefix   = "/v1/indexes/release-groups/by-name/environment/"
)

type releaseGroupStoredRecord struct {
	ID            string           `json:"id"`
	EnvironmentID string           `json:"environment_id"`
	Name          string           `json:"name"`
	ServiceIDs    []string         `json:"service_ids"`
	Order         []string         `json:"order"`
	DefaultTag    string           `json:"default_tag,omitempty"`
	OnFailure     domain.OnFailure `json:"on_failure"`
}

type releaseGroupTaskChange struct {
	applies    bool
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
	values     [][]byte
}

func releaseGroupRecordKey(id string) string { return releaseGroupRecordPrefix + id }
func releaseGroupOwnerKey(environmentID, id string) string {
	return releaseGroupOwnerPrefix + environmentID + "/" + id
}
func releaseGroupNameKey(environmentID, name string) string {
	return releaseGroupNamePrefix + environmentID + "/" + recordcodec.EncodeKeySegment(name)
}

func decodeReleaseGroupStored(value []byte) (domain.Group, error) {
	record, err := recordcodec.Decode[releaseGroupStoredRecord](value, "release_group")
	if err != nil {
		return domain.Group{}, err
	}
	group, err := domain.New(domain.Input{
		ID: record.ID, EnvironmentID: record.EnvironmentID, Name: record.Name,
		ServiceIDs: record.ServiceIDs, Order: record.Order, DefaultTag: record.DefaultTag, OnFailure: record.OnFailure,
	})
	if err != nil {
		return domain.Group{}, recordcodec.CorruptRecord()
	}
	return group, nil
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
		retry.Params[TaskResourceKindParam] != TaskResourceReleaseGroup {
		return releaseGroupTaskChange{}, errs.New(errs.KindInternal, "release group retry changed its durable target")
	}
	stored, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		releaseGroupRecordKey(source.Target), deletionTombstoneKey(string(deletionrecord.DeletionTargetReleaseGroup), source.Target),
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
	group, err := decodeReleaseGroupStored(stored.Values[0].Value)
	if err != nil || group.ID != source.Target {
		return releaseGroupTaskChange{}, recordcodec.CorruptRecord()
	}
	indexes, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		releaseGroupOwnerKey(group.EnvironmentID, group.ID), releaseGroupNameKey(group.EnvironmentID, group.Name),
		hierarchyrecord.EnvironmentKey(group.EnvironmentID), hierarchyrecord.EnvironmentMutationEpochKey(group.EnvironmentID),
		deletionTombstoneKey(string(deletionrecord.DeletionTargetEnvironment), group.EnvironmentID),
		deletionTombstoneKey(string(deletionrecord.DeletionTargetProject), source.Owner.ProjectID),
		deletionTombstoneKey(string(deletionrecord.DeletionTargetTenant), source.Owner.TenantID),
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
			{Key: releaseGroupRecordKey(group.ID), ModRevision: stored.Values[0].ModRevision},
			{Key: releaseGroupOwnerKey(group.EnvironmentID, group.ID), ModRevision: indexes.Values[0].ModRevision},
			{Key: releaseGroupNameKey(group.EnvironmentID, group.Name), ModRevision: indexes.Values[1].ModRevision},
			{Key: hierarchyrecord.EnvironmentKey(group.EnvironmentID), ModRevision: indexes.Values[2].ModRevision},
			{Key: hierarchyrecord.EnvironmentMutationEpochKey(group.EnvironmentID), ModRevision: indexes.Values[3].ModRevision},
			{Key: deletionTombstoneKey(string(deletionrecord.DeletionTargetEnvironment), group.EnvironmentID)},
			{Key: deletionTombstoneKey(string(deletionrecord.DeletionTargetProject), source.Owner.ProjectID)},
			{Key: deletionTombstoneKey(string(deletionrecord.DeletionTargetTenant), source.Owner.TenantID)},
			{Key: deletionTombstoneKey(string(deletionrecord.DeletionTargetReleaseGroup), group.ID)},
		},
		mutations: []etcdstore.Mutation{
			{Type: etcdstore.MutationPut, Key: deletionTombstoneKey(string(deletionrecord.DeletionTargetReleaseGroup), group.ID), Value: value},
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
		releaseGroupRecordKey(task.Target), deletionTombstoneKey(string(deletionrecord.DeletionTargetReleaseGroup), task.Target),
	}, Revision: revision})
	if err != nil {
		return releaseGroupTaskChange{}, err
	}
	if stored == nil || len(stored.Values) != 2 || stored.Values[0] == nil || stored.Values[1] == nil {
		return releaseGroupTaskChange{}, errs.New(errs.KindInternal, "release group deletion state is inconsistent")
	}
	group, err := decodeReleaseGroupStored(stored.Values[0].Value)
	if err != nil || group.ID != task.Target {
		return releaseGroupTaskChange{}, recordcodec.CorruptRecord()
	}
	tombstone, err := deletionrecord.DecodeDeletionTombstone(stored.Values[1].Value)
	if err != nil || tombstone.TargetKind != deletionrecord.DeletionTargetReleaseGroup || tombstone.TargetID != task.Target ||
		tombstone.TargetRevision != stored.Values[0].ModRevision || tombstone.TaskID != task.ID || tombstone.Phase != deletionrecord.DeletionPhaseFinalizing {
		return releaseGroupTaskChange{}, errs.New(
			errs.KindStateConflict,
			"release group deletion tombstone does not match its task",
		)
	}
	indexes, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		releaseGroupOwnerKey(group.EnvironmentID, group.ID), releaseGroupNameKey(group.EnvironmentID, group.Name),
		hierarchyrecord.EnvironmentMutationEpochKey(group.EnvironmentID), hierarchyrecord.EnvironmentKey(group.EnvironmentID),
		deletionTombstoneKey(string(deletionrecord.DeletionTargetEnvironment), group.EnvironmentID),
		deletionTombstoneKey(string(deletionrecord.DeletionTargetProject), task.Owner.ProjectID),
		deletionTombstoneKey(string(deletionrecord.DeletionTargetTenant), task.Owner.TenantID),
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
			{Key: releaseGroupRecordKey(group.ID), ModRevision: stored.Values[0].ModRevision},
			{
				Key:         deletionTombstoneKey(string(deletionrecord.DeletionTargetReleaseGroup), group.ID),
				ModRevision: stored.Values[1].ModRevision,
			},
			{Key: releaseGroupOwnerKey(group.EnvironmentID, group.ID), ModRevision: indexes.Values[0].ModRevision},
			{Key: releaseGroupNameKey(group.EnvironmentID, group.Name), ModRevision: indexes.Values[1].ModRevision},
			{Key: hierarchyrecord.EnvironmentKey(group.EnvironmentID), ModRevision: indexes.Values[3].ModRevision},
			{Key: deletionTombstoneKey(string(deletionrecord.DeletionTargetEnvironment), group.EnvironmentID)},
			{Key: deletionTombstoneKey(string(deletionrecord.DeletionTargetProject), task.Owner.ProjectID)},
			{Key: deletionTombstoneKey(string(deletionrecord.DeletionTargetTenant), task.Owner.TenantID)},
		},
		mutations: []etcdstore.Mutation{
			{Type: etcdstore.MutationDelete, Key: deletionTombstoneKey(string(deletionrecord.DeletionTargetReleaseGroup), group.ID)},
		},
	}
	if terminal == taskjournal.TaskStatusCompleted {
		collectionCondition, collectionMutation, collectionErr := loadReleaseGroupCollectionEpoch(
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
		change.mutations = append(change.mutations,
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: releaseGroupOwnerKey(group.EnvironmentID, group.ID)},
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: releaseGroupNameKey(group.EnvironmentID, group.Name)},
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: releaseGroupRecordKey(group.ID)},
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: hierarchyrecord.EnvironmentMutationEpochKey(group.EnvironmentID), Value: epochValue},
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
		releaseGroupRecordKey(task.Target), deletionTombstoneKey(string(deletionrecord.DeletionTargetReleaseGroup), task.Target),
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
	if task.Executor != taskjournal.TaskExecutorController || task.Params[TaskResourceKindParam] != TaskResourceReleaseGroup {
		return false, nil
	}
	if task.Type != taskjournal.TaskRemove || len(task.Params) != 1 || ids.Validate(ids.KindReleaseGroup, task.Target) != nil {
		return false, errs.New(errs.KindInternal, "release group deletion task has invalid durable input")
	}
	return true, nil
}

func clearReleaseGroupTaskChange(change releaseGroupTaskChange) {
	for _, value := range change.values {
		clear(value)
	}
}
