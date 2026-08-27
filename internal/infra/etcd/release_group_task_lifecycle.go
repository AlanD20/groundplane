package etcd

import (
	"context"

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
	conditions []Condition
	mutations  []Mutation
	values     [][]byte
}

func releaseGroupRecordKey(id string) string { return releaseGroupRecordPrefix + id }
func releaseGroupOwnerKey(environmentID, id string) string {
	return releaseGroupOwnerPrefix + environmentID + "/" + id
}
func releaseGroupNameKey(environmentID, name string) string {
	return releaseGroupNamePrefix + environmentID + "/" + encodeDynamicSegment(name)
}

func decodeReleaseGroupStored(value []byte) (domain.Group, error) {
	record, err := decodeEnvelope[releaseGroupStoredRecord](value, "release_group")
	if err != nil {
		return domain.Group{}, err
	}
	group, err := domain.New(domain.Input{
		ID: record.ID, EnvironmentID: record.EnvironmentID, Name: record.Name,
		ServiceIDs: record.ServiceIDs, Order: record.Order, DefaultTag: record.DefaultTag, OnFailure: record.OnFailure,
	})
	if err != nil {
		return domain.Group{}, corruptRecord()
	}
	return group, nil
}

func (repository *TaskRepository) prepareReleaseGroupTaskRetry(ctx context.Context, source, retry TaskRecord, revision int64) (releaseGroupTaskChange, error) {
	applies, err := taskOwnsReleaseGroupRemoval(source)
	if err != nil || !applies {
		return releaseGroupTaskChange{}, err
	}
	if retry.Type != TaskRemove || retry.Target != source.Target || retry.Params[TaskResourceKindParam] != TaskResourceReleaseGroup {
		return releaseGroupTaskChange{}, errs.New(errs.KindInternal, "release group retry changed its durable target")
	}
	stored, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{
		releaseGroupRecordKey(source.Target), deletionTombstoneKey(string(DeletionTargetReleaseGroup), source.Target),
	}, Revision: revision})
	if err != nil {
		return releaseGroupTaskChange{}, err
	}
	if stored == nil || len(stored.Values) != 2 || stored.Values[0] == nil || stored.Values[1] != nil {
		return releaseGroupTaskChange{}, errs.New(errs.KindStateConflict, "release group is not available for deletion retry")
	}
	group, err := decodeReleaseGroupStored(stored.Values[0].Value)
	if err != nil || group.ID != source.Target {
		return releaseGroupTaskChange{}, corruptRecord()
	}
	indexes, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{
		releaseGroupOwnerKey(group.EnvironmentID, group.ID), releaseGroupNameKey(group.EnvironmentID, group.Name),
		environmentKey(group.EnvironmentID), environmentMutationEpochKey(group.EnvironmentID),
		deletionTombstoneKey(string(DeletionTargetEnvironment), group.EnvironmentID),
		deletionTombstoneKey(string(DeletionTargetProject), source.Owner.ProjectID),
		deletionTombstoneKey(string(DeletionTargetTenant), source.Owner.TenantID),
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
	epoch, err := decodeEnvironmentMutationEpochRecord(indexes.Values[3].Value)
	if err != nil || epoch.EnvironmentID != group.EnvironmentID {
		return releaseGroupTaskChange{}, corruptRecord()
	}
	tombstone := DeletionTombstoneRecord{
		TargetKind: DeletionTargetReleaseGroup, TargetID: group.ID, TargetRevision: stored.Values[0].ModRevision,
		TaskID: retry.ID, Phase: DeletionPhaseFinalizing, CreatedAt: retry.CreatedAt, UpdatedAt: retry.CreatedAt,
	}
	value, err := encodeDeletionTombstone(tombstone)
	if err != nil {
		return releaseGroupTaskChange{}, err
	}
	return releaseGroupTaskChange{
		applies: true,
		conditions: []Condition{
			{Key: releaseGroupRecordKey(group.ID), ModRevision: stored.Values[0].ModRevision},
			{Key: releaseGroupOwnerKey(group.EnvironmentID, group.ID), ModRevision: indexes.Values[0].ModRevision},
			{Key: releaseGroupNameKey(group.EnvironmentID, group.Name), ModRevision: indexes.Values[1].ModRevision},
			{Key: environmentKey(group.EnvironmentID), ModRevision: indexes.Values[2].ModRevision},
			{Key: environmentMutationEpochKey(group.EnvironmentID), ModRevision: indexes.Values[3].ModRevision},
			{Key: deletionTombstoneKey(string(DeletionTargetEnvironment), group.EnvironmentID)},
			{Key: deletionTombstoneKey(string(DeletionTargetProject), source.Owner.ProjectID)},
			{Key: deletionTombstoneKey(string(DeletionTargetTenant), source.Owner.TenantID)},
			{Key: deletionTombstoneKey(string(DeletionTargetReleaseGroup), group.ID)},
		},
		mutations: []Mutation{{Type: MutationPut, Key: deletionTombstoneKey(string(DeletionTargetReleaseGroup), group.ID), Value: value}},
		values:    [][]byte{value},
	}, nil
}

func (repository *TaskRepository) prepareReleaseGroupTaskAcknowledgement(ctx context.Context, task TaskRecord, terminal TaskStatus, revision int64) (releaseGroupTaskChange, error) {
	applies, err := taskOwnsReleaseGroupRemoval(task)
	if err != nil || !applies {
		return releaseGroupTaskChange{}, err
	}
	stored, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{
		releaseGroupRecordKey(task.Target), deletionTombstoneKey(string(DeletionTargetReleaseGroup), task.Target),
	}, Revision: revision})
	if err != nil {
		return releaseGroupTaskChange{}, err
	}
	if stored == nil || len(stored.Values) != 2 || stored.Values[0] == nil || stored.Values[1] == nil {
		return releaseGroupTaskChange{}, errs.New(errs.KindInternal, "release group deletion state is inconsistent")
	}
	group, err := decodeReleaseGroupStored(stored.Values[0].Value)
	if err != nil || group.ID != task.Target {
		return releaseGroupTaskChange{}, corruptRecord()
	}
	tombstone, err := decodeDeletionTombstone(stored.Values[1].Value)
	if err != nil || tombstone.TargetKind != DeletionTargetReleaseGroup || tombstone.TargetID != task.Target ||
		tombstone.TargetRevision != stored.Values[0].ModRevision || tombstone.TaskID != task.ID || tombstone.Phase != DeletionPhaseFinalizing {
		return releaseGroupTaskChange{}, errs.New(errs.KindStateConflict, "release group deletion tombstone does not match its task")
	}
	indexes, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{
		releaseGroupOwnerKey(group.EnvironmentID, group.ID), releaseGroupNameKey(group.EnvironmentID, group.Name),
		environmentMutationEpochKey(group.EnvironmentID), environmentKey(group.EnvironmentID),
		deletionTombstoneKey(string(DeletionTargetEnvironment), group.EnvironmentID),
		deletionTombstoneKey(string(DeletionTargetProject), task.Owner.ProjectID),
		deletionTombstoneKey(string(DeletionTargetTenant), task.Owner.TenantID),
	}, Revision: revision})
	if err != nil {
		return releaseGroupTaskChange{}, err
	}
	if indexes == nil || len(indexes.Values) != 7 || indexes.Values[0] == nil || indexes.Values[1] == nil || indexes.Values[2] == nil ||
		indexes.Values[3] == nil || indexes.Values[4] != nil || indexes.Values[5] != nil || indexes.Values[6] != nil ||
		task.Owner.EnvironmentID != group.EnvironmentID ||
		string(indexes.Values[0].Value) != group.ID || string(indexes.Values[1].Value) != group.ID {
		return releaseGroupTaskChange{}, errs.New(errs.KindInternal, "release group deletion indexes are corrupt")
	}
	epoch, err := decodeEnvironmentMutationEpochRecord(indexes.Values[2].Value)
	if err != nil || epoch.EnvironmentID != group.EnvironmentID {
		return releaseGroupTaskChange{}, corruptRecord()
	}
	epochValue, err := encodeEnvironmentMutationEpochRecord(epoch)
	if err != nil {
		return releaseGroupTaskChange{}, err
	}
	change := releaseGroupTaskChange{
		applies: true,
		conditions: []Condition{
			{Key: releaseGroupRecordKey(group.ID), ModRevision: stored.Values[0].ModRevision},
			{Key: deletionTombstoneKey(string(DeletionTargetReleaseGroup), group.ID), ModRevision: stored.Values[1].ModRevision},
			{Key: releaseGroupOwnerKey(group.EnvironmentID, group.ID), ModRevision: indexes.Values[0].ModRevision},
			{Key: releaseGroupNameKey(group.EnvironmentID, group.Name), ModRevision: indexes.Values[1].ModRevision},
			{Key: environmentKey(group.EnvironmentID), ModRevision: indexes.Values[3].ModRevision},
			{Key: deletionTombstoneKey(string(DeletionTargetEnvironment), group.EnvironmentID)},
			{Key: deletionTombstoneKey(string(DeletionTargetProject), task.Owner.ProjectID)},
			{Key: deletionTombstoneKey(string(DeletionTargetTenant), task.Owner.TenantID)},
		},
		mutations: []Mutation{{Type: MutationDelete, Key: deletionTombstoneKey(string(DeletionTargetReleaseGroup), group.ID)}},
	}
	if terminal == TaskStatusCompleted {
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
			Condition{Key: environmentMutationEpochKey(group.EnvironmentID), ModRevision: indexes.Values[2].ModRevision},
			collectionCondition,
		)
		change.mutations = append(change.mutations,
			Mutation{Type: MutationDelete, Key: releaseGroupOwnerKey(group.EnvironmentID, group.ID)},
			Mutation{Type: MutationDelete, Key: releaseGroupNameKey(group.EnvironmentID, group.Name)},
			Mutation{Type: MutationDelete, Key: releaseGroupRecordKey(group.ID)},
			Mutation{Type: MutationPut, Key: environmentMutationEpochKey(group.EnvironmentID), Value: epochValue},
			collectionMutation,
		)
		change.values = append(change.values, epochValue, collectionMutation.Value)
	} else {
		clear(epochValue)
	}
	return change, nil
}

func (repository *TaskRepository) validateReleaseGroupTaskAcknowledgementReplay(ctx context.Context, task TaskRecord, terminal TaskStatus, revision int64) error {
	applies, err := taskOwnsReleaseGroupRemoval(task)
	if err != nil || !applies {
		return err
	}
	stored, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{
		releaseGroupRecordKey(task.Target), deletionTombstoneKey(string(DeletionTargetReleaseGroup), task.Target),
	}, Revision: revision})
	if err != nil {
		return err
	}
	if stored == nil || len(stored.Values) != 2 || stored.Values[1] != nil {
		return errs.New(errs.KindStateConflict, "release group deletion terminal state does not match its task")
	}
	if terminal == TaskStatusCompleted && stored.Values[0] != nil {
		return errs.New(errs.KindStateConflict, "completed release group deletion retained its target")
	}
	if terminal != TaskStatusCompleted && stored.Values[0] == nil {
		return errs.New(errs.KindStateConflict, "failed release group deletion lost its target")
	}
	return nil
}

func taskOwnsReleaseGroupRemoval(task TaskRecord) (bool, error) {
	if task.Executor != TaskExecutorController || task.Params[TaskResourceKindParam] != TaskResourceReleaseGroup {
		return false, nil
	}
	if task.Type != TaskRemove || len(task.Params) != 1 || ids.Validate(ids.KindReleaseGroup, task.Target) != nil {
		return false, errs.New(errs.KindInternal, "release group deletion task has invalid durable input")
	}
	return true, nil
}

func clearReleaseGroupTaskChange(change releaseGroupTaskChange) {
	for _, value := range change.values {
		clear(value)
	}
}
