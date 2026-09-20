package etcd

import (
	"bytes"
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"

	domain "github.com/AlanD20/groundplane/internal/core/releasegroup"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type releaseGroupMutationEvidence struct {
	conditions      []etcdstore.Condition
	epochMutation   etcdstore.Mutation
	collectionEpoch etcdstore.Mutation
}

func (repository *TaskRepository) PrepareReleaseGroupCreate(
	ctx context.Context,
	group domain.Group,
) (ReleaseGroupPreparedMutation, error) {
	if err := domain.Validate(group); err != nil {
		return ReleaseGroupPreparedMutation{}, err
	}
	evidence, err := repository.prepareReleaseGroupMutationEvidence(ctx, group, "", 0)
	if err != nil {
		return ReleaseGroupPreparedMutation{}, err
	}
	value, err := encodeReleaseGroup(group)
	if err != nil {
		return ReleaseGroupPreparedMutation{}, err
	}
	return newReleaseGroupPreparedMutation(
		group.EnvironmentID, group.ID, 0, TaskCreate, evidence.conditions,
		[]etcdstore.Mutation{
			{Type: etcdstore.MutationPut, Key: releaseGroupRecordKey(group.ID), Value: value},
			{Type: etcdstore.MutationPut, Key: releaseGroupOwnerKey(group.EnvironmentID, group.ID), Value: []byte(group.ID)},
			{Type: etcdstore.MutationPut, Key: releaseGroupNameKey(group.EnvironmentID, group.Name), Value: []byte(group.ID)},
			evidence.epochMutation,
			evidence.collectionEpoch,
		},
	), nil
}

func (repository *TaskRepository) PrepareReleaseGroupUpdate(
	ctx context.Context,
	current domain.Group,
	currentRevision int64,
	replacement domain.Group,
) (ReleaseGroupPreparedMutation, error) {
	if err := domain.Validate(current); err != nil {
		return ReleaseGroupPreparedMutation{}, err
	}
	if err := domain.Validate(replacement); err != nil {
		return ReleaseGroupPreparedMutation{}, err
	}
	if current.ID != replacement.ID || current.EnvironmentID != replacement.EnvironmentID ||
		currentRevision <= 0 {
		return ReleaseGroupPreparedMutation{}, errs.New(
			errs.KindValidationFailed,
			"release group update changed stable identity, ownership, or revision",
		)
	}
	evidence, err := repository.prepareReleaseGroupMutationEvidence(
		ctx, replacement, current.Name, currentRevision,
	)
	if err != nil {
		return ReleaseGroupPreparedMutation{}, err
	}
	value, err := encodeReleaseGroup(replacement)
	if err != nil {
		return ReleaseGroupPreparedMutation{}, err
	}
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: releaseGroupRecordKey(replacement.ID), Value: value},
		evidence.epochMutation,
		evidence.collectionEpoch,
	}
	if current.Name != replacement.Name {
		mutations = append(
			mutations,
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: releaseGroupNameKey(replacement.EnvironmentID, current.Name)},
			etcdstore.Mutation{
				Type:  etcdstore.MutationPut,
				Key:   releaseGroupNameKey(replacement.EnvironmentID, replacement.Name),
				Value: []byte(replacement.ID),
			},
		)
	}
	return newReleaseGroupPreparedMutation(
		replacement.EnvironmentID, replacement.ID, currentRevision, TaskUpdate,
		evidence.conditions, mutations,
	), nil
}

func (repository *TaskRepository) PrepareReleaseGroupRemove(
	ctx context.Context,
	current domain.Group,
	currentRevision int64,
) (ReleaseGroupPreparedMutation, error) {
	if err := domain.Validate(current); err != nil {
		return ReleaseGroupPreparedMutation{}, err
	}
	if currentRevision <= 0 {
		return ReleaseGroupPreparedMutation{}, errs.New(
			errs.KindValidationFailed,
			"release group removal revision is invalid",
		)
	}
	evidence, err := repository.prepareReleaseGroupMutationEvidence(
		ctx, current, current.Name, currentRevision,
	)
	if err != nil {
		return ReleaseGroupPreparedMutation{}, err
	}
	return newReleaseGroupPreparedMutation(
		current.EnvironmentID, current.ID, currentRevision, TaskRemove,
		evidence.conditions,
		[]etcdstore.Mutation{evidence.epochMutation, evidence.collectionEpoch},
	), nil
}

func encodeReleaseGroup(group domain.Group) ([]byte, error) {
	return encodeEnvelope("release_group", releaseGroupStoredRecord{
		ID: group.ID, EnvironmentID: group.EnvironmentID, Name: group.Name,
		ServiceIDs: group.ServiceIDs, Order: group.Order,
		DefaultTag: group.DefaultTag, OnFailure: group.OnFailure,
	})
}

func (repository *TaskRepository) prepareReleaseGroupMutationEvidence(
	ctx context.Context,
	group domain.Group,
	oldName string,
	revision int64,
) (releaseGroupMutationEvidence, error) {
	if err := validateContext(ctx); err != nil {
		return releaseGroupMutationEvidence{}, err
	}
	baseKeys := []string{
		environmentKey(group.EnvironmentID),
		environmentMutationEpochKey(group.EnvironmentID),
		environmentOperationLockKey(group.EnvironmentID),
		deletionTombstoneKey(string(DeletionTargetEnvironment), group.EnvironmentID),
		releaseGroupRecordKey(group.ID),
		releaseGroupOwnerKey(group.EnvironmentID, group.ID),
		releaseGroupNameKey(group.EnvironmentID, group.Name),
		environmentComposeProjectionKey(group.EnvironmentID),
		deletionTombstoneKey(string(DeletionTargetReleaseGroup), group.ID),
	}
	if oldName != "" && oldName != group.Name {
		baseKeys = append(baseKeys, releaseGroupNameKey(group.EnvironmentID, oldName))
	}
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: baseKeys})
	if err != nil {
		return releaseGroupMutationEvidence{}, err
	}
	if result == nil || len(result.Values) != len(baseKeys) || result.Values[0] == nil {
		return releaseGroupMutationEvidence{}, errs.New(
			errs.KindEnvironmentNotFound,
			"release group environment was not found",
		)
	}
	environment, err := decodeEnvironment(result.Values[0].Value)
	if err != nil || environment.ID != group.EnvironmentID || result.Values[1] == nil {
		return releaseGroupMutationEvidence{}, corruptRecord()
	}
	epoch, err := decodeEnvironmentMutationEpochRecord(result.Values[1].Value)
	if err != nil || epoch.EnvironmentID != group.EnvironmentID {
		return releaseGroupMutationEvidence{}, corruptRecord()
	}
	epochValue, err := encodeEnvironmentMutationEpochRecord(epoch)
	if err != nil {
		return releaseGroupMutationEvidence{}, err
	}
	if result.Values[2] != nil || result.Values[3] != nil {
		clear(epochValue)
		return releaseGroupMutationEvidence{}, errs.New(
			errs.KindResourceInUse,
			"release group environment has an active operation or deletion",
		)
	}
	if result.Values[7] == nil {
		clear(epochValue)
		return releaseGroupMutationEvidence{}, errs.New(
			errs.KindResourceInUse,
			"release group environment has no enabled compose project",
		)
	}
	projection, err := decodeEnvironmentComposeProjection(result.Values[7].Value)
	if err != nil || projection.EnvironmentID != group.EnvironmentID {
		clear(epochValue)
		return releaseGroupMutationEvidence{}, corruptRecord()
	}
	if result.Values[8] != nil {
		clear(epochValue)
		return releaseGroupMutationEvidence{}, errs.New(
			errs.KindResourceInUse,
			"release group deletion is already in progress",
		)
	}
	collectionCondition, collectionMutation, err := loadReleaseGroupCollectionEpoch(
		ctx, repository.store, group.EnvironmentID, 0,
	)
	if err != nil {
		clear(epochValue)
		return releaseGroupMutationEvidence{}, err
	}
	conditions := []etcdstore.Condition{
		{Key: baseKeys[0], ModRevision: result.Values[0].ModRevision},
		{Key: baseKeys[1], ModRevision: result.Values[1].ModRevision},
		{Key: baseKeys[2]},
		{Key: baseKeys[3]},
		{Key: baseKeys[7], ModRevision: result.Values[7].ModRevision},
		{Key: baseKeys[8]},
		collectionCondition,
	}
	if revision == 0 {
		if result.Values[4] != nil || result.Values[5] != nil {
			return releaseGroupMutationEvidence{}, errs.New(
				errs.KindStateConflict,
				"release group stable id already exists",
			)
		}
		if result.Values[6] != nil {
			return releaseGroupMutationEvidence{}, errs.New(
				errs.KindNameConflict,
				"release group name already exists in the environment",
			)
		}
		conditions = append(conditions,
			etcdstore.Condition{Key: baseKeys[4]},
			etcdstore.Condition{Key: baseKeys[5]},
			etcdstore.Condition{Key: baseKeys[6]},
		)
	} else {
		if result.Values[4] == nil || result.Values[4].ModRevision != revision ||
			result.Values[5] == nil {
			return releaseGroupMutationEvidence{}, errs.New(
				errs.KindStateConflict,
				"release group changed before mutation",
			)
		}
		stored, decodeErr := decodeReleaseGroupStored(result.Values[4].Value)
		if decodeErr != nil || stored.ID != group.ID ||
			stored.EnvironmentID != group.EnvironmentID || stored.Name != oldName ||
			!bytes.Equal(result.Values[5].Value, []byte(group.ID)) {
			return releaseGroupMutationEvidence{}, corruptRecord()
		}
		conditions = append(conditions,
			etcdstore.Condition{Key: baseKeys[4], ModRevision: revision},
			etcdstore.Condition{Key: baseKeys[5], ModRevision: result.Values[5].ModRevision},
		)
		if oldName != group.Name {
			if result.Values[6] != nil {
				return releaseGroupMutationEvidence{}, errs.New(
					errs.KindNameConflict,
					"release group name already exists in the environment",
				)
			}
			oldNameIndex := result.Values[9]
			if oldNameIndex == nil || !bytes.Equal(oldNameIndex.Value, []byte(group.ID)) {
				return releaseGroupMutationEvidence{}, corruptRecord()
			}
			conditions = append(conditions,
				etcdstore.Condition{Key: baseKeys[6]},
				etcdstore.Condition{Key: baseKeys[9], ModRevision: oldNameIndex.ModRevision},
			)
		} else {
			if result.Values[6] == nil || !bytes.Equal(result.Values[6].Value, []byte(group.ID)) {
				return releaseGroupMutationEvidence{}, corruptRecord()
			}
			conditions = append(conditions,
				etcdstore.Condition{Key: baseKeys[6], ModRevision: result.Values[6].ModRevision},
			)
		}
	}
	projectConditions, err := repository.prepareReleaseGroupProjectEvidence(ctx, environment)
	if err != nil {
		return releaseGroupMutationEvidence{}, err
	}
	memberConditions, err := repository.prepareReleaseGroupMemberEvidence(ctx, group, projection)
	if err != nil {
		return releaseGroupMutationEvidence{}, err
	}
	conditions = append(conditions, projectConditions...)
	conditions = append(conditions, memberConditions...)
	return releaseGroupMutationEvidence{
		conditions: conditions,
		epochMutation: etcdstore.Mutation{
			Type:  etcdstore.MutationPut,
			Key:   environmentMutationEpochKey(group.EnvironmentID),
			Value: epochValue,
		},
		collectionEpoch: collectionMutation,
	}, nil
}
