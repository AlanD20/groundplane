package etcd

import (
	"bytes"
	"context"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	recordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"sort"

	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/releasegroup"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ReleaseGroupSnapshotEntry is semantic fixed-revision evidence. The adapter
// re-reads every key before it mints an opaque publication fragment.
type ReleaseGroupSnapshotEntry struct {
	Group        domain.Group
	Revision     int64
	ReadRevision int64
}

func (repository *HierarchyRepository) PrepareReleaseGroupBlueprintMutation(
	ctx context.Context,
	environmentID string,
	readRevision int64,
	current []ReleaseGroupSnapshotEntry,
	desired []domain.Group,
) (ReleaseGroupBlueprintPreparedMutation, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return ReleaseGroupBlueprintPreparedMutation{}, err
	}
	if ids.Validate(ids.KindEnvironment, environmentID) != nil || readRevision <= 0 {
		return ReleaseGroupBlueprintPreparedMutation{}, errs.New(
			errs.KindValidationFailed,
			"Blueprint Release Group snapshot is invalid",
		)
	}
	collectionCondition, collectionMutation, err := loadReleaseGroupCollectionEpoch(
		ctx, repository.store, environmentID, readRevision,
	)
	if err != nil {
		return ReleaseGroupBlueprintPreparedMutation{}, err
	}
	currentByID := make(map[string]ReleaseGroupSnapshotEntry, len(current))
	currentNames := make(map[string]string, len(current))
	for _, versioned := range current {
		if err := domain.Validate(versioned.Group); err != nil ||
			versioned.Group.EnvironmentID != environmentID ||
			versioned.Revision <= 0 || versioned.ReadRevision != readRevision ||
			versioned.Revision > readRevision {
			return ReleaseGroupBlueprintPreparedMutation{}, errs.New(
				errs.KindInternal,
				"Blueprint Release Group snapshot is inconsistent",
			)
		}
		if _, duplicate := currentByID[versioned.Group.ID]; duplicate {
			return ReleaseGroupBlueprintPreparedMutation{}, errs.New(
				errs.KindInternal,
				"Blueprint Release Group snapshot repeats an id",
			)
		}
		if _, duplicate := currentNames[versioned.Group.Name]; duplicate {
			return ReleaseGroupBlueprintPreparedMutation{}, errs.New(
				errs.KindInternal,
				"Blueprint Release Group snapshot repeats a name",
			)
		}
		currentByID[versioned.Group.ID] = versioned
		currentNames[versioned.Group.Name] = versioned.Group.ID
	}
	desiredByID := make(map[string]domain.Group, len(desired))
	desiredNames := make(map[string]string, len(desired))
	for _, group := range desired {
		if err := domain.Validate(group); err != nil || group.EnvironmentID != environmentID {
			return ReleaseGroupBlueprintPreparedMutation{}, errs.New(
				errs.KindValidationFailed,
				"Blueprint Release Group desired state is invalid",
			)
		}
		if _, duplicate := desiredByID[group.ID]; duplicate {
			return ReleaseGroupBlueprintPreparedMutation{}, errs.New(
				errs.KindInternal,
				"Blueprint Release Group desired state repeats an id",
			)
		}
		if _, duplicate := desiredNames[group.Name]; duplicate {
			return ReleaseGroupBlueprintPreparedMutation{}, errs.New(
				errs.KindNameConflict,
				"Blueprint Release Group desired state repeats a name",
			)
		}
		desiredByID[group.ID] = group
		desiredNames[group.Name] = group.ID
	}
	currentIDs := sortedReleaseGroupIDs(currentByID)
	desiredIDs := sortedReleaseGroupIDs(desiredByID)
	conditions := []etcdstore.Condition{collectionCondition}
	mutations := []etcdstore.Mutation{collectionMutation}
	for _, id := range currentIDs {
		versioned := currentByID[id]
		indexes, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
			Keys: []string{
				releaseGroupOwnerKey(environmentID, id),
				releaseGroupNameKey(environmentID, versioned.Group.Name),
				deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetReleaseGroup), id),
			},
			Revision: readRevision,
		})
		if err != nil {
			return ReleaseGroupBlueprintPreparedMutation{}, err
		}
		if indexes == nil || indexes.ReadRevision != readRevision ||
			len(indexes.Values) != 3 || indexes.Values[0] == nil ||
			indexes.Values[1] == nil || indexes.Values[2] != nil ||
			!bytes.Equal(indexes.Values[0].Value, []byte(id)) ||
			!bytes.Equal(indexes.Values[1].Value, []byte(id)) {
			return ReleaseGroupBlueprintPreparedMutation{}, recordcodec.CorruptRecord()
		}
		conditions = append(
			conditions,
			etcdstore.Condition{Key: releaseGroupRecordKey(id), ModRevision: versioned.Revision},
			etcdstore.Condition{Key: releaseGroupOwnerKey(environmentID, id), ModRevision: indexes.Values[0].ModRevision},
			etcdstore.Condition{
				Key:         releaseGroupNameKey(environmentID, versioned.Group.Name),
				ModRevision: indexes.Values[1].ModRevision,
			},
			etcdstore.Condition{Key: deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetReleaseGroup), id)},
		)
		next, retained := desiredByID[id]
		if retained {
			if next.Name != versioned.Group.Name {
				return ReleaseGroupBlueprintPreparedMutation{}, errs.New(
					errs.KindStateConflict,
					"Blueprint Release Group identity changed names",
				)
			}
			if !domain.Equal(versioned.Group, next) {
				value, err := encodeReleaseGroup(next)
				if err != nil {
					return ReleaseGroupBlueprintPreparedMutation{}, err
				}
				mutations = append(mutations, etcdstore.Mutation{
					Type:  etcdstore.MutationPut,
					Key:   releaseGroupRecordKey(id),
					Value: value,
				})
			}
			continue
		}
		mutations = append(mutations,
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: releaseGroupRecordKey(id)},
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: releaseGroupOwnerKey(environmentID, id)},
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: releaseGroupNameKey(environmentID, versioned.Group.Name)},
		)
	}
	for _, id := range desiredIDs {
		if _, retained := currentByID[id]; retained {
			continue
		}
		group := desiredByID[id]
		keys := []string{
			releaseGroupRecordKey(id),
			releaseGroupOwnerKey(environmentID, id),
			releaseGroupNameKey(environmentID, group.Name),
			deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetReleaseGroup), id),
		}
		absent, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
			Keys:     keys,
			Revision: readRevision,
		})
		if err != nil {
			return ReleaseGroupBlueprintPreparedMutation{}, err
		}
		if absent == nil || absent.ReadRevision != readRevision ||
			len(absent.Values) != len(keys) {
			return ReleaseGroupBlueprintPreparedMutation{}, recordcodec.CorruptRecord()
		}
		for _, value := range absent.Values {
			if value != nil {
				return ReleaseGroupBlueprintPreparedMutation{}, errs.New(
					errs.KindStateConflict,
					"Blueprint Release Group creation raced",
				)
			}
		}
		value, err := encodeReleaseGroup(group)
		if err != nil {
			return ReleaseGroupBlueprintPreparedMutation{}, err
		}
		for _, key := range keys {
			conditions = append(conditions, etcdstore.Condition{Key: key})
		}
		mutations = append(mutations,
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: releaseGroupRecordKey(id), Value: value},
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: releaseGroupOwnerKey(environmentID, id), Value: []byte(id)},
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: releaseGroupNameKey(environmentID, group.Name), Value: []byte(id)},
		)
	}
	return newReleaseGroupBlueprintPreparedMutation(
		environmentID,
		conditions,
		mutations,
	), nil
}

func sortedReleaseGroupIDs[T any](values map[string]T) []string {
	result := make([]string, 0, len(values))
	for id := range values {
		result = append(result, id)
	}
	sort.Strings(result)
	return result
}
