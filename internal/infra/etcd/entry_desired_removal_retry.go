package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) prepareDesiredEntryRemovalRetry(
	ctx context.Context, source, retry TaskRecord, intent EntryRemovalIntent, sourceIntentRevision, revision int64,
) (routeTaskChange, error) {
	desired := intent.Desired
	keys := []string{environmentBlueprintHeadKey(intent.EnvironmentID),
		deletionTombstoneKey(
			string(DeletionTargetEntry),
			intent.EntryID,
		), componentTaskActiveEnvironmentKey(intent.EnvironmentID),
		environmentComposeProjectionKey(
			intent.EnvironmentID,
		), environmentBlueprintDescriptorKeyByID(desired.DescriptorID),
		environmentBlueprintRootKey(
			intent.EnvironmentID,
			desired.RevisionID,
		), blueprintEntryEnvironmentPrefix + intent.EntryID,
		taskMaterializationWriterKey(intent.EnvironmentID)}
	read, err := repository.store.GetMany(ctx, GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return routeTaskChange{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) ||
		read.Values[0] == nil || read.Values[0].ModRevision != intent.EntryRevision || read.Values[1] != nil ||
		read.Values[2] != nil || read.Values[4] == nil || read.Values[5] == nil || read.Values[6] == nil ||
		string(read.Values[6].Value) != intent.EnvironmentID || read.Values[7] != nil {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "Entry removal retry ownership changed")
	}
	baseID, err := decodeTaskReference(read.Values[0].Value)
	if err != nil || baseID != desired.BaseRevisionID {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "Entry removal retry desired head changed")
	}
	descriptor, err := decodeEnvironmentBlueprintStageDescriptor(read.Values[4].Value)
	if err != nil {
		return routeTaskChange{}, err
	}
	seal, err := decodeEnvironmentBlueprintSeal(read.Values[5].Value)
	if err != nil || descriptor.State != EnvironmentBlueprintStageSealed ||
		descriptor.Claim.DescriptorID != desired.DescriptorID || descriptor.Claim.EnvironmentID != intent.EnvironmentID ||
		descriptor.Claim.RevisionID != desired.RevisionID || descriptor.Claim.TaskID != desired.RevisionID ||
		descriptor.Claim.BaselineHeadRevision != intent.EntryRevision || descriptor.Claim.RenderGeneration != desired.RenderGeneration ||
		descriptor.Claim.SourceKind != EnvironmentBlueprintSourceMutation || seal != environmentBlueprintSealFromDescriptor(descriptor) {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "Entry removal retry staged candidate changed")
	}
	if intent.CurrentProjection != nil &&
		(read.Values[3] == nil || read.Values[3].ModRevision != intent.CurrentProjectionRevision) {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "Entry removal retry applied state changed")
	}
	if read.Values[3] != nil {
		applied, err := decodeEnvironmentComposeProjection(read.Values[3].Value)
		if err != nil || applied.EnvironmentID != intent.EnvironmentID {
			return routeTaskChange{}, corruptEnvironmentComposeProjection()
		}
		if intent.CurrentProjection != nil && !sameEntryRemovalProjection(applied, *intent.CurrentProjection) {
			return routeTaskChange{}, errs.New(errs.KindStateConflict, "Entry removal retry applied state changed")
		}
		if intent.CurrentProjection == nil {
			for _, entry := range applied.Entries {
				if entry.Entry.ID == intent.EntryID {
					return routeTaskChange{}, errs.New(
						errs.KindStateConflict,
						"Entry removal now requires host cleanup",
					)
				}
			}
		}
	}
	sources, err := prepareEntryScriptAbsence(ctx, repository.store, intent.EntryID, revision)
	if err != nil {
		return routeTaskChange{}, err
	}
	tombstone, err := encodeDeletionTombstone(DeletionTombstoneRecord{TargetKind: DeletionTargetEntry,
		TargetID: intent.EntryID, TargetRevision: intent.EntryRevision, TaskID: retry.ID,
		Phase: entryRemovalTombstonePhase(intent), CreatedAt: retry.CreatedAt, UpdatedAt: retry.CreatedAt})
	if err != nil {
		return routeTaskChange{}, err
	}
	value, err := encodeEntryRemovalIntent(intent)
	if err != nil {
		clear(tombstone)
		return routeTaskChange{}, err
	}
	change := routeTaskChange{applies: true,
		conditions: []Condition{
			{Key: entryRemovalIntentKey(source.ID), ModRevision: sourceIntentRevision},
			{Key: entryRemovalIntentKey(retry.ID)},
		},
		mutations: []Mutation{{Type: MutationPut, Key: entryRemovalIntentKey(retry.ID), Value: value},
			{
				Type:  MutationPut,
				Key:   keys[1],
				Value: tombstone,
			}, {Type: MutationPut, Key: keys[2], Value: []byte(retry.ID)}},
		values: [][]byte{value, tombstone}}
	for index, key := range keys {
		condition := Condition{Key: key}
		if read.Values[index] != nil {
			condition.ModRevision = read.Values[index].ModRevision
		}
		change.conditions = append(change.conditions, condition)
	}
	change.conditions = append(change.conditions, sources...)
	writer, err := entryRemovalControllerWriter(retry)
	if err != nil {
		clearRouteTaskChange(change)
		return routeTaskChange{}, err
	}
	change.mutations = append(change.mutations, writer...)
	for _, mutation := range writer {
		change.values = append(change.values, mutation.Value)
	}
	return change, nil
}
