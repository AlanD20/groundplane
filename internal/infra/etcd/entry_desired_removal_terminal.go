package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) prepareDesiredEntryRemovalAcknowledgement(
	ctx context.Context, task TaskRecord, intent EntryRemovalIntent, intentRevision int64,
	status TaskStatus, terminalAt time.Time, revision int64,
) (routeTaskChange, error) {
	keys := []string{deletionTombstoneKey(string(DeletionTargetEntry), intent.EntryID),
		componentTaskActiveEnvironmentKey(intent.EnvironmentID)}
	if task.Executor == TaskExecutorController {
		keys = append(keys, taskMaterializationWriterKey(intent.EnvironmentID))
	}
	read, err := repository.store.GetMany(ctx, GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return routeTaskChange{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) ||
		read.Values[0] == nil || read.Values[1] == nil || string(read.Values[1].Value) != task.ID {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "Entry removal terminal ownership changed")
	}
	tombstone, err := decodeDeletionTombstone(read.Values[0].Value)
	if err != nil || tombstone.TargetKind != DeletionTargetEntry || tombstone.TargetID != intent.EntryID ||
		tombstone.TaskID != task.ID || tombstone.TargetRevision != intent.EntryRevision ||
		tombstone.Phase != entryRemovalTombstonePhase(intent) {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "Entry removal tombstone changed")
	}
	terminal, err := terminalEntryRemovalIntent(intent, status, terminalAt)
	if err != nil {
		return routeTaskChange{}, err
	}
	value, err := encodeEntryRemovalIntent(terminal)
	if err != nil {
		return routeTaskChange{}, err
	}
	change := routeTaskChange{applies: true,
		conditions: []Condition{{Key: entryRemovalIntentKey(task.ID), ModRevision: intentRevision},
			{
				Key:         keys[0],
				ModRevision: read.Values[0].ModRevision,
			}, {Key: keys[1], ModRevision: read.Values[1].ModRevision}},
		mutations: []Mutation{{Type: MutationPut, Key: entryRemovalIntentKey(task.ID), Value: value},
			{Type: MutationDelete, Key: keys[0]}, {Type: MutationDelete, Key: keys[1]}}, values: [][]byte{value}}
	if task.Executor == TaskExecutorController {
		if read.Values[2] == nil {
			clearRouteTaskChange(change)
			return routeTaskChange{}, errs.New(
				errs.KindStateConflict,
				"Entry removal materialization exclusion is absent",
			)
		}
		writer, err := decodeTaskMaterializationWriter(read.Values[2].Value)
		if err != nil || validateTaskMaterializationWriterForTask(writer, task, intent.EnvironmentID) != nil {
			clearRouteTaskChange(change)
			return routeTaskChange{}, errs.New(
				errs.KindStateConflict,
				"Entry removal materialization exclusion changed",
			)
		}
		change.conditions = append(change.conditions, Condition{Key: keys[2], ModRevision: read.Values[2].ModRevision})
		change.mutations = append(change.mutations, Mutation{Type: MutationDelete, Key: keys[2]})
	}
	if status != TaskStatusCompleted {
		return change, nil
	}
	promotion, err := repository.prepareEntryRemovalHeadPromotion(ctx, intent, revision)
	if err != nil {
		clearRouteTaskChange(change)
		return routeTaskChange{}, err
	}
	change.conditions = append(change.conditions, promotion.conditions...)
	change.mutations = append(change.mutations, promotion.mutations...)
	change.values = append(change.values, promotion.values...)
	return change, nil
}

func (repository *TaskRepository) prepareEntryRemovalHeadPromotion(
	ctx context.Context, intent EntryRemovalIntent, revision int64,
) (routeTaskChange, error) {
	desired := intent.Desired
	keys := []string{environmentBlueprintHeadKey(intent.EnvironmentID),
		environmentBlueprintDescriptorKeyByID(desired.DescriptorID),
		environmentBlueprintRootKey(intent.EnvironmentID, desired.RevisionID),
		environmentComposeProjectionKey(intent.EnvironmentID), blueprintEntryEnvironmentPrefix + intent.EntryID}
	read, err := repository.store.GetMany(ctx, GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return routeTaskChange{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) ||
		read.Values[0] == nil || read.Values[1] == nil || read.Values[2] == nil || read.Values[4] == nil ||
		read.Values[0].ModRevision != intent.EntryRevision || string(read.Values[4].Value) != intent.EnvironmentID {
		return routeTaskChange{}, errs.New(
			errs.KindStateConflict,
			"Entry removal desired publication authority changed",
		)
	}
	baseID, err := decodeTaskReference(read.Values[0].Value)
	if err != nil || baseID != desired.BaseRevisionID {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "Entry removal baseline head changed")
	}
	descriptor, err := decodeEnvironmentBlueprintStageDescriptor(read.Values[1].Value)
	if err != nil {
		return routeTaskChange{}, err
	}
	seal, err := decodeEnvironmentBlueprintSeal(read.Values[2].Value)
	if err != nil || descriptor.State != EnvironmentBlueprintStageSealed ||
		descriptor.Claim.DescriptorID != desired.DescriptorID || descriptor.Claim.EnvironmentID != intent.EnvironmentID ||
		descriptor.Claim.RevisionID != desired.RevisionID || descriptor.Claim.TaskID != desired.RevisionID ||
		descriptor.Claim.RenderGeneration != desired.RenderGeneration ||
		descriptor.Claim.BaselineHeadRevision != intent.EntryRevision ||
		descriptor.Claim.SourceKind != EnvironmentBlueprintSourceMutation || seal != environmentBlueprintSealFromDescriptor(descriptor) {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "Entry removal staged revision changed")
	}
	hierarchy := &HierarchyRepository{store: repository.store}
	chunkKeys := make([]string, seal.ProjectionChunks)
	for index := range chunkKeys {
		chunkKeys[index] = environmentBlueprintChunkKeyFor(intent.EnvironmentID, desired.RevisionID,
			EnvironmentBlueprintChunkProjection, uint32(index))
	}
	projectionValue, _, err := hierarchy.readEnvironmentBlueprintStreamAtRevision(
		ctx,
		seal,
		"projection",
		chunkKeys,
		revision,
	)
	if err != nil {
		return routeTaskChange{}, err
	}
	defer clear(projectionValue)
	candidate, err := decodeEnvironmentComposeProjection(projectionValue)
	if err != nil || candidate.EnvironmentID != intent.EnvironmentID || candidate.RevisionID != desired.RevisionID ||
		candidate.RenderGeneration != desired.RenderGeneration {
		return routeTaskChange{}, corruptEnvironmentComposeProjection()
	}
	for _, entry := range candidate.Entries {
		if entry.Entry.ID == intent.EntryID {
			return routeTaskChange{}, errs.New(errs.KindStateConflict, "Entry removal candidate retains its target")
		}
	}
	sources, err := prepareEntryScriptAbsence(ctx, repository.store, intent.EntryID, revision)
	if err != nil {
		return routeTaskChange{}, err
	}
	locatorKey, _, err := environmentBlueprintLocatorKey(descriptor.Claim.Locator)
	if err != nil {
		return routeTaskChange{}, err
	}
	descriptor.State, descriptor.UpdatedAt = EnvironmentBlueprintStagePublished, nextBlueprintProgressTime(
		descriptor.UpdatedAt,
	)
	descriptorValue, err := encodeEnvironmentBlueprintStageDescriptor(descriptor)
	if err != nil {
		return routeTaskChange{}, err
	}
	reference, err := encodeTaskReference(desired.RevisionID)
	if err != nil {
		clear(descriptorValue)
		return routeTaskChange{}, err
	}
	change := routeTaskChange{applies: true,
		conditions: []Condition{{Key: keys[0], ModRevision: read.Values[0].ModRevision},
			{
				Key:         keys[1],
				ModRevision: read.Values[1].ModRevision,
			}, {Key: keys[2], ModRevision: read.Values[2].ModRevision},
			{Key: keys[4], ModRevision: read.Values[4].ModRevision}},
		mutations: []Mutation{{Type: MutationPut, Key: keys[0], Value: reference},
			{Type: MutationPut, Key: keys[1], Value: descriptorValue}, {Type: MutationDelete, Key: locatorKey},
			{Type: MutationDelete, Key: keys[4]},
			{Type: MutationDelete, Key: entryPlainValueGenerationPrefix + intent.EntryID + "/", Prefix: true},
			{Type: MutationDelete, Key: entrySecretValueGenerationPrefix + intent.EntryID + "/", Prefix: true}},
		values: [][]byte{descriptorValue, reference}}
	change.conditions = append(change.conditions, sources...)
	change.conditions = append(
		change.conditions,
		Condition{Key: keys[3], ModRevision: keyValueRevision(read.Values[3])},
	)
	if intent.CurrentProjection == nil && read.Values[3] != nil {
		applied, err := decodeEnvironmentComposeProjection(read.Values[3].Value)
		if err != nil || applied.EnvironmentID != intent.EnvironmentID {
			clearRouteTaskChange(change)
			return routeTaskChange{}, corruptEnvironmentComposeProjection()
		}
		for _, entry := range applied.Entries {
			if entry.Entry.ID == intent.EntryID {
				clearRouteTaskChange(change)
				return routeTaskChange{}, errs.New(errs.KindStateConflict, "Entry removal now requires host cleanup")
			}
		}
	}
	if intent.CurrentProjection != nil {
		if read.Values[3] == nil || read.Values[3].ModRevision != intent.CurrentProjectionRevision {
			clearRouteTaskChange(change)
			return routeTaskChange{}, errs.New(errs.KindStateConflict, "Entry removal applied state changed")
		}
		current, err := decodeEnvironmentComposeProjection(read.Values[3].Value)
		if err != nil || !sameEntryRemovalProjection(current, *intent.CurrentProjection) {
			clearRouteTaskChange(change)
			return routeTaskChange{}, errs.New(errs.KindStateConflict, "Entry removal applied state changed")
		}
		appliedValue, err := encodeEnvironmentComposeProjection(*intent.CandidateProjection)
		if err != nil {
			clearRouteTaskChange(change)
			return routeTaskChange{}, err
		}
		change.mutations = append(change.mutations, Mutation{Type: MutationPut, Key: keys[3], Value: appliedValue})
		change.values = append(change.values, appliedValue)
	}
	return change, nil
}
