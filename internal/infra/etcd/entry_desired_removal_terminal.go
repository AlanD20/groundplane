package etcd

import (
	"context"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	entryvalues "github.com/AlanD20/groundplane/internal/infra/etcd/entryvalues"
	environmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) prepareDesiredEntryRemovalAcknowledgement(
	ctx context.Context, task TaskRecord, intent environmentchanges.EntryRemovalIntent, intentRevision int64,
	status taskjournal.TaskStatus, terminalAt time.Time, revision int64,
) (routeTaskChange, error) {
	keys := []string{deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetEntry), intent.EntryID),
		environmentchanges.ComponentTaskActiveEnvironmentKey(intent.EnvironmentID)}
	if task.Executor == taskjournal.TaskExecutorController {
		keys = append(keys, taskjournal.TaskMaterializationWriterKey(intent.EnvironmentID))
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return routeTaskChange{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) ||
		read.Values[0] == nil || read.Values[1] == nil || string(read.Values[1].Value) != task.ID {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "Entry removal terminal ownership changed")
	}
	tombstone, err := deletionrecord.DecodeDeletionTombstone(read.Values[0].Value)
	if err != nil || tombstone.TargetKind != deletionrecord.DeletionTargetEntry || tombstone.TargetID != intent.EntryID ||
		tombstone.TaskID != task.ID || tombstone.TargetRevision != intent.EntryRevision ||
		tombstone.Phase != entryRemovalTombstonePhase(intent) {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "Entry removal tombstone changed")
	}
	terminal, err := environmentchanges.TerminalEntryRemovalIntent(intent, status, terminalAt)
	if err != nil {
		return routeTaskChange{}, err
	}
	value, err := environmentchanges.EncodeEntryRemovalIntent(terminal)
	if err != nil {
		return routeTaskChange{}, err
	}
	change := routeTaskChange{applies: true,
		conditions: []etcdstore.Condition{{Key: environmentchanges.EntryRemovalIntentKey(task.ID), ModRevision: intentRevision},
			{
				Key:         keys[0],
				ModRevision: read.Values[0].ModRevision,
			}, {Key: keys[1], ModRevision: read.Values[1].ModRevision}},
		mutations: []etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: environmentchanges.EntryRemovalIntentKey(task.ID), Value: value},
			{Type: etcdstore.MutationDelete, Key: keys[0]}, {Type: etcdstore.MutationDelete, Key: keys[1]}}, values: [][]byte{value}}
	if task.Executor == taskjournal.TaskExecutorController {
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
		change.conditions = append(change.conditions, etcdstore.Condition{Key: keys[2], ModRevision: read.Values[2].ModRevision})
		change.mutations = append(change.mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: keys[2]})
	}
	if status != taskjournal.TaskStatusCompleted {
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
	ctx context.Context, intent environmentchanges.EntryRemovalIntent, revision int64,
) (routeTaskChange, error) {
	desired := intent.Desired
	keys := []string{blueprints.EnvironmentBlueprintHeadKey(intent.EnvironmentID),
		blueprints.EnvironmentBlueprintDescriptorKeyByID(desired.DescriptorID),
		blueprints.EnvironmentBlueprintRootKey(intent.EnvironmentID, desired.RevisionID),
		projectionrecord.EnvironmentComposeProjectionStorageKey(intent.EnvironmentID), blueprintEntryEnvironmentPrefix + intent.EntryID}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
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
	baseID, err := idempotencyrecord.DecodeTaskReference(read.Values[0].Value)
	if err != nil || baseID != desired.BaseRevisionID {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "Entry removal baseline head changed")
	}
	descriptor, err := blueprints.DecodeEnvironmentBlueprintStageDescriptor(read.Values[1].Value)
	if err != nil {
		return routeTaskChange{}, err
	}
	seal, err := blueprints.DecodeEnvironmentBlueprintSeal(read.Values[2].Value)
	if err != nil || descriptor.State != blueprints.EnvironmentBlueprintStageSealed ||
		descriptor.Claim.DescriptorID != desired.DescriptorID || descriptor.Claim.EnvironmentID != intent.EnvironmentID ||
		descriptor.Claim.RevisionID != desired.RevisionID || descriptor.Claim.TaskID != desired.RevisionID ||
		descriptor.Claim.RenderGeneration != desired.RenderGeneration ||
		descriptor.Claim.BaselineHeadRevision != intent.EntryRevision ||
		descriptor.Claim.SourceKind != blueprints.EnvironmentBlueprintSourceMutation || seal != blueprints.EnvironmentBlueprintSealFromDescriptor(descriptor) {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "Entry removal staged revision changed")
	}
	hierarchy := &HierarchyRepository{store: repository.store}
	chunkKeys := make([]string, seal.ProjectionChunks)
	for index := range chunkKeys {
		chunkKeys[index] = blueprints.EnvironmentBlueprintChunkKeyFor(intent.EnvironmentID, desired.RevisionID,
			blueprints.EnvironmentBlueprintChunkProjection, uint32(index))
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
	candidate, err := projectionrecord.DecodeEnvironmentComposeProjectionStorage(projectionValue)
	if err != nil || candidate.EnvironmentID != intent.EnvironmentID || candidate.RevisionID != desired.RevisionID ||
		candidate.RenderGeneration != desired.RenderGeneration {
		return routeTaskChange{}, projectionrecord.CorruptEnvironmentComposeProjection()
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
	locatorKey, _, err := blueprints.EnvironmentBlueprintLocatorKey(descriptor.Claim.Locator)
	if err != nil {
		return routeTaskChange{}, err
	}
	descriptor.State, descriptor.UpdatedAt = blueprints.EnvironmentBlueprintStagePublished, blueprints.NextBlueprintProgressTime(
		descriptor.UpdatedAt,
	)
	descriptorValue, err := blueprints.EncodeEnvironmentBlueprintStageDescriptor(descriptor)
	if err != nil {
		return routeTaskChange{}, err
	}
	reference, err := idempotencyrecord.EncodeTaskReference(desired.RevisionID)
	if err != nil {
		clear(descriptorValue)
		return routeTaskChange{}, err
	}
	change := routeTaskChange{applies: true,
		conditions: []etcdstore.Condition{{Key: keys[0], ModRevision: read.Values[0].ModRevision},
			{
				Key:         keys[1],
				ModRevision: read.Values[1].ModRevision,
			}, {Key: keys[2], ModRevision: read.Values[2].ModRevision},
			{Key: keys[4], ModRevision: read.Values[4].ModRevision}},
		mutations: []etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: keys[0], Value: reference},
			{Type: etcdstore.MutationPut, Key: keys[1], Value: descriptorValue}, {Type: etcdstore.MutationDelete, Key: locatorKey},
			{Type: etcdstore.MutationDelete, Key: keys[4]},
			{Type: etcdstore.MutationDelete, Key: entryvalues.PlainPrefix + intent.EntryID + "/", Prefix: true},
			{Type: etcdstore.MutationDelete, Key: entryvalues.SecretPrefix + intent.EntryID + "/", Prefix: true}},
		values: [][]byte{descriptorValue, reference}}
	change.conditions = append(change.conditions, sources...)
	change.conditions = append(
		change.conditions,
		etcdstore.Condition{Key: keys[3], ModRevision: etcdstore.RevisionOf(read.Values[3])},
	)
	if intent.CurrentProjection == nil && read.Values[3] != nil {
		applied, err := projectionrecord.DecodeEnvironmentComposeProjectionStorage(read.Values[3].Value)
		if err != nil || applied.EnvironmentID != intent.EnvironmentID {
			clearRouteTaskChange(change)
			return routeTaskChange{}, projectionrecord.CorruptEnvironmentComposeProjection()
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
		current, err := projectionrecord.DecodeEnvironmentComposeProjectionStorage(read.Values[3].Value)
		if err != nil || !environmentchanges.SameEntryRemovalProjection(current, *intent.CurrentProjection) {
			clearRouteTaskChange(change)
			return routeTaskChange{}, errs.New(errs.KindStateConflict, "Entry removal applied state changed")
		}
		appliedValue, err := projectionrecord.EncodeEnvironmentComposeProjectionStorage(*intent.CandidateProjection)
		if err != nil {
			clearRouteTaskChange(change)
			return routeTaskChange{}, err
		}
		change.mutations = append(change.mutations, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: keys[3], Value: appliedValue})
		change.values = append(change.values, appliedValue)
	}
	return change, nil
}
