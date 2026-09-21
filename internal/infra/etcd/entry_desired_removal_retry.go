package etcd

import (
	"context"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	environmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) prepareDesiredEntryRemovalRetry(
	ctx context.Context, source, retry TaskRecord, intent environmentchanges.EntryRemovalIntent, sourceIntentRevision, revision int64,
) (routeTaskChange, error) {
	desired := intent.Desired
	keys := []string{blueprints.EnvironmentBlueprintHeadKey(intent.EnvironmentID),
		deletionrecord.TombstoneKey(
			string(deletionrecord.DeletionTargetEntry),
			intent.EntryID,
		), componentTaskActiveEnvironmentKey(intent.EnvironmentID),
		projectionrecord.EnvironmentComposeProjectionStorageKey(
			intent.EnvironmentID,
		), blueprints.EnvironmentBlueprintDescriptorKeyByID(desired.DescriptorID),
		blueprints.EnvironmentBlueprintRootKey(
			intent.EnvironmentID,
			desired.RevisionID,
		), blueprintEntryEnvironmentPrefix + intent.EntryID,
		taskjournal.TaskMaterializationWriterKey(intent.EnvironmentID)}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return routeTaskChange{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) ||
		read.Values[0] == nil || read.Values[0].ModRevision != intent.EntryRevision || read.Values[1] != nil ||
		read.Values[2] != nil || read.Values[4] == nil || read.Values[5] == nil || read.Values[6] == nil ||
		string(read.Values[6].Value) != intent.EnvironmentID || read.Values[7] != nil {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "Entry removal retry ownership changed")
	}
	baseID, err := idempotencyrecord.DecodeTaskReference(read.Values[0].Value)
	if err != nil || baseID != desired.BaseRevisionID {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "Entry removal retry desired head changed")
	}
	descriptor, err := blueprints.DecodeEnvironmentBlueprintStageDescriptor(read.Values[4].Value)
	if err != nil {
		return routeTaskChange{}, err
	}
	seal, err := blueprints.DecodeEnvironmentBlueprintSeal(read.Values[5].Value)
	if err != nil || descriptor.State != blueprints.EnvironmentBlueprintStageSealed ||
		descriptor.Claim.DescriptorID != desired.DescriptorID || descriptor.Claim.EnvironmentID != intent.EnvironmentID ||
		descriptor.Claim.RevisionID != desired.RevisionID || descriptor.Claim.TaskID != desired.RevisionID ||
		descriptor.Claim.BaselineHeadRevision != intent.EntryRevision || descriptor.Claim.RenderGeneration != desired.RenderGeneration ||
		descriptor.Claim.SourceKind != blueprints.EnvironmentBlueprintSourceMutation || seal != blueprints.EnvironmentBlueprintSealFromDescriptor(descriptor) {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "Entry removal retry staged candidate changed")
	}
	if intent.CurrentProjection != nil &&
		(read.Values[3] == nil || read.Values[3].ModRevision != intent.CurrentProjectionRevision) {
		return routeTaskChange{}, errs.New(errs.KindStateConflict, "Entry removal retry applied state changed")
	}
	if read.Values[3] != nil {
		applied, err := projectionrecord.DecodeEnvironmentComposeProjectionStorage(read.Values[3].Value)
		if err != nil || applied.EnvironmentID != intent.EnvironmentID {
			return routeTaskChange{}, projectionrecord.CorruptEnvironmentComposeProjection()
		}
		if intent.CurrentProjection != nil && !environmentchanges.SameEntryRemovalProjection(applied, *intent.CurrentProjection) {
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
	tombstone, err := deletionrecord.EncodeDeletionTombstone(deletionrecord.DeletionTombstoneRecord{TargetKind: deletionrecord.DeletionTargetEntry,
		TargetID: intent.EntryID, TargetRevision: intent.EntryRevision, TaskID: retry.ID,
		Phase: entryRemovalTombstonePhase(intent), CreatedAt: retry.CreatedAt, UpdatedAt: retry.CreatedAt})
	if err != nil {
		return routeTaskChange{}, err
	}
	value, err := environmentchanges.EncodeEntryRemovalIntent(intent)
	if err != nil {
		clear(tombstone)
		return routeTaskChange{}, err
	}
	change := routeTaskChange{applies: true,
		conditions: []etcdstore.Condition{
			{Key: environmentchanges.EntryRemovalIntentKey(source.ID), ModRevision: sourceIntentRevision},
			{Key: environmentchanges.EntryRemovalIntentKey(retry.ID)},
		},
		mutations: []etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: environmentchanges.EntryRemovalIntentKey(retry.ID), Value: value},
			{
				Type:  etcdstore.MutationPut,
				Key:   keys[1],
				Value: tombstone,
			}, {Type: etcdstore.MutationPut, Key: keys[2], Value: []byte(retry.ID)}},
		values: [][]byte{value, tombstone}}
	for index, key := range keys {
		condition := etcdstore.Condition{Key: key}
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
