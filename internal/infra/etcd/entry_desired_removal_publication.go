package etcd

import (
	"context"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type entryDesiredRemovalPublication struct {
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
	deferred   map[string]bool
}

func desiredRevisionTaskEnvironment(task TaskRecord) (string, bool, error) {
	if task.Executor == taskjournal.TaskExecutorController && task.Type == taskjournal.TaskRemove &&
		ids.Validate(ids.KindEnvEntry, task.Target) == nil && len(task.Params) == 3 &&
		task.Params[TaskResourceKindParam] == TaskResourceEntry && len(task.Materializations) == 0 &&
		ids.Validate(ids.KindEnvironment, task.Params[TaskEntryEnvironmentParam]) == nil &&
		ids.Validate(ids.KindTask, task.Params[EnvironmentDesiredRevisionParam]) == nil {
		return task.Params[TaskEntryEnvironmentParam], true, nil
	}
	return taskMaterializationEnvironment(task)
}

func (repository *HierarchyRepository) prepareDesiredEntryRemovalPublication(
	ctx context.Context, claim EnvironmentBlueprintStageClaim, candidate projectionrecord.EnvironmentComposeProjection,
	task TaskRecord, removed preparedDesiredScriptRemoval, revision int64,
) (entryDesiredRemovalPublication, error) {
	if task.Type != taskjournal.TaskRemove || ids.Validate(ids.KindEnvEntry, task.Target) != nil {
		return entryDesiredRemovalPublication{}, nil
	}
	if claim.SourceKind != EnvironmentBlueprintSourceMutation || len(removed.entryIDs) != 1 ||
		removed.entryIDs[0] != task.Target || removed.volumeID != "" {
		return entryDesiredRemovalPublication{}, errs.New(
			errs.KindValidationFailed,
			"Entry removal must remove exactly its target",
		)
	}
	current, found, err := repository.getEnvironmentBlueprintProjectionAtRevision(ctx, claim.EnvironmentID, revision)
	if err != nil {
		return entryDesiredRemovalPublication{}, err
	}
	if !found || current.Revision != claim.BaselineHeadRevision {
		return entryDesiredRemovalPublication{}, errs.New(errs.KindStateConflict, "Entry removal desired head changed")
	}
	expected, changed, err := projectionrecord.RemoveEnvironmentEntry(current.Record, task.Target)
	if err != nil || !changed {
		return entryDesiredRemovalPublication{}, errs.New(errs.KindEntryNotFound, "Entry removal target is absent")
	}
	expected.RevisionID, expected.RenderGeneration = candidate.RevisionID, candidate.RenderGeneration
	expected.ComposeArtifact = candidate.ComposeArtifact
	if !sameEntryRemovalProjection(expected, candidate) {
		return entryDesiredRemovalPublication{}, errs.New(
			errs.KindValidationFailed,
			"Entry removal changed unrelated desired decisions",
		)
	}
	keys := []string{projectionrecord.EnvironmentComposeProjectionStorageKey(claim.EnvironmentID),
		deletionTombstoneKey(string(deletionrecord.DeletionTargetEntry), task.Target), entryRemovalIntentKey(task.ID),
		componentTaskActiveEnvironmentKey(claim.EnvironmentID), taskMaterializationWriterKey(claim.EnvironmentID),
		environmentBlueprintDescriptorKeyByID(claim.DescriptorID)}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return entryDesiredRemovalPublication{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
		return entryDesiredRemovalPublication{}, errs.New(
			errs.KindInternal,
			"Entry removal publication read is incomplete",
		)
	}
	var applied *etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]
	conditions := make([]etcdstore.Condition, len(keys)-1)
	for index, key := range keys[:len(conditions)] {
		conditions[index] = etcdstore.Condition{Key: key}
		if index != 0 && read.Values[index] != nil {
			return entryDesiredRemovalPublication{}, errs.New(
				errs.KindResourceInUse,
				"Entry removal or Environment mutation is in progress",
			)
		}
	}
	if value := read.Values[0]; value != nil {
		projection, err := projectionrecord.DecodeEnvironmentComposeProjectionStorage(value.Value)
		if err != nil || projection.EnvironmentID != claim.EnvironmentID {
			return entryDesiredRemovalPublication{}, projectionrecord.CorruptEnvironmentComposeProjection()
		}
		conditions[0].ModRevision = value.ModRevision
		for _, entry := range projection.Entries {
			if entry.Entry.ID == task.Target {
				applied = &etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{
					Record:       projection,
					Revision:     value.ModRevision,
					ReadRevision: revision,
				}
				break
			}
		}
	}
	intent, err := NewDesiredEntryRemovalIntent(task.Target, current.Record.RevisionID, claim, applied)
	if err != nil {
		return entryDesiredRemovalPublication{}, err
	}
	if err := validateEntryRemovalTaskOwner(task, intent); err != nil {
		return entryDesiredRemovalPublication{}, err
	}
	tombstone, err := deletionrecord.EncodeDeletionTombstone(deletionrecord.DeletionTombstoneRecord{
		TargetKind: deletionrecord.DeletionTargetEntry, TargetID: task.Target, TargetRevision: current.Revision,
		TaskID: task.ID, Phase: entryRemovalTombstonePhase(intent), CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt,
	})
	if err != nil {
		return entryDesiredRemovalPublication{}, err
	}
	intentValue, err := encodeEntryRemovalIntent(intent)
	if err != nil {
		clear(tombstone)
		return entryDesiredRemovalPublication{}, err
	}
	// Keep the private stage sealed until cleanup succeeds, but advance its
	// revision atomically with Task publication. Staging GC's descriptor CAS
	// then fences a Task published after its fixed-revision absence read.
	if read.Values[5] == nil {
		clear(tombstone)
		clear(intentValue)
		return entryDesiredRemovalPublication{}, corruptEnvironmentBlueprintStage()
	}
	descriptor, err := decodeEnvironmentBlueprintStageDescriptor(read.Values[5].Value)
	if err != nil || descriptor.State != EnvironmentBlueprintStageSealed ||
		!sameEnvironmentBlueprintStageClaim(descriptor.Claim, claim) {
		clear(tombstone)
		clear(intentValue)
		return entryDesiredRemovalPublication{}, corruptEnvironmentBlueprintStage()
	}
	descriptor.UpdatedAt = nextBlueprintProgressTime(descriptor.UpdatedAt)
	descriptorValue, err := encodeEnvironmentBlueprintStageDescriptor(descriptor)
	if err != nil {
		clear(tombstone)
		clear(intentValue)
		return entryDesiredRemovalPublication{}, err
	}
	locatorKey, _, err := environmentBlueprintLocatorKey(claim.Locator)
	if err != nil {
		clear(tombstone)
		clear(intentValue)
		clear(descriptorValue)
		return entryDesiredRemovalPublication{}, err
	}
	writer, err := entryRemovalControllerWriter(task)
	if err != nil {
		clear(tombstone)
		clear(intentValue)
		clear(descriptorValue)
		return entryDesiredRemovalPublication{}, err
	}
	return entryDesiredRemovalPublication{conditions: conditions,
		mutations: append([]etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: keys[1], Value: tombstone},
			{Type: etcdstore.MutationPut, Key: keys[2], Value: intentValue},
			{Type: etcdstore.MutationPut, Key: keys[3], Value: []byte(task.ID)},
			{Type: etcdstore.MutationPut, Key: keys[5], Value: descriptorValue}}, writer...),
		deferred: map[string]bool{environmentBlueprintHeadKey(claim.EnvironmentID): true,
			environmentBlueprintDescriptorKeyByID(claim.DescriptorID): true, locatorKey: true}}, nil
}

func entryRemovalControllerWriter(task TaskRecord) ([]etcdstore.Mutation, error) {
	if task.Executor != taskjournal.TaskExecutorController {
		return nil, nil
	}
	environmentID := task.Params[TaskEntryEnvironmentParam]
	value, err := encodeTaskMaterializationWriter(taskMaterializationWriter(task, environmentID, nil))
	if err != nil {
		return nil, err
	}
	return []etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: taskMaterializationWriterKey(environmentID), Value: value}}, nil
}

func (publication entryDesiredRemovalPublication) bind(
	conditions []etcdstore.Condition, mutations []etcdstore.Mutation, classify idempotencyPlanClassifier,
) ([]etcdstore.Condition, []etcdstore.Mutation, idempotencyPlanClassifier) {
	if publication.deferred == nil {
		return conditions, mutations, classify
	}
	base := len(conditions)
	conditions = append(conditions, publication.conditions...)
	retained := mutations[:0]
	for _, mutation := range mutations {
		if !publication.deferred[mutation.Key] {
			retained = append(retained, mutation)
		}
	}
	mutations = append(retained, publication.mutations...)
	return conditions, mutations, func(revision int64, values []*etcdstore.KeyValue) error {
		if len(values) != base+len(publication.conditions) {
			return errs.New(errs.KindInternal, "Entry removal compare evidence is incomplete")
		}
		if err := classify(revision, values[:base]); err != nil {
			return err
		}
		for index, condition := range publication.conditions {
			if !conditionMatchesRead(condition, values[base+index]) {
				return errs.New(errs.KindStateConflict, "Entry removal publication authority changed")
			}
		}
		return nil
	}
}
