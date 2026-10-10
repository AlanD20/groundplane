package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// BlueprintEntryRemovalPublication is the private immutable cleanup candidate
// carried from authored planning into the child publication transaction.
type BlueprintEntryRemovalPublication struct {
	intent environmentchanges.EntryRemovalIntent
}

func NewBlueprintEntryRemovalPublication(
	task TaskRecord, intent environmentchanges.EntryRemovalIntent,
) (BlueprintEntryRemovalPublication, error) {
	if err := validateEntryRemovalTaskOwner(task, intent); err != nil || intent.Authored == nil {
		return BlueprintEntryRemovalPublication{}, errs.New(
			errs.KindValidationFailed, "Blueprint Entry removal publication is invalid",
		)
	}
	return BlueprintEntryRemovalPublication{intent: environmentchanges.CloneEntryRemovalIntent(intent)}, nil
}

func (publication BlueprintEntryRemovalPublication) IsZero() bool {
	return publication.intent.TaskID == ""
}

func (publication *BlueprintEntryRemovalPublication) Clear() {
	if publication == nil {
		return
	}
	if publication.intent.CurrentProjection != nil {
		clear(publication.intent.CurrentProjection.ComposeArtifact)
	}
	if publication.intent.CandidateProjection != nil {
		clear(publication.intent.CandidateProjection.ComposeArtifact)
	}
	*publication = BlueprintEntryRemovalPublication{}
}

func (publication BlueprintEntryRemovalPublication) prepare(
	ctx context.Context,
	repository *TaskRepository,
	task TaskRecord,
	unit blueprintunits.Unit,
	revision int64,
) ([]keyvalue.Condition, []keyvalue.Mutation, error) {
	intent := publication.intent
	if repository == nil || publication.IsZero() || intent.Authored == nil ||
		unit.Target.Kind != ids.KindEnvEntry || !unit.Removal || unit.Target.ID != intent.EntryID ||
		validateEntryRemovalTaskOwner(task, intent) != nil {
		return nil, nil, errs.New(errs.KindValidationFailed, "Blueprint Entry removal publication changed")
	}
	desired, found, err := blueprints.ReadDesiredInputRevision(
		ctx, repository.store, intent.EnvironmentID, intent.Authored.ParentTaskID, revision,
	)
	if err != nil {
		return nil, nil, err
	}
	if !found || desired.Record.EnvironmentID != intent.EnvironmentID ||
		desired.Record.RevisionID != intent.Authored.ParentTaskID ||
		desired.Record.RenderGeneration != intent.Authored.RenderGeneration {
		return nil, nil, errs.New(errs.KindStateConflict, "Blueprint Entry desired revision changed")
	}
	keys := []string{
		entryrecord.RecordKey(intent.EntryID),
		entryrecord.EntryOwnerKey(intent.EnvironmentID, intent.EntryID),
		entryrecord.BlueprintEntryEnvironmentPrefix + intent.EntryID,
		projectionrecord.EnvironmentComposeProjectionStorageKey(intent.EnvironmentID),
		deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetEntry), intent.EntryID),
		environmentchanges.ComponentTaskActiveEnvironmentKey(intent.EnvironmentID),
		taskjournal.TaskMaterializationWriterKey(intent.EnvironmentID),
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) ||
		read.Values[0] == nil || read.Values[1] == nil || read.Values[2] == nil || read.Values[3] == nil {
		return nil, nil, errs.New(errs.KindStateConflict, "Blueprint Entry removal authority is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	for _, value := range read.Values[4:] {
		if value != nil {
			return nil, nil, errs.New(
				errs.KindResourceInUse, "Blueprint Entry cleanup overlaps an active mutation",
			)
		}
	}
	entry, entryErr := entryrecord.DecodeRecord(read.Values[0].Value)
	applied, appliedErr := projectionrecord.DecodeEnvironmentComposeProjectionStorage(read.Values[3].Value)
	if entryErr != nil || appliedErr != nil || read.Values[0].ModRevision != intent.EntryRevision ||
		entry.EnvironmentID != intent.EnvironmentID || entry.Entry.ID != intent.EntryID ||
		string(read.Values[1].Value) != intent.EntryID || string(read.Values[2].Value) != intent.EnvironmentID ||
		read.Values[3].ModRevision != intent.CurrentProjectionRevision ||
		!environmentchanges.SameEntryRemovalProjection(applied, *intent.CurrentProjection) {
		return nil, nil, errs.New(errs.KindStateConflict, "Blueprint Entry removal predecessor changed")
	}
	if _, declared := desired.Record.Input.Entries[entry.BlueprintKey]; declared {
		return nil, nil, errs.New(errs.KindStateConflict, "Blueprint Entry is still declared")
	}
	scriptConditions, err := entryrecord.PrepareEntryScriptAbsence(ctx, repository.store, intent.EntryID, revision)
	if err != nil {
		return nil, nil, err
	}
	fence, err := environmentfence.LoadOrdinary(ctx, repository.store, intent.EnvironmentID, revision)
	if err != nil {
		return nil, nil, err
	}
	epoch, err := fence.EpochRewriteMutation()
	if err != nil {
		return nil, nil, err
	}
	tombstone, err := deletionrecord.EncodeDeletionTombstone(deletionrecord.DeletionTombstoneRecord{
		TargetKind: deletionrecord.DeletionTargetEntry, TargetID: intent.EntryID,
		TargetRevision: intent.EntryRevision, TaskID: task.ID, Phase: entryRemovalTombstonePhase(intent),
		CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt,
	})
	if err != nil {
		clear(epoch.Value)
		return nil, nil, err
	}
	encodedIntent, err := environmentchanges.EncodeEntryRemovalIntent(intent)
	if err != nil {
		clear(epoch.Value)
		clear(tombstone)
		return nil, nil, err
	}
	conditions := []keyvalue.Condition{
		{
			Key:         blueprints.EnvironmentBlueprintRootKey(intent.EnvironmentID, intent.Authored.ParentTaskID),
			ModRevision: desired.Revision,
		},
		{Key: environmentchanges.EntryRemovalIntentKey(task.ID)},
	}
	for index, key := range keys {
		conditions = append(
			conditions,
			keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(read.Values[index])},
		)
	}
	conditions = append(conditions, scriptConditions...)
	conditions = append(conditions, fence.TransactionConditions()...)
	mutations := []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: keys[4], Value: tombstone},
		{Type: keyvalue.MutationPut, Key: environmentchanges.EntryRemovalIntentKey(task.ID), Value: encodedIntent},
		{Type: keyvalue.MutationPut, Key: keys[5], Value: []byte(task.ID)},
		epoch,
	}
	return conditions, mutations, nil
}
