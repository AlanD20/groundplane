package etcd

import (
	"context"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"maps"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type backingZoneTaskChange struct {
	applies    bool
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
	values     [][]byte
}

func (repository *TaskRepository) prepareBackingZoneTaskRetry(
	ctx context.Context,
	source TaskRecord,
	retry TaskRecord,
	revision int64,
) (backingZoneTaskChange, error) {
	if source.Params[taskjournal.TaskZoneRemovalOperationParam] != "" {
		return repository.prepareZoneRemovalTaskRetry(ctx, source, retry, revision)
	}
	_, err := taskOwnsBackingZoneCascade(source)
	return backingZoneTaskChange{}, err
}

func (repository *TaskRepository) prepareZoneRemovalTaskRetry(
	ctx context.Context, source TaskRecord, retry TaskRecord, revision int64,
) (backingZoneTaskChange, error) {
	operationID := source.Params[taskjournal.TaskZoneRemovalOperationParam]
	if ids.Validate(ids.KindOperation, operationID) != nil || source.FinishedAt == nil || retry.RetryOf != source.ID ||
		retry.Executor != source.Executor || retry.Type != source.Type || retry.Target != source.Target ||
		retry.PlanID != source.PlanID || retry.PlanHash != source.PlanHash ||
		retry.RenderGeneration != source.RenderGeneration || !maps.Equal(retry.Params, source.Params) {
		return backingZoneTaskChange{}, errs.New(errs.KindStateConflict, "Zone removal retry changed its pinned Task")
	}
	keys := []string{
		zoneRemovalIntentKey(operationID), deletionTombstoneKey(string(deletionrecord.DeletionTargetZone), source.Target),
		blueprints.EnvironmentBlueprintHeadKey(source.Params[taskjournal.TaskZoneEnvironmentParam]),
		projectionrecord.EnvironmentComposeProjectionStorageKey(source.Params[taskjournal.TaskZoneEnvironmentParam]),
		componentTaskActiveEnvironmentKey(source.Params[taskjournal.TaskZoneEnvironmentParam]),
	}
	state, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return backingZoneTaskChange{}, err
	}
	if state == nil || len(state.Values) != len(keys) || state.Values[0] == nil || state.Values[1] != nil ||
		state.Values[2] == nil || state.Values[3] == nil || state.Values[4] != nil {
		return backingZoneTaskChange{}, errs.New(errs.KindStateConflict, "Zone is not available for removal retry")
	}
	intent, err := decodeZoneRemovalIntent(state.Values[0].Value)
	if err != nil || validateZoneRemovalTaskOwner(source, intent) != nil || intent.Status != source.Status ||
		intent.TerminalAt == nil || !intent.TerminalAt.Equal(*source.FinishedAt) ||
		state.Values[2].ModRevision != intent.DesiredHeadRevision ||
		state.Values[3].ModRevision != intent.AppliedProjectionRevision {
		return backingZoneTaskChange{}, errs.New(errs.KindStateConflict, "Zone removal retry authority changed")
	}
	headID, err := idempotencyrecord.DecodeTaskReference(state.Values[2].Value)
	if err != nil || headID != intent.DesiredProjection.RevisionID {
		return backingZoneTaskChange{}, errs.New(errs.KindStateConflict, "Zone removal retry head changed")
	}
	projection, decodeErr := projectionrecord.DecodeEnvironmentComposeProjectionStorage(state.Values[3].Value)
	if decodeErr != nil || !sameServiceRemovalProjection(projection, intent.AppliedProjection) {
		return backingZoneTaskChange{}, errs.New(errs.KindStateConflict, "Zone removal retry projection changed")
	}
	if _, err := projectedZoneRemovalTarget(intent, revision); err != nil {
		return backingZoneTaskChange{}, err
	}
	retryIntent, err := TransferZoneRemovalIntent(intent, retry.ID, retry.CreatedAt)
	if err != nil {
		return backingZoneTaskChange{}, err
	}
	if err := validateZoneRemovalTaskOwner(retry, retryIntent); err != nil {
		return backingZoneTaskChange{}, err
	}
	hierarchy := &HierarchyRepository{store: repository.store}
	publication, err := hierarchy.prepareEnvironmentDirectPublication(
		ctx,
		retryIntent.Claim,
		blueprints.EnvironmentDesiredRevisionIdentity{
			EnvironmentID: retryIntent.EnvironmentID,
			RevisionID:    retryIntent.Claim.RevisionID,
		},
		retryIntent.CandidateProjection,
		idempotencyrecord.IdempotencyMarker{
			Locator: retryIntent.Claim.Locator,
			Intent:  retryIntent.Claim.Intent,
		},
		retryIntent.DesiredHeadRevision,
	)
	if err != nil {
		return backingZoneTaskChange{}, err
	}
	intentValue, err := encodeZoneRemovalIntent(retryIntent)
	if err != nil {
		clear(publication.publishedDescriptor)
		return backingZoneTaskChange{}, err
	}
	tombstoneValue, err := deletionrecord.EncodeDeletionTombstone(deletionrecord.DeletionTombstoneRecord{
		TargetKind: deletionrecord.DeletionTargetZone, TargetID: retry.Target, TargetRevision: intent.ZoneRevision,
		TaskID: retry.ID, Phase: deletionrecord.DeletionPhaseHostEffects, CreatedAt: retry.CreatedAt, UpdatedAt: retry.CreatedAt,
	})
	if err != nil {
		clear(publication.publishedDescriptor)
		clear(intentValue)
		return backingZoneTaskChange{}, err
	}
	return backingZoneTaskChange{
		applies: true,
		conditions: []etcdstore.Condition{
			{Key: keys[0], ModRevision: state.Values[0].ModRevision},
			{Key: keys[1]},
			{Key: keys[2], ModRevision: state.Values[2].ModRevision},
			{Key: keys[3], ModRevision: state.Values[3].ModRevision},
			{Key: keys[4]},
			{
				Key:         blueprints.EnvironmentBlueprintRootKey(intent.EnvironmentID, intent.Claim.RevisionID),
				ModRevision: publication.rootRevision,
			},
			{Key: publication.descriptorKey, ModRevision: publication.descriptorRevision},
			{Key: publication.locatorKey, ModRevision: publication.locatorRevision},
		},
		mutations: []etcdstore.Mutation{
			{Type: etcdstore.MutationPut, Key: keys[0], Value: intentValue},
			{Type: etcdstore.MutationPut, Key: keys[1], Value: tombstoneValue},
			{Type: etcdstore.MutationPut, Key: keys[4], Value: []byte(retry.ID)},
		},
		values: [][]byte{intentValue, tombstoneValue, publication.publishedDescriptor},
	}, nil
}

func (repository *TaskRepository) prepareBackingZoneTaskAcknowledgement(
	ctx context.Context,
	task TaskRecord,
	terminalStatus taskjournal.TaskStatus,
	terminalAt time.Time,
	revision int64,
) (backingZoneTaskChange, error) {
	if task.Params[taskjournal.TaskZoneRemovalOperationParam] != "" {
		conditions, mutations, err := repository.prepareZoneRemovalAcknowledgement(
			ctx, task, terminalStatus, terminalAt, revision,
		)
		return backingZoneTaskChange{applies: true, conditions: conditions, mutations: mutations}, err
	}
	_, err := taskOwnsBackingZoneCascade(task)
	return backingZoneTaskChange{}, err
}

func (repository *TaskRepository) validateBackingZoneTaskAcknowledgementReplay(
	ctx context.Context,
	task TaskRecord,
	terminalStatus taskjournal.TaskStatus,
	revision int64,
) error {
	if task.Params[taskjournal.TaskZoneRemovalOperationParam] != "" {
		return repository.validateZoneRemovalReplay(ctx, task, terminalStatus, revision)
	}
	_, err := taskOwnsBackingZoneCascade(task)
	return err
}

func taskOwnsBackingZoneCascade(task TaskRecord) (bool, error) {
	if task.Executor != taskjournal.TaskExecutorController || task.Params[taskjournal.TaskResourceKindParam] != taskjournal.TaskResourceBackingZone {
		return false, nil
	}
	if task.Type != taskjournal.TaskRemove || ids.Validate(ids.KindNetwork, task.Target) != nil || len(task.Params) != 5 ||
		ids.Validate(ids.KindEnvironment, task.Params[taskjournal.TaskZoneEnvironmentParam]) != nil ||
		ids.Validate(ids.KindOperation, task.Params[taskjournal.TaskZoneRemovalOperationParam]) != nil ||
		ids.Validate(ids.KindTask, task.Params[blueprints.EnvironmentDesiredRevisionParam]) != nil ||
		!recordcodec.ValidSHA256(task.Params[taskjournal.TaskZoneImpactTokenParam]) {
		return false, errs.New(errs.KindInternal, "backing Zone cascade Task has invalid durable input")
	}
	return true, nil
}

func clearBackingZoneTaskChange(change backingZoneTaskChange) {
	for _, value := range change.values {
		clear(value)
	}
}
