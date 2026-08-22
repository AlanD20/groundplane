package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type backingZoneTaskChange struct {
	applies    bool
	conditions []Condition
	mutations  []Mutation
	values     [][]byte
}

func (repository *TaskRepository) prepareBackingZoneTaskRetry(
	ctx context.Context,
	source TaskRecord,
	retry TaskRecord,
	revision int64,
) (backingZoneTaskChange, error) {
	applies, err := taskOwnsBackingZoneCascade(source)
	if err != nil || !applies {
		return backingZoneTaskChange{}, err
	}
	if retry.Type != source.Type || retry.Target != source.Target ||
		retry.Params[TaskResourceKindParam] != TaskResourceBackingZone {
		return backingZoneTaskChange{}, errs.New(errs.KindInternal, "backing Zone retry changed its durable target")
	}
	state, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{
		zoneKey(source.Target), deletionTombstoneKey(string(DeletionTargetZone), source.Target),
	}, Revision: revision})
	if err != nil {
		return backingZoneTaskChange{}, err
	}
	if state == nil || len(state.Values) != 2 || state.Values[0] == nil || state.Values[1] != nil {
		return backingZoneTaskChange{}, errs.New(
			errs.KindStateConflict,
			"backing Zone is not available for cascade retry",
		)
	}
	zone, err := decodeZoneRecord(state.Values[0].Value)
	if err != nil || zone.Desired.ID != source.Target ||
		zone.EnvironmentID != source.Params[TaskZoneEnvironmentParam] ||
		zone.Desired.OwnerKind != core.ZoneOwnerBackingProject {
		return backingZoneTaskChange{}, errs.New(errs.KindStateConflict, "backing Zone retry target changed")
	}
	tombstone := DeletionTombstoneRecord{
		TargetKind: DeletionTargetZone, TargetID: source.Target, TargetRevision: state.Values[0].ModRevision,
		TaskID: retry.ID, Phase: DeletionPhaseHostEffects, CreatedAt: retry.CreatedAt, UpdatedAt: retry.CreatedAt,
	}
	value, err := encodeDeletionTombstone(tombstone)
	if err != nil {
		return backingZoneTaskChange{}, err
	}
	return backingZoneTaskChange{
		applies: true,
		conditions: []Condition{
			{Key: zoneKey(source.Target), ModRevision: state.Values[0].ModRevision},
			{Key: deletionTombstoneKey(string(DeletionTargetZone), source.Target)},
		},
		mutations: []Mutation{{
			Type: MutationPut, Key: deletionTombstoneKey(string(DeletionTargetZone), source.Target), Value: value,
		}},
		values: [][]byte{value},
	}, nil
}

func (repository *TaskRepository) prepareBackingZoneTaskAcknowledgement(
	ctx context.Context,
	task TaskRecord,
	terminalStatus TaskStatus,
	revision int64,
) (backingZoneTaskChange, error) {
	applies, err := taskOwnsBackingZoneCascade(task)
	if err != nil || !applies {
		return backingZoneTaskChange{}, err
	}
	state, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{
		zoneKey(task.Target), deletionTombstoneKey(string(DeletionTargetZone), task.Target),
	}, Revision: revision})
	if err != nil {
		return backingZoneTaskChange{}, err
	}
	if state == nil || len(state.Values) != 2 {
		return backingZoneTaskChange{}, errs.New(errs.KindInternal, "backing Zone cascade state is incomplete")
	}
	if terminalStatus == TaskStatusCompleted {
		if state.Values[0] != nil || state.Values[1] != nil {
			return backingZoneTaskChange{}, errs.New(
				errs.KindStateConflict,
				"completed backing Zone cascade retained durable state",
			)
		}
		return backingZoneTaskChange{applies: true}, nil
	}
	if state.Values[0] == nil {
		return backingZoneTaskChange{}, errs.New(errs.KindStateConflict, "failed backing Zone cascade lost its target")
	}
	zone, err := decodeZoneRecord(state.Values[0].Value)
	if err != nil || zone.Desired.ID != task.Target || zone.Desired.OwnerKind != core.ZoneOwnerBackingProject {
		return backingZoneTaskChange{}, errs.New(
			errs.KindStateConflict,
			"failed backing Zone cascade retained another target",
		)
	}
	change := backingZoneTaskChange{applies: true}
	if state.Values[1] == nil {
		return change, nil
	}
	tombstone, err := decodeDeletionTombstone(state.Values[1].Value)
	if err != nil || tombstone.TaskID != task.ID || tombstone.TargetID != task.Target {
		return backingZoneTaskChange{}, errs.New(
			errs.KindStateConflict,
			"backing Zone cascade fence belongs to another Task",
		)
	}
	change.conditions = []Condition{{
		Key: deletionTombstoneKey(string(DeletionTargetZone), task.Target), ModRevision: state.Values[1].ModRevision,
	}}
	change.mutations = []Mutation{{
		Type: MutationDelete, Key: deletionTombstoneKey(string(DeletionTargetZone), task.Target),
	}}
	return change, nil
}

func (repository *TaskRepository) validateBackingZoneTaskAcknowledgementReplay(
	ctx context.Context,
	task TaskRecord,
	terminalStatus TaskStatus,
	revision int64,
) error {
	applies, err := taskOwnsBackingZoneCascade(task)
	if err != nil || !applies {
		return err
	}
	state, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{
		zoneKey(task.Target), deletionTombstoneKey(string(DeletionTargetZone), task.Target),
	}, Revision: revision})
	if err != nil {
		return err
	}
	if state == nil || len(state.Values) != 2 || state.Values[1] != nil {
		return errs.New(errs.KindStateConflict, "backing Zone cascade terminal state does not match its Task")
	}
	if terminalStatus == TaskStatusCompleted {
		if state.Values[0] != nil {
			return errs.New(errs.KindStateConflict, "completed backing Zone cascade retained its target")
		}
		return nil
	}
	if state.Values[0] == nil {
		return errs.New(errs.KindStateConflict, "failed backing Zone cascade lost its target")
	}
	return nil
}

func taskOwnsBackingZoneCascade(task TaskRecord) (bool, error) {
	if task.Executor != TaskExecutorController || task.Params[TaskResourceKindParam] != TaskResourceBackingZone {
		return false, nil
	}
	if task.Type != TaskRemove || ids.Validate(ids.KindNetwork, task.Target) != nil || len(task.Params) != 3 ||
		ids.Validate(ids.KindEnvironment, task.Params[TaskZoneEnvironmentParam]) != nil ||
		!validSHA256(task.Params[TaskZoneImpactTokenParam]) {
		return false, errs.New(errs.KindInternal, "backing Zone cascade Task has invalid durable input")
	}
	return true, nil
}

func clearBackingZoneTaskChange(change backingZoneTaskChange) {
	for _, value := range change.values {
		clear(value)
	}
}
