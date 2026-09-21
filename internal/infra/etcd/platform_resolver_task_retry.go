package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	platformcomponents "github.com/AlanD20/groundplane/internal/infra/etcd/platformcomponents"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// preparePlatformDNSResolverTaskRetry reuses the exact immutable plan input
// and only transfers the private active-attempt fence to the new Task. The
// input's TaskID remains the originating system attempt as durable provenance.
func (repository *TaskRepository) preparePlatformDNSResolverTaskRetry(
	ctx context.Context,
	source etcdstore.Versioned[TaskRecord],
	retry TaskRecord,
) (hostResolutionReconciliationChange, error) {
	if !isPlatformDNSResolverTaskAttempt(source.Record) {
		return hostResolutionReconciliationChange{}, nil
	}
	if source.ReadRevision <= 0 || source.Revision <= 0 || retry.RetryOf != source.Record.ID ||
		retry.Actor != taskjournal.TaskActorOperator || retry.PlanID != source.Record.PlanID ||
		retry.PlanHash != source.Record.PlanHash || retry.Target != source.Record.Target ||
		retry.Params[TaskPlatformComponentDesiredSHA256Param] !=
			source.Record.Params[TaskPlatformComponentDesiredSHA256Param] {
		return hostResolutionReconciliationChange{}, nil
	}
	state, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			platformcomponents.PlatformComponentTaskRenderInputKey(source.Record.PlanID),
			platformComponentTaskActiveKey(source.Record.Target),
		},
		Revision: source.ReadRevision,
	})
	if err != nil {
		return hostResolutionReconciliationChange{}, err
	}
	if state == nil || state.ReadRevision != source.ReadRevision || len(state.Values) != 2 || state.Values[0] == nil {
		return hostResolutionReconciliationChange{}, errs.New(
			errs.KindStateConflict,
			"platform resolver retry input is missing",
		)
	}
	input, err := platformcomponents.DecodePlatformComponentTaskRenderInput(state.Values[0].Value)
	if err != nil {
		return hostResolutionReconciliationChange{}, err
	}
	if input.PlanID != source.Record.PlanID || input.ComponentID != source.Record.Target ||
		input.ExecutionPlanSHA256 != source.Record.PlanHash || input.ExecutionPlanSHA256 != retry.PlanHash ||
		input.DesiredSHA256 != source.Record.Params[TaskPlatformComponentDesiredSHA256Param] {
		return hostResolutionReconciliationChange{}, errs.New(
			errs.KindStateConflict,
			"platform resolver retry changed its pinned input",
		)
	}
	origin := source
	if input.TaskID != source.Record.ID {
		originRead, readErr := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
			Keys: []string{taskjournal.TaskStorageKey(input.TaskID)}, Revision: source.ReadRevision,
		})
		if readErr != nil {
			return hostResolutionReconciliationChange{}, readErr
		}
		if originRead == nil || originRead.ReadRevision != source.ReadRevision || len(originRead.Values) != 1 ||
			originRead.Values[0] == nil {
			return hostResolutionReconciliationChange{}, errs.New(
				errs.KindStateConflict,
				"platform resolver retry origin is missing",
			)
		}
		originRecord, decodeErr := DecodeTaskRecord(originRead.Values[0].Value)
		if decodeErr != nil {
			return hostResolutionReconciliationChange{}, decodeErr
		}
		origin = etcdstore.Versioned[TaskRecord]{
			Record: originRecord, Revision: originRead.Values[0].ModRevision, ReadRevision: source.ReadRevision,
		}
	}
	if !isPlatformDNSResolverTaskAttempt(origin.Record) || origin.Record.ID != input.TaskID ||
		origin.Record.PlanID != input.PlanID || origin.Record.Target != input.ComponentID {
		return hostResolutionReconciliationChange{}, nil
	}
	if state.Values[1] != nil {
		return hostResolutionReconciliationChange{}, errs.New(
			errs.KindStateConflict,
			"platform resolver already has an active successor",
		)
	}
	conditions := []etcdstore.Condition{
		{
			Key:         platformcomponents.PlatformComponentTaskRenderInputKey(input.PlanID),
			ModRevision: state.Values[0].ModRevision,
		},
		{Key: platformComponentTaskActiveKey(input.ComponentID)},
	}
	if origin.Record.ID != source.Record.ID {
		conditions = append(
			conditions,
			etcdstore.Condition{Key: taskjournal.TaskStorageKey(origin.Record.ID), ModRevision: origin.Revision},
		)
	}
	value := []byte(retry.ID)
	return hostResolutionReconciliationChange{
		applies:    true,
		conditions: conditions,
		mutations: []etcdstore.Mutation{{
			Type: etcdstore.MutationPut, Key: platformComponentTaskActiveKey(input.ComponentID), Value: value,
		}},
		values: [][]byte{value},
	}, nil
}
