package etcd

import (
	"context"
	environmentfence "github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

func (repository *TaskRepository) bindOrdinaryTaskEnvironmentMutation(
	ctx context.Context,
	task TaskRecord,
	readRevision int64,
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
	materializationChange bool,
	attachChange bool,
	entryChange bool,
	serviceChange bool,
	connectorChange bool,
) (*environmentfence.MutationBinding, error) {
	environmentID, applies, err := ordinaryTaskEnvironmentMutationTarget(
		task, materializationChange, attachChange, entryChange, serviceChange, connectorChange,
	)
	if err != nil || !applies {
		return nil, err
	}
	fence, err := environmentfence.LoadOrdinary(ctx, repository.store, environmentID, readRevision)
	if err != nil {
		return nil, err
	}
	if attachChange {
		gateConditions, gateMutations, gateErr :=
			repository.prepareBlueprintRequirementGatePrerequisiteAcknowledgement(
				ctx, task, fence, readRevision,
			)
		if gateErr != nil {
			return nil, gateErr
		}
		conditions = append(conditions, gateConditions...)
		mutations = append(mutations, gateMutations...)
	}
	advanceEpoch, err := blueprintCandidateShouldAdvanceEpoch(task, environmentID, mutations)
	if err != nil {
		return nil, err
	}
	return bindTaskLifecycleEnvironment(ctx, fence.MutationContext(), repository.store, task, conditions, mutations, advanceEpoch)
}

func prepareTerminalTaskMarker(
	task TaskRecord,
	status taskjournal.TaskStatus,
	terminalAt time.Time,
) (idempotencyrecord.IdempotencyMarker, string, string, error) {
	if task.idempotencyMarker == nil {
		return idempotencyrecord.IdempotencyMarker{}, "", "", errs.New(
			errs.KindInternal,
			"task is missing its idempotency marker locator",
		)
	}
	markerKey, err := idempotencyrecord.IdempotencyMarkerKey(*task.idempotencyMarker)
	if err != nil {
		return idempotencyrecord.IdempotencyMarker{}, "", "", errs.New(errs.KindInternal, "task idempotency marker locator is corrupt")
	}
	state := idempotencyrecord.IdempotencyMarkerFailed
	if status == taskjournal.TaskStatusCompleted {
		state = idempotencyrecord.IdempotencyMarkerCompleted
	}
	marker := idempotencyrecord.IdempotencyMarker{
		Kind: idempotencyrecord.IdempotencyMarkerTask, State: state, Locator: *task.idempotencyMarker,
		TaskID: task.ID, UpdatedAt: terminalAt, TerminalAt: terminalAt,
		RetainUntil: terminalAt.Add(idempotencyrecord.MarkerRetention),
	}
	retentionKey, err := idempotencyrecord.IdempotencyRetentionKey(markerKey, marker.RetainUntil)
	if err != nil {
		return idempotencyrecord.IdempotencyMarker{}, "", "", err
	}
	return marker, markerKey, retentionKey, nil
}
