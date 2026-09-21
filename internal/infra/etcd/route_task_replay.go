package etcd

import (
	"context"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) validateRouteTaskAcknowledgementReplay(
	ctx context.Context,
	task TaskRecord,
	terminalStatus taskjournal.TaskStatus,
	revision int64,
) error {
	matched, err := repository.validateRouteMutationTaskAcknowledgementReplay(ctx, task, terminalStatus, revision)
	if err != nil || matched {
		return err
	}
	intentRead, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{routeRemovalIntentKey(task.ID)}, Revision: revision,
	})
	if err != nil {
		return err
	}
	if intentRead == nil || len(intentRead.Values) != 1 {
		return errs.New(errs.KindInternal, "Route removal replay read is incomplete")
	}
	if intentRead.Values[0] == nil {
		if terminalStatus == taskjournal.TaskStatusCompleted {
			return validateCompletedRouteHeadReplay(ctx, repository.store, task, revision)
		}
		return nil
	}
	intent, err := decodeRouteRemovalIntent(intentRead.Values[0].Value)
	if err != nil {
		return err
	}
	if err := validateRouteRemovalTaskOwner(task, intent); err != nil {
		return err
	}
	if intent.Status != terminalStatus || intent.TerminalAt == nil || task.FinishedAt == nil ||
		!intent.TerminalAt.Equal(*task.FinishedAt) {
		return errs.New(errs.KindStateConflict, "Route removal intent does not match terminal Task")
	}
	state, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			deletionTombstoneKey(string(deletionrecord.DeletionTargetRoute), intent.RouteID),
			componentTaskActiveEnvironmentKey(intent.EnvironmentID),
		},
		Revision: revision,
	})
	if err != nil {
		return err
	}
	if state == nil || len(state.Values) != 2 || state.Values[0] != nil || state.Values[1] != nil {
		return errs.New(errs.KindStateConflict, "Route removal terminal fence is inconsistent")
	}
	if intent.CandidateProjection == nil {
		return errs.New(errs.KindStateConflict, "Route removal terminal projection is missing")
	}
	if terminalStatus == taskjournal.TaskStatusCompleted {
		return errs.New(errs.KindStateConflict, "completed Route removal retained its active intent")
	}
	staging, err := prepareRouteHeadCandidate(ctx, repository.store, intent, revision)
	if err != nil {
		return err
	}
	clearRouteHeadPublication(staging)
	projection, found, projectionErr := currentEnvironmentProjectionAtRevision(
		ctx, repository.store, intent.EnvironmentID, revision,
	)
	if projectionErr != nil || !found || intent.CurrentProjection == nil ||
		!sameRouteRemovalProjection(projection.Record, *intent.CurrentProjection) {
		return errs.New(errs.KindStateConflict, "Route removal terminal projection changed")
	}
	return nil
}

func (repository *TaskRepository) validateRouteMutationTaskAcknowledgementReplay(
	ctx context.Context,
	task TaskRecord,
	terminalStatus taskjournal.TaskStatus,
	revision int64,
) (bool, error) {
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{routeMutationIntentKey(task.ID)}, Revision: revision,
	})
	if err != nil {
		return false, err
	}
	if read == nil || len(read.Values) != 1 {
		return false, errs.New(errs.KindInternal, "Route mutation replay read is incomplete")
	}
	if read.Values[0] == nil {
		return false, nil
	}
	intent, err := decodeRouteMutationIntent(read.Values[0].Value)
	if err != nil || validateRouteMutationTaskOwner(task, intent) != nil || intent.Status != terminalStatus ||
		intent.TerminalAt == nil || task.FinishedAt == nil || !intent.TerminalAt.Equal(*task.FinishedAt) {
		return true, errs.New(errs.KindStateConflict, "Route mutation replay evidence changed")
	}
	stateKeys := []string{componentTaskActiveEnvironmentKey(intent.EnvironmentID)}
	state, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: stateKeys, Revision: revision})
	if err != nil {
		return true, err
	}
	if state == nil || len(state.Values) != len(stateKeys) || state.Values[0] != nil {
		return true, errs.New(errs.KindStateConflict, "Route mutation replay state is incomplete")
	}
	projection, found, projectionErr := currentEnvironmentProjectionAtRevision(
		ctx,
		repository.store,
		intent.EnvironmentID,
		revision,
	)
	if projectionErr != nil || !found || !routeMutationSelectedProjection(projection.Record, task, intent) {
		return true, errs.New(errs.KindStateConflict, "Route mutation terminal projection changed")
	}
	return true, nil
}
