package etcd

import (
	"context"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	runnerrecord "github.com/AlanD20/groundplane/internal/infra/etcd/runners"
)

func (repository *RunnerRepository) PutRunnerObservation(
	ctx context.Context,
	record runnerrecord.RunnerObservationRecord,
	expectedRevision int64,
) (etcdstore.Versioned[runnerrecord.RunnerObservationRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[runnerrecord.RunnerObservationRecord]{}, err
	}
	if err := runnerrecord.ValidateRunnerObservation(record); err != nil {
		return etcdstore.Versioned[runnerrecord.RunnerObservationRecord]{}, err
	}
	current, err := repository.GetRunner(ctx, record.RunnerID)
	if err != nil {
		return etcdstore.Versioned[runnerrecord.RunnerObservationRecord]{}, err
	}
	parents, err := repository.resolveRunnerParents(ctx, current.Record.Desired)
	if err != nil {
		return etcdstore.Versioned[runnerrecord.RunnerObservationRecord]{}, err
	}
	observation, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{runnerrecord.RunnerObservationKey(record.RunnerID)},
	})
	if err != nil {
		return etcdstore.Versioned[runnerrecord.RunnerObservationRecord]{}, err
	}
	if observation == nil || len(observation.Values) != 1 ||
		revisionChanged(observation.Values[0], expectedRevision) {
		return etcdstore.Versioned[runnerrecord.RunnerObservationRecord]{}, recordcodec.StateConflict("runner observation", record.RunnerID)
	}
	value, err := runnerrecord.EncodeRunnerObservation(record)
	if err != nil {
		return etcdstore.Versioned[runnerrecord.RunnerObservationRecord]{}, err
	}
	defer clear(value)
	conditions := []etcdstore.Condition{
		{Key: runnerrecord.RunnerObservationKey(record.RunnerID), ModRevision: expectedRevision},
		{Key: runnerrecord.RunnerKey(record.RunnerID), ModRevision: current.Revision},
		{Key: runnerrecord.RunnerLifecycleKey(record.RunnerID), ModRevision: current.Record.LifecycleRevision},
		{Key: hierarchyrecord.TenantKey(current.Record.Desired.TenantID), ModRevision: parents.tenant.Revision},
		{Key: deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetRunner), record.RunnerID)},
		{Key: deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetTenant), current.Record.Desired.TenantID)},
	}
	if current.Record.Desired.OwnerKind == runnerrecord.RunnerOwnerProject {
		conditions = append(conditions,
			etcdstore.Condition{Key: hierarchyrecord.ProjectKey(current.Record.Desired.OwnerID), ModRevision: parents.project.Revision},
			etcdstore.Condition{Key: deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetProject), current.Record.Desired.OwnerID)},
		)
	}
	result, err := repository.store.Transact(ctx, conditions, []etcdstore.Mutation{{
		Type: etcdstore.MutationPut, Key: runnerrecord.RunnerObservationKey(record.RunnerID), Value: value,
	}})
	if err != nil {
		return etcdstore.Versioned[runnerrecord.RunnerObservationRecord]{}, err
	}
	if !result.Succeeded {
		return etcdstore.Versioned[runnerrecord.RunnerObservationRecord]{}, recordcodec.StateConflict("runner observation", record.RunnerID)
	}
	return etcdstore.Versioned[runnerrecord.RunnerObservationRecord]{
		Record: record, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}
