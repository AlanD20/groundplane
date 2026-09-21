package runners

import (
	"context"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
)

func (repository *Repository) PutRunnerObservation(
	ctx context.Context,
	record RunnerObservationRecord,
	expectedRevision int64,
) (etcdstore.Versioned[RunnerObservationRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[RunnerObservationRecord]{}, err
	}
	if err := ValidateRunnerObservation(record); err != nil {
		return etcdstore.Versioned[RunnerObservationRecord]{}, err
	}
	current, err := repository.GetRunner(ctx, record.RunnerID)
	if err != nil {
		return etcdstore.Versioned[RunnerObservationRecord]{}, err
	}
	parents, err := repository.ResolveRunnerParents(ctx, current.Record.Desired)
	if err != nil {
		return etcdstore.Versioned[RunnerObservationRecord]{}, err
	}
	observation, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{RunnerObservationKey(record.RunnerID)},
	})
	if err != nil {
		return etcdstore.Versioned[RunnerObservationRecord]{}, err
	}
	if observation == nil || len(observation.Values) != 1 ||
		etcdstore.RevisionChanged(observation.Values[0], expectedRevision) {
		return etcdstore.Versioned[RunnerObservationRecord]{}, recordcodec.StateConflict(
			"runner observation",
			record.RunnerID,
		)
	}
	value, err := EncodeRunnerObservation(record)
	if err != nil {
		return etcdstore.Versioned[RunnerObservationRecord]{}, err
	}
	defer clear(value)
	conditions := []etcdstore.Condition{
		{Key: RunnerObservationKey(record.RunnerID), ModRevision: expectedRevision},
		{Key: RunnerKey(record.RunnerID), ModRevision: current.Revision},
		{Key: RunnerLifecycleKey(record.RunnerID), ModRevision: current.Record.LifecycleRevision},
		{Key: hierarchyrecord.TenantKey(current.Record.Desired.TenantID), ModRevision: parents.Tenant().Revision},
		{Key: deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetRunner), record.RunnerID)},
		{
			Key: deletionrecord.TombstoneKey(
				string(deletionrecord.DeletionTargetTenant),
				current.Record.Desired.TenantID,
			),
		},
	}
	if current.Record.Desired.OwnerKind == RunnerOwnerProject {
		conditions = append(
			conditions,
			etcdstore.Condition{
				Key:         hierarchyrecord.ProjectKey(current.Record.Desired.OwnerID),
				ModRevision: parents.Project().Revision,
			},
			etcdstore.Condition{
				Key: deletionrecord.TombstoneKey(
					string(deletionrecord.DeletionTargetProject),
					current.Record.Desired.OwnerID,
				),
			},
		)
	}
	result, err := repository.store.Transact(ctx, conditions, []etcdstore.Mutation{{
		Type: etcdstore.MutationPut, Key: RunnerObservationKey(record.RunnerID), Value: value,
	}})
	if err != nil {
		return etcdstore.Versioned[RunnerObservationRecord]{}, err
	}
	if !result.Succeeded {
		return etcdstore.Versioned[RunnerObservationRecord]{}, recordcodec.StateConflict(
			"runner observation",
			record.RunnerID,
		)
	}
	return etcdstore.Versioned[RunnerObservationRecord]{
		Record: record, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}
