package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	runnerrecord "github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *RunnerRepository) PutRunnerObservation(
	ctx context.Context,
	record runnerrecord.RunnerObservationRecord,
	expectedRevision int64,
) (Versioned[runnerrecord.RunnerObservationRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[runnerrecord.RunnerObservationRecord]{}, err
	}
	if err := runnerrecord.ValidateRunnerObservation(record); err != nil {
		return Versioned[runnerrecord.RunnerObservationRecord]{}, err
	}
	current, err := repository.GetRunner(ctx, record.RunnerID)
	if err != nil {
		return Versioned[runnerrecord.RunnerObservationRecord]{}, err
	}
	parents, err := repository.resolveRunnerParents(ctx, current.Record.Desired)
	if err != nil {
		return Versioned[runnerrecord.RunnerObservationRecord]{}, err
	}
	observation, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{runnerObservationKey(record.RunnerID)},
	})
	if err != nil {
		return Versioned[runnerrecord.RunnerObservationRecord]{}, err
	}
	if observation == nil || len(observation.Values) != 1 ||
		revisionChanged(observation.Values[0], expectedRevision) {
		return Versioned[runnerrecord.RunnerObservationRecord]{}, stateConflict("runner observation", record.RunnerID)
	}
	value, err := runnerrecord.EncodeRunnerObservation(record)
	if err != nil {
		return Versioned[runnerrecord.RunnerObservationRecord]{}, err
	}
	defer clear(value)
	conditions := []etcdstore.Condition{
		{Key: runnerObservationKey(record.RunnerID), ModRevision: expectedRevision},
		{Key: runnerKey(record.RunnerID), ModRevision: current.Revision},
		{Key: runnerLifecycleKey(record.RunnerID), ModRevision: current.Record.LifecycleRevision},
		{Key: hierarchyrecord.TenantKey(current.Record.Desired.TenantID), ModRevision: parents.tenant.Revision},
		{Key: deletionTombstoneKey(string(DeletionTargetRunner), record.RunnerID)},
		{Key: deletionTombstoneKey(string(DeletionTargetTenant), current.Record.Desired.TenantID)},
	}
	if current.Record.Desired.OwnerKind == runnerrecord.RunnerOwnerProject {
		conditions = append(conditions,
			etcdstore.Condition{Key: hierarchyrecord.ProjectKey(current.Record.Desired.OwnerID), ModRevision: parents.project.Revision},
			etcdstore.Condition{Key: deletionTombstoneKey(string(DeletionTargetProject), current.Record.Desired.OwnerID)},
		)
	}
	result, err := repository.store.Transact(ctx, conditions, []etcdstore.Mutation{{
		Type: etcdstore.MutationPut, Key: runnerObservationKey(record.RunnerID), Value: value,
	}})
	if err != nil {
		return Versioned[runnerrecord.RunnerObservationRecord]{}, err
	}
	if !result.Succeeded {
		return Versioned[runnerrecord.RunnerObservationRecord]{}, stateConflict("runner observation", record.RunnerID)
	}
	return Versioned[runnerrecord.RunnerObservationRecord]{
		Record: record, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}

func (repository *RunnerRepository) GetRunnerObservation(
	ctx context.Context,
	runnerID string,
) (Versioned[runnerrecord.RunnerObservationRecord], bool, error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[runnerrecord.RunnerObservationRecord]{}, false, err
	}
	if err := recordcodec.ValidateID(ids.KindRunner, runnerID); err != nil {
		return Versioned[runnerrecord.RunnerObservationRecord]{}, false, err
	}
	result, err := repository.store.Get(ctx, runnerObservationKey(runnerID))
	if err != nil {
		return Versioned[runnerrecord.RunnerObservationRecord]{}, false, err
	}
	if result == nil {
		return Versioned[runnerrecord.RunnerObservationRecord]{}, false, errs.New(
			errs.KindInternal,
			"runner observation read is empty",
		)
	}
	if result.Entry == nil {
		return Versioned[runnerrecord.RunnerObservationRecord]{ReadRevision: result.ReadRevision}, false, nil
	}
	record, err := runnerrecord.DecodeRunnerObservation(result.Entry.Value)
	if err != nil || record.RunnerID != runnerID {
		return Versioned[runnerrecord.RunnerObservationRecord]{}, false, errs.New(errs.KindInternal, "runner observation is corrupt")
	}
	return Versioned[runnerrecord.RunnerObservationRecord]{
		Record: record, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, true, nil
}
