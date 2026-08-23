package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type environmentTaskChange struct {
	applies    bool
	conditions []Condition
	mutations  []Mutation
	values     [][]byte
}

func (repository *TaskRepository) prepareEnvironmentTaskRetry(
	ctx context.Context,
	source TaskRecord,
	retry TaskRecord,
	revision int64,
) (environmentTaskChange, error) {
	if source.Executor != TaskExecutorAgent || source.Type != TaskCreate ||
		ids.Validate(ids.KindEnvironment, source.Target) != nil {
		return environmentTaskChange{}, nil
	}
	if retry.Type != source.Type || retry.Target != source.Target {
		return environmentTaskChange{}, errs.New(
			errs.KindInternal,
			"Environment retry changed its durable target",
		)
	}
	current, err := repository.readTaskEnvironment(ctx, source.Target, revision)
	if err != nil {
		return environmentTaskChange{}, err
	}
	if current.Record.CreateTaskID != source.ID {
		return environmentTaskChange{}, errs.New(
			errs.KindStateConflict,
			"source Task no longer owns Environment provisioning",
		)
	}
	retrying, err := RetryEnvironmentProvisioning(current.Record, retry.ID)
	if err != nil {
		return environmentTaskChange{}, err
	}
	value, err := encodeEnvironment(retrying)
	if err != nil {
		return environmentTaskChange{}, err
	}
	return environmentTaskChange{
		applies:    true,
		conditions: []Condition{{Key: environmentKey(source.Target), ModRevision: current.Revision}},
		mutations: []Mutation{{
			Type: MutationPut, Key: environmentKey(source.Target), Value: value,
		}},
		values: [][]byte{value},
	}, nil
}

func (repository *TaskRepository) readTaskEnvironment(
	ctx context.Context,
	environmentID string,
	revision int64,
) (Versioned[EnvironmentRecord], error) {
	result, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{environmentKey(environmentID)}, Revision: revision,
	})
	if err != nil {
		return Versioned[EnvironmentRecord]{}, err
	}
	if len(result.Values) != 1 {
		clearKeyValues(result.Values)
		return Versioned[EnvironmentRecord]{}, errs.New(
			errs.KindInternal,
			"Environment retry read returned an invalid result",
		)
	}
	value := result.Values[0]
	if value == nil {
		return Versioned[EnvironmentRecord]{}, errs.New(
			errs.KindEnvironmentNotFound,
			"Environment was not found",
		)
	}
	defer clear(value.Value)
	record, err := decodeEnvironment(value.Value)
	if err != nil {
		return Versioned[EnvironmentRecord]{}, err
	}
	if record.ID != environmentID {
		return Versioned[EnvironmentRecord]{}, errs.New(
			errs.KindInternal,
			"Environment retry read returned the wrong record",
		)
	}
	return Versioned[EnvironmentRecord]{
		Record: record, Revision: value.ModRevision, ReadRevision: result.ReadRevision,
	}, nil
}

func clearEnvironmentTaskChange(change environmentTaskChange) {
	for _, value := range change.values {
		clear(value)
	}
}
