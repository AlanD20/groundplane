package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type serviceTaskChange struct {
	applies    bool
	conditions []Condition
	mutations  []Mutation
}

func (repository *TaskRepository) prepareServiceTaskRetry(
	ctx context.Context,
	source TaskRecord,
	retry TaskRecord,
	readRevision int64,
) (serviceTaskChange, error) {
	if !isServiceLifecycleTask(source) {
		return serviceTaskChange{}, nil
	}
	service, err := findServiceAtRevision(ctx, repository.store, source.Target, readRevision)
	if err != nil {
		return serviceTaskChange{}, err
	}
	values, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{
		serviceLifecycleActiveKey(source.Target),
		serviceLifecycleRenderInputKey(source.ID),
		serviceLifecycleRenderInputKey(retry.ID),
	}, Revision: readRevision})
	if err != nil {
		return serviceTaskChange{}, err
	}
	if values == nil || len(values.Values) != 3 {
		return serviceTaskChange{}, errs.New(errs.KindInternal, "Service retry evidence is incomplete")
	}
	if service.Record.Runtime.RuntimeIntent != serviceLifecycleIntent(source.Type) {
		return serviceTaskChange{}, errs.New(errs.KindStateConflict, "Service runtime intent changed before retry")
	}
	if values.Values[0] != nil {
		return serviceTaskChange{}, errs.New(errs.KindResourceInUse, "Service already has an active lifecycle Task")
	}
	if values.Values[2] != nil {
		return serviceTaskChange{}, errs.New(errs.KindInternal, "Service retry render input already exists")
	}
	reference, err := encodeTaskReference(retry.ID)
	if err != nil {
		return serviceTaskChange{}, err
	}
	change := serviceTaskChange{
		applies: true,
		conditions: []Condition{
			serviceDesiredCondition(service),
			serviceRuntimeCondition(service),
			{Key: serviceLifecycleActiveKey(source.Target)},
		},
		mutations: []Mutation{{Type: MutationPut, Key: serviceLifecycleActiveKey(source.Target), Value: reference}},
	}
	if source.Executor == TaskExecutorAgent {
		if values.Values[1] == nil {
			return serviceTaskChange{}, errs.New(errs.KindInternal, "Service lifecycle render input was not found")
		}
		input, decodeErr := decodeServiceLifecycleRenderInput(values.Values[1].Value)
		if decodeErr != nil || input.PlanID != source.PlanID || source.PlanID != retry.PlanID {
			return serviceTaskChange{}, errs.New(errs.KindInternal, "Service lifecycle render input changed")
		}
		change.conditions = append(change.conditions,
			Condition{Key: serviceLifecycleRenderInputKey(source.ID), ModRevision: values.Values[1].ModRevision},
			Condition{Key: serviceLifecycleRenderInputKey(retry.ID)},
		)
		change.mutations = append(change.mutations, Mutation{
			Type: MutationPut, Key: serviceLifecycleRenderInputKey(retry.ID), Value: values.Values[1].Value,
		})
	} else if values.Values[1] != nil {
		return serviceTaskChange{}, errs.New(errs.KindInternal, "Controller Service Task has a render input")
	}
	return change, nil
}

func (repository *TaskRepository) prepareServiceTaskAcknowledgement(
	ctx context.Context,
	task TaskRecord,
	readRevision int64,
) (serviceTaskChange, error) {
	if !isServiceLifecycleTask(task) {
		return serviceTaskChange{}, nil
	}
	values, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{serviceLifecycleActiveKey(task.Target)}, Revision: readRevision,
	})
	if err != nil {
		return serviceTaskChange{}, err
	}
	if values == nil || len(values.Values) != 1 || values.Values[0] == nil {
		return serviceTaskChange{}, errs.New(errs.KindStateConflict, "Service lifecycle Task is not active")
	}
	activeTaskID, err := decodeTaskReference(values.Values[0].Value)
	if err != nil {
		return serviceTaskChange{}, err
	}
	if activeTaskID != task.ID {
		return serviceTaskChange{}, errs.New(errs.KindStateConflict, "Service lifecycle Task ownership changed")
	}
	return serviceTaskChange{
		applies: true,
		conditions: []Condition{
			{Key: serviceLifecycleActiveKey(task.Target), ModRevision: values.Values[0].ModRevision},
		},
		mutations: []Mutation{{Type: MutationDelete, Key: serviceLifecycleActiveKey(task.Target)}},
	}, nil
}

func isServiceLifecycleTask(task TaskRecord) bool {
	if task.Target == "" {
		return false
	}
	switch task.Type {
	case TaskStart, TaskStop, TaskDestroy:
		return true
	default:
		return false
	}
}

func serviceLifecycleIntent(taskType TaskType) core.ServiceRuntimeIntent {
	switch taskType {
	case TaskStart:
		return core.ServiceRuntimeIntentRunning
	case TaskStop:
		return core.ServiceRuntimeIntentStopped
	case TaskDestroy:
		return core.ServiceRuntimeIntentAbsent
	default:
		return ""
	}
}

func clearServiceTaskChange(change serviceTaskChange) {
	for _, mutation := range change.mutations {
		clear(mutation.Value)
	}
}
