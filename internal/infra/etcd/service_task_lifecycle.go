package etcd

import (
	"context"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type serviceTaskChange struct {
	applies    bool
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
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
	values, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		serviceLifecycleActiveKey(source.Target),
		releaserender.ServiceLifecycleRenderInputKey(source.ID),
		releaserender.ServiceLifecycleRenderInputKey(retry.ID),
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
	reference, err := idempotencyrecord.EncodeTaskReference(retry.ID)
	if err != nil {
		return serviceTaskChange{}, err
	}
	change := serviceTaskChange{
		applies: true,
		conditions: []etcdstore.Condition{
			servicerecord.ServiceDesiredCondition(service),
			servicerecord.ServiceRuntimeCondition(service),
			{Key: serviceLifecycleActiveKey(source.Target)},
		},
		mutations: []etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: serviceLifecycleActiveKey(source.Target), Value: reference}},
	}
	if source.Executor == taskjournal.TaskExecutorAgent {
		if values.Values[1] == nil {
			return serviceTaskChange{}, errs.New(errs.KindInternal, "Service lifecycle render input was not found")
		}
		input, decodeErr := releaserender.DecodeServiceLifecycleRenderInput(values.Values[1].Value)
		if decodeErr != nil || input.PlanID != source.PlanID || source.PlanID != retry.PlanID {
			return serviceTaskChange{}, errs.New(errs.KindInternal, "Service lifecycle render input changed")
		}
		change.conditions = append(change.conditions,
			etcdstore.Condition{Key: releaserender.ServiceLifecycleRenderInputKey(source.ID), ModRevision: values.Values[1].ModRevision},
			etcdstore.Condition{Key: releaserender.ServiceLifecycleRenderInputKey(retry.ID)},
		)
		change.mutations = append(change.mutations, etcdstore.Mutation{
			Type: etcdstore.MutationPut, Key: releaserender.ServiceLifecycleRenderInputKey(retry.ID), Value: values.Values[1].Value,
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
	values, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{serviceLifecycleActiveKey(task.Target)}, Revision: readRevision,
	})
	if err != nil {
		return serviceTaskChange{}, err
	}
	if values == nil || len(values.Values) != 1 || values.Values[0] == nil {
		return serviceTaskChange{}, errs.New(errs.KindStateConflict, "Service lifecycle Task is not active")
	}
	activeTaskID, err := idempotencyrecord.DecodeTaskReference(values.Values[0].Value)
	if err != nil {
		return serviceTaskChange{}, err
	}
	if activeTaskID != task.ID {
		return serviceTaskChange{}, errs.New(errs.KindStateConflict, "Service lifecycle Task ownership changed")
	}
	return serviceTaskChange{
		applies: true,
		conditions: []etcdstore.Condition{
			{Key: serviceLifecycleActiveKey(task.Target), ModRevision: values.Values[0].ModRevision},
		},
		mutations: []etcdstore.Mutation{{Type: etcdstore.MutationDelete, Key: serviceLifecycleActiveKey(task.Target)}},
	}, nil
}

func (repository *TaskRepository) prepareAcknowledgedServiceTask(
	ctx context.Context,
	task TaskRecord,
	assignment taskassignments.TaskAssignmentRecord,
	terminalStatus taskjournal.TaskStatus,
	readRevision int64,
) (serviceTaskChange, error) {
	change, err := repository.prepareServiceTaskAcknowledgement(ctx, task, readRevision)
	if err != nil || !change.applies || terminalStatus != taskjournal.TaskStatusCompleted || task.Executor != taskjournal.TaskExecutorAgent {
		return change, err
	}
	inputRead, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{releaserender.ServiceLifecycleRenderInputKey(task.ID)}, Revision: readRevision,
	})
	if err != nil {
		return serviceTaskChange{}, err
	}
	if inputRead == nil || inputRead.ReadRevision != readRevision || len(inputRead.Values) != 1 ||
		inputRead.Values[0] == nil {
		if inputRead != nil {
			etcdstore.ClearValues(inputRead.Values)
		}
		return serviceTaskChange{}, errs.New(errs.KindInternal, "Service lifecycle render input is missing")
	}
	defer etcdstore.ClearValues(inputRead.Values)
	input, err := releaserender.DecodeServiceLifecycleRenderInput(inputRead.Values[0].Value)
	if err != nil || input.PlanID != task.PlanID || input.ServiceID != task.Target {
		return serviceTaskChange{}, errs.New(errs.KindInternal, "Service lifecycle render input changed")
	}
	event := backinghook.Event("")
	stepID := ""
	if task.Type == taskjournal.TaskStart && input.HookConfiguration != nil && input.HookConfiguration.AfterStart != nil {
		event = backinghook.AfterStart
		stepID = task.Steps[len(task.Steps)-1].ID
	}
	if (task.Type == taskjournal.TaskStop || task.Type == taskjournal.TaskDestroy) && input.HookConfiguration != nil &&
		input.HookConfiguration.BeforeStop != nil {
		event = backinghook.BeforeStop
		stepID = task.Steps[0].ID
	}
	if event == "" {
		return change, nil
	}
	checkpoint, condition, err := repository.requireBackingHookResultCheckpoint(
		ctx, task, assignment, stepID, "", event, readRevision,
	)
	if err != nil {
		return serviceTaskChange{}, err
	}
	if checkpoint.Facts != nil {
		clear(checkpoint.Facts.Ciphertext)
		return serviceTaskChange{}, errs.New(errs.KindInternal, "Service lifecycle hook checkpoint contains facts")
	}
	change.conditions = append(change.conditions, condition)
	return change, nil
}

func isServiceLifecycleTask(task TaskRecord) bool {
	if task.Target == "" {
		return false
	}
	switch task.Type {
	case taskjournal.TaskStart, taskjournal.TaskStop, taskjournal.TaskDestroy:
		return true
	default:
		return false
	}
}

func serviceLifecycleIntent(taskType taskjournal.TaskType) core.ServiceRuntimeIntent {
	switch taskType {
	case taskjournal.TaskStart:
		return core.ServiceRuntimeIntentRunning
	case taskjournal.TaskStop:
		return core.ServiceRuntimeIntentStopped
	case taskjournal.TaskDestroy:
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
