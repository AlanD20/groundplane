package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type attachTaskChange struct {
	applies    bool
	mutates    bool
	conditions []Condition
	mutations  []Mutation
	values     [][]byte
}

func (repository *TaskRepository) prepareAttachTaskClaim(
	ctx context.Context,
	task TaskRecord,
	revision int64,
) (attachTaskChange, error) {
	applies, err := taskOwnsAttachLifecycle(task)
	if err != nil || !applies {
		return attachTaskChange{}, err
	}
	current, err := repository.readTaskAttach(ctx, task, revision)
	if err != nil {
		return attachTaskChange{}, err
	}
	change := attachTaskChange{
		applies:    true,
		conditions: []Condition{{Key: attachKey(task.Target), ModRevision: current.Revision}},
	}
	if task.Type == TaskDetach {
		if current.Record.Status != core.AttachDetaching ||
			current.Record.Operation != AttachOperationDetach ||
			current.Record.TaskID != task.ID {
			return attachTaskChange{}, attachStateError(current.Record, "cannot claim detaching Task")
		}
		return change, nil
	}
	provisioning, err := MarkAttachProvisioning(current.Record, task.ID)
	if err != nil {
		return attachTaskChange{}, err
	}
	return encodeAttachTaskChange(change, provisioning)
}

func (repository *TaskRepository) prepareAttachTaskRetry(
	ctx context.Context,
	source TaskRecord,
	retry TaskRecord,
	revision int64,
) (attachTaskChange, error) {
	applies, err := taskOwnsAttachLifecycle(source)
	if err != nil || !applies {
		return attachTaskChange{}, err
	}
	if retry.Type != source.Type || retry.Target != source.Target {
		return attachTaskChange{}, errs.New(errs.KindInternal, "Attach retry changed its durable target")
	}
	current, err := repository.readTaskAttach(ctx, source, revision)
	if err != nil {
		return attachTaskChange{}, err
	}
	retrying, err := RetryAttachOperation(current.Record, retry.ID)
	if err != nil {
		return attachTaskChange{}, err
	}
	return encodeAttachTaskChange(attachTaskChange{
		applies:    true,
		conditions: []Condition{{Key: attachKey(source.Target), ModRevision: current.Revision}},
	}, retrying)
}

func (repository *TaskRepository) prepareAttachTaskAcknowledgement(
	ctx context.Context,
	task TaskRecord,
	terminalStatus TaskStatus,
	revision int64,
) (attachTaskChange, error) {
	applies, err := taskOwnsAttachLifecycle(task)
	if err != nil || !applies {
		return attachTaskChange{}, err
	}
	current, err := repository.readTaskAttach(ctx, task, revision)
	if err != nil {
		return attachTaskChange{}, err
	}
	succeeded := terminalStatus == TaskStatusCompleted
	if task.Type == TaskAttach {
		terminal, completeErr := CompleteAttachProvisioning(current.Record, task.ID, succeeded)
		if completeErr != nil {
			return attachTaskChange{}, completeErr
		}
		return encodeAttachTaskChange(attachTaskChange{
			applies:    true,
			conditions: []Condition{{Key: attachKey(task.Target), ModRevision: current.Revision}},
		}, terminal)
	}
	terminal, err := CompleteAttachDetaching(current.Record, task.ID, succeeded)
	if err != nil {
		return attachTaskChange{}, err
	}
	if !succeeded {
		return encodeAttachTaskChange(attachTaskChange{
			applies:    true,
			conditions: []Condition{{Key: attachKey(task.Target), ModRevision: current.Revision}},
		}, terminal)
	}
	conditions, mutations, values, err := prepareAttachRemoval(
		ctx,
		repository.store,
		current,
		revision,
	)
	if err != nil {
		return attachTaskChange{}, err
	}
	return attachTaskChange{
		applies: true, mutates: true, conditions: conditions, mutations: mutations, values: values,
	}, nil
}

func (repository *TaskRepository) validateAttachTaskAcknowledgementReplay(
	ctx context.Context,
	task TaskRecord,
	terminalStatus TaskStatus,
	revision int64,
) error {
	applies, err := taskOwnsAttachLifecycle(task)
	if err != nil || !applies {
		return err
	}
	if task.Type == TaskDetach && terminalStatus == TaskStatusCompleted {
		result, readErr := repository.store.GetMany(ctx, GetManyRequest{
			Keys: []string{attachKey(task.Target)}, Revision: revision,
		})
		if readErr != nil {
			return readErr
		}
		if result == nil || len(result.Values) != 1 {
			return errs.New(errs.KindInternal, "Attach Task replay read is incomplete")
		}
		if result.Values[0] == nil {
			return nil
		}
		return errs.New(errs.KindStateConflict, "completed detach still has a durable Attach")
	}
	current, err := repository.readTaskAttach(ctx, task, revision)
	if err != nil {
		return err
	}
	expectedStatus := core.AttachFailed
	expectedOperation := AttachOperationProvision
	if task.Type == TaskAttach && terminalStatus == TaskStatusCompleted {
		expectedStatus = core.AttachReady
	}
	if task.Type == TaskDetach {
		expectedOperation = AttachOperationDetach
		if terminalStatus == TaskStatusCompleted {
			expectedStatus = core.AttachDetached
		}
	}
	if current.Record.Status != expectedStatus || current.Record.Operation != expectedOperation ||
		current.Record.TaskID != task.ID {
		return errs.New(errs.KindStateConflict, "Attach does not match the terminal Task acknowledgement")
	}
	return nil
}

func (repository *TaskRepository) readTaskAttach(
	ctx context.Context,
	task TaskRecord,
	revision int64,
) (Versioned[AttachRecord], error) {
	result, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{attachKey(task.Target)}, Revision: revision,
	})
	if err != nil {
		return Versioned[AttachRecord]{}, err
	}
	if len(result.Values) != 1 || result.Values[0] == nil {
		return Versioned[AttachRecord]{}, errs.New(errs.KindInternal, "Attach Task target is missing")
	}
	value := result.Values[0]
	record, err := decodeAttachRecord(value.Value)
	if err != nil {
		return Versioned[AttachRecord]{}, err
	}
	if record.ID != task.Target {
		return Versioned[AttachRecord]{}, corruptAttachRecord()
	}
	return Versioned[AttachRecord]{
		Record: record, Revision: value.ModRevision, ReadRevision: result.ReadRevision,
	}, nil
}

func taskOwnsAttachLifecycle(task TaskRecord) (bool, error) {
	if task.Executor != TaskExecutorAgent || (task.Type != TaskAttach && task.Type != TaskDetach) {
		return false, nil
	}
	if validateStableID(ids.KindAttach, task.Target) != nil {
		return false, errs.New(errs.KindInternal, "Attach Task has an invalid durable target")
	}
	return true, nil
}

func encodeAttachTaskChange(change attachTaskChange, record AttachRecord) (attachTaskChange, error) {
	value, err := encodeAttachRecord(record)
	if err != nil {
		return attachTaskChange{}, err
	}
	change.mutates = true
	change.values = append(change.values, value)
	change.mutations = append(change.mutations, Mutation{Type: MutationPut, Key: attachKey(record.ID), Value: value})
	return change, nil
}

func clearAttachTaskChange(change attachTaskChange) {
	for _, value := range change.values {
		clear(value)
	}
}
