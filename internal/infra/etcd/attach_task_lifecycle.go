package etcd

import (
	"context"
	"slices"

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
	change, err := encodeAttachTaskChange(attachTaskChange{
		applies:    true,
		conditions: []Condition{{Key: attachKey(source.Target), ModRevision: current.Revision}},
	}, retrying)
	if err != nil {
		return attachTaskChange{}, err
	}
	if source.Type == TaskDetach {
		exclusionCondition, exclusionErr := requireAttachBackupSourceExclusionAbsent(
			ctx, repository.store, source.Target, revision,
		)
		if exclusionErr != nil {
			clearAttachTaskChange(change)
			return attachTaskChange{}, exclusionErr
		}
		change.conditions = append(change.conditions, exclusionCondition)
	}
	planReferenceKey, planReferenceValue, _, err := prepareAttachTaskPlanReference(retry)
	if err != nil {
		clearAttachTaskChange(change)
		return attachTaskChange{}, err
	}
	change.conditions = append(change.conditions, Condition{Key: planReferenceKey})
	change.mutations = append(change.mutations, Mutation{
		Type: MutationPut, Key: planReferenceKey, Value: planReferenceValue,
	})
	change.values = append(change.values, planReferenceValue)
	return change, nil
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
		var terminal AttachRecord
		var completeErr error
		if terminalStatus == TaskStatusAborted && current.Record.Status == core.AttachPending {
			terminal, completeErr = AbortPendingAttachProvisioning(current.Record, task.ID)
		} else {
			terminal, completeErr = CompleteAttachProvisioning(current.Record, task.ID, succeeded)
		}
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
		return repository.validateCompletedAttachDetachReplay(ctx, task, revision)
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
	if result == nil || result.ReadRevision != revision || len(result.Values) != 1 || result.Values[0] == nil {
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

func (repository *TaskRepository) validateCompletedAttachDetachReplay(
	ctx context.Context,
	task TaskRecord,
	revision int64,
) error {
	evidence, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{attachTaskRenderInputKey(task.PlanID)}, Revision: revision,
	})
	if err != nil {
		return err
	}
	if evidence == nil || evidence.ReadRevision != revision || len(evidence.Values) != 1 || evidence.Values[0] == nil {
		return errs.New(errs.KindInternal, "attach detach replay evidence is incomplete")
	}
	defer clearKeyValues(evidence.Values)
	input, err := decodeAttachTaskRenderInput(evidence.Values[0].Value)
	if err != nil || input.PlanID != task.PlanID || input.AttachID != task.Target ||
		input.EnvironmentID != task.Params[TaskMutationEnvironmentParam] {
		return errs.New(errs.KindInternal, "attach detach replay evidence is corrupt")
	}
	if len(input.ConsumerServiceIDs) != 1 || len(input.GrantAttachIDs) > MaximumAttachGrants {
		return errs.New(errs.KindInternal, "attach detach replay removal evidence exceeds Attach bounds")
	}
	keys := []string{
		attachKey(task.Target),
		attachNameKey(input.EnvironmentID, input.AttachName),
		attachOwnerKey(input.EnvironmentID, task.Target),
		attachBackingServiceKey(input.BackingServiceID, task.Target),
		attachBackingProjectKey(input.BackingProjectID, task.Target),
		attachFactsKey(task.Target),
	}
	exclusionKey, err := backupSourceTargetExclusionKey(BackupSourceTargetAttach, task.Target)
	if err != nil {
		return err
	}
	exclusionIndex := len(keys)
	keys = append(keys, exclusionKey)
	for _, serviceID := range input.ConsumerServiceIDs {
		keys = append(keys, attachServiceKey(serviceID, task.Target))
	}
	for _, grantID := range input.GrantAttachIDs {
		keys = append(keys, attachGrantedByKey(grantID, task.Target))
	}
	if len(input.GrantAttachIDs) != 0 {
		keys = append(keys, attachDependentGrantKey(task.Target))
	}
	replayTargetIndex := -1
	markerKey := ""
	if task.RetryOf == "" {
		locator := IdempotencyLocator{
			ScopeKind: IdempotencyScopeEnvironment,
			ScopeID:   input.EnvironmentID,
			Method:    "DELETE",
			Route:     "/attaches/{id}",
			Key:       task.IdempotencyKey,
		}
		markerKey, err = idempotencyMarkerKey(locator)
		if err != nil {
			return err
		}
		replayTargetKey, keyErr := idempotencyReplayTargetKey(
			IdempotencyReplayTarget{Kind: IdempotencyReplayTargetAttach, ID: task.Target},
			locator.Method,
			locator.Route,
			locator.Key,
		)
		if keyErr != nil {
			return keyErr
		}
		replayTargetIndex = len(keys)
		keys = append(keys, replayTargetKey)
	}
	state, err := repository.store.GetMany(ctx, GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return err
	}
	if state == nil || state.ReadRevision != revision || len(state.Values) != len(keys) {
		return errs.New(errs.KindInternal, "attach detach replay state is incomplete")
	}
	defer clearKeyValues(state.Values)
	companionCount := len(state.Values)
	if replayTargetIndex >= 0 {
		companionCount = replayTargetIndex
	}
	successorNameOwnerID := ""
	for index, value := range state.Values[:companionCount] {
		if value == nil {
			continue
		}
		if value.Key != keys[index] {
			return errs.New(errs.KindInternal, "attach detach replay companion evidence is misbucketed")
		}
		if index == exclusionIndex {
			exclusion, decodeErr := decodeBackupSourceTargetExclusionRecord(value.Value)
			if decodeErr != nil || exclusion.TargetKind != BackupSourceTargetAttach ||
				exclusion.TargetID != task.Target || value.Key != exclusionKey {
				return errs.New(errs.KindInternal, "attach detach replay exclusion evidence is corrupt")
			}
			return errs.New(errs.KindStateConflict, "attach detach replay retained backup source exclusion")
		}
		if index == 1 {
			ownerID := string(value.Value)
			if validateStableID(ids.KindAttach, ownerID) != nil {
				return errs.New(errs.KindInternal, "attach detach replay successor name owner is corrupt")
			}
			if ownerID != task.Target {
				successorNameOwnerID = ownerID
				continue
			}
		}
		return errs.New(errs.KindStateConflict, "completed attach detach retained durable companion state")
	}
	if successorNameOwnerID != "" {
		successorKey := attachKey(successorNameOwnerID)
		successor, successorErr := repository.store.GetMany(ctx, GetManyRequest{
			Keys: []string{successorKey}, Revision: revision,
		})
		if successorErr != nil {
			return successorErr
		}
		if successor == nil || successor.ReadRevision != revision || len(successor.Values) != 1 ||
			successor.Values[0] == nil {
			return errs.New(errs.KindInternal, "attach detach replay successor name owner is dangling")
		}
		defer clearKeyValues(successor.Values)
		primary := successor.Values[0]
		successorRecord, decodeErr := decodeAttachRecord(primary.Value)
		if decodeErr != nil || primary.Key != successorKey || successorRecord.ID != successorNameOwnerID ||
			successorRecord.EnvironmentID != input.EnvironmentID || successorRecord.Name != input.AttachName {
			return errs.New(errs.KindInternal, "attach detach replay successor name owner is corrupt")
		}
	}
	grants, err := repository.store.Range(ctx, RangeRequest{
		Prefix: attachGrantedByPrefix(task.Target), Limit: 1, Revision: revision,
	})
	if err != nil {
		return err
	}
	if grants != nil {
		defer clearRangeValues(grants.Values)
	}
	if grants == nil || grants.ReadRevision != revision || len(grants.Values) > 1 {
		return errs.New(errs.KindInternal, "attach detach replay grant evidence is incomplete")
	}
	if len(grants.Values) != 0 {
		return errs.New(errs.KindStateConflict, "attach detach replay retained grant membership")
	}
	dependentGrants, err := repository.store.Range(ctx, RangeRequest{
		Prefix: attachDependentGrantPrefix(task.Target),
		Limit:  2, Revision: revision,
	})
	if err != nil {
		return err
	}
	if dependentGrants != nil {
		defer clearRangeValues(dependentGrants.Values)
	}
	if dependentGrants == nil || dependentGrants.ReadRevision != revision ||
		dependentGrants.More || len(dependentGrants.Values) > 1 {
		return errs.New(errs.KindInternal, "attach detach replay dependent grant evidence is incomplete")
	}
	storedDependentGrants, err := readAttachDependentGrantIndex(dependentGrants, task.Target)
	if err != nil {
		return err
	}
	if len(dependentGrants.Values) != 0 {
		if len(input.GrantAttachIDs) == 0 ||
			!slices.Equal(storedDependentGrants, input.GrantAttachIDs) {
			return errs.New(errs.KindInternal, "attach detach replay retained impossible dependent grant membership")
		}
		return errs.New(errs.KindStateConflict, "attach detach replay retained dependent grant membership")
	}
	if replayTargetIndex >= 0 {
		replayTarget := state.Values[replayTargetIndex]
		if replayTarget == nil || decodeReplayTargetReference(replayTarget.Value, markerKey) != nil {
			return errs.New(errs.KindStateConflict, "attach detach replay target is missing or corrupt")
		}
	}
	return nil
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
