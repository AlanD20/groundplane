package etcd

import (
	"context"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type attachTaskChange struct {
	applies    bool
	mutates    bool
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
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
		conditions: []etcdstore.Condition{{Key: attachrecord.AttachKey(task.Target), ModRevision: current.Revision}},
	}
	runtimeConditions, err := repository.attachRuntimeClaimConditions(ctx, task, revision)
	if err != nil {
		return attachTaskChange{}, err
	}
	change.conditions = append(change.conditions, runtimeConditions...)
	if task.Type == taskjournal.TaskDetach {
		if current.Record.Status != core.AttachDetaching ||
			current.Record.Operation != attachrecord.AttachOperationDetach ||
			current.Record.TaskID != task.ID {
			return attachTaskChange{}, attachStateError(current.Record, "cannot claim detaching Task")
		}
		return change, nil
	}
	provisioning, err := attachrecord.MarkAttachProvisioning(current.Record, task.ID)
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
	retrying, err := attachrecord.RetryAttachOperation(current.Record, retry.ID)
	if err != nil {
		return attachTaskChange{}, err
	}
	change, err := encodeAttachTaskChange(attachTaskChange{
		applies:    true,
		conditions: []etcdstore.Condition{{Key: attachrecord.AttachKey(source.Target), ModRevision: current.Revision}},
	}, retrying)
	if err != nil {
		return attachTaskChange{}, err
	}
	if source.Type == taskjournal.TaskDetach {
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
	change.conditions = append(change.conditions, etcdstore.Condition{Key: planReferenceKey})
	change.mutations = append(change.mutations, etcdstore.Mutation{
		Type: etcdstore.MutationPut, Key: planReferenceKey, Value: planReferenceValue,
	})
	change.values = append(change.values, planReferenceValue)
	return change, nil
}

func (repository *TaskRepository) prepareAttachTaskAcknowledgement(
	ctx context.Context,
	task TaskRecord,
	terminalStatus taskjournal.TaskStatus,
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
	succeeded := terminalStatus == taskjournal.TaskStatusCompleted
	if task.Type == taskjournal.TaskAttach {
		var terminal attachrecord.Record
		var completeErr error
		if terminalStatus == taskjournal.TaskStatusAborted && current.Record.Status == core.AttachPending {
			terminal, completeErr = attachrecord.AbortPendingAttachProvisioning(current.Record, task.ID)
		} else {
			terminal, completeErr = attachrecord.CompleteAttachProvisioning(current.Record, task.ID, succeeded)
		}
		if completeErr != nil {
			return attachTaskChange{}, completeErr
		}
		return encodeAttachTaskChange(attachTaskChange{
			applies:    true,
			conditions: []etcdstore.Condition{{Key: attachrecord.AttachKey(task.Target), ModRevision: current.Revision}},
		}, terminal)
	}
	terminal, err := attachrecord.CompleteAttachDetaching(current.Record, task.ID, succeeded)
	if err != nil {
		return attachTaskChange{}, err
	}
	if !succeeded {
		return encodeAttachTaskChange(attachTaskChange{
			applies:    true,
			conditions: []etcdstore.Condition{{Key: attachrecord.AttachKey(task.Target), ModRevision: current.Revision}},
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
	terminalStatus taskjournal.TaskStatus,
	revision int64,
) error {
	applies, err := taskOwnsAttachLifecycle(task)
	if err != nil || !applies {
		return err
	}
	if task.Type == taskjournal.TaskDetach && terminalStatus == taskjournal.TaskStatusCompleted {
		return repository.validateCompletedAttachDetachReplay(ctx, task, revision)
	}
	current, err := repository.readTaskAttach(ctx, task, revision)
	if err != nil {
		return err
	}
	expectedStatus := core.AttachFailed
	expectedOperation := attachrecord.AttachOperationProvision
	if task.Type == taskjournal.TaskAttach && terminalStatus == taskjournal.TaskStatusCompleted {
		expectedStatus = core.AttachReady
	}
	if task.Type == taskjournal.TaskDetach {
		expectedOperation = attachrecord.AttachOperationDetach
		if terminalStatus == taskjournal.TaskStatusCompleted {
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
) (etcdstore.Versioned[attachrecord.Record], error) {
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{attachrecord.AttachKey(task.Target)}, Revision: revision,
	})
	if err != nil {
		return etcdstore.Versioned[attachrecord.Record]{}, err
	}
	if result == nil || result.ReadRevision != revision || len(result.Values) != 1 || result.Values[0] == nil {
		return etcdstore.Versioned[attachrecord.Record]{}, errs.New(errs.KindInternal, "Attach Task target is missing")
	}
	value := result.Values[0]
	record, err := attachrecord.DecodeAttachRecord(value.Value)
	if err != nil {
		return etcdstore.Versioned[attachrecord.Record]{}, err
	}
	if record.ID != task.Target {
		return etcdstore.Versioned[attachrecord.Record]{}, attachrecord.CorruptAttachRecord()
	}
	return etcdstore.Versioned[attachrecord.Record]{
		Record: record, Revision: value.ModRevision, ReadRevision: result.ReadRevision,
	}, nil
}

func (repository *TaskRepository) validateCompletedAttachDetachReplay(
	ctx context.Context,
	task TaskRecord,
	revision int64,
) error {
	evidence, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
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
	if len(input.ConsumerServiceIDs) != 1 || len(input.GrantAttachIDs) > attachrecord.MaximumAttachGrants {
		return errs.New(errs.KindInternal, "attach detach replay removal evidence exceeds Attach bounds")
	}
	keys := []string{
		attachrecord.AttachKey(task.Target),
		attachrecord.AttachNameKey(input.EnvironmentID, input.AttachName),
		attachrecord.AttachOwnerKey(input.EnvironmentID, task.Target),
		attachrecord.AttachBackingServiceKey(input.BackingServiceID, task.Target),
		attachrecord.AttachBackingProjectKey(input.BackingProjectID, task.Target),
		attachrecord.AttachFactsKey(task.Target),
	}
	exclusionKey, err := backupruntime.BackupSourceTargetExclusionKey(backupruntime.BackupSourceTargetAttach, task.Target)
	if err != nil {
		return err
	}
	exclusionIndex := len(keys)
	keys = append(keys, exclusionKey)
	for _, serviceID := range input.ConsumerServiceIDs {
		keys = append(keys, attachrecord.AttachServiceKey(serviceID, task.Target))
	}
	for _, grantID := range input.GrantAttachIDs {
		keys = append(keys, attachrecord.AttachGrantedByKey(grantID, task.Target))
	}
	if len(input.GrantAttachIDs) != 0 {
		keys = append(keys, attachrecord.AttachDependentGrantKey(task.Target))
	}
	replayTargetIndex := -1
	markerKey := ""
	if task.RetryOf == "" {
		locator := idempotencyrecord.IdempotencyLocator{
			ScopeKind: idempotencyrecord.IdempotencyScopeEnvironment,
			ScopeID:   input.EnvironmentID,
			Method:    "DELETE",
			Route:     "/attaches/{id}",
			Key:       task.IdempotencyKey,
		}
		markerKey, err = idempotencyrecord.IdempotencyMarkerKey(locator)
		if err != nil {
			return err
		}
		replayTargetKey, keyErr := idempotencyrecord.IdempotencyReplayTargetKey(
			idempotencyrecord.IdempotencyReplayTarget{Kind: idempotencyrecord.IdempotencyReplayTargetAttach, ID: task.Target},
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
	state, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
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
			exclusion, decodeErr := backupruntime.DecodeBackupSourceTargetExclusionRecord(value.Value)
			if decodeErr != nil || exclusion.TargetKind != backupruntime.BackupSourceTargetAttach ||
				exclusion.TargetID != task.Target || value.Key != exclusionKey {
				return errs.New(errs.KindInternal, "attach detach replay exclusion evidence is corrupt")
			}
			return errs.New(errs.KindStateConflict, "attach detach replay retained backup source exclusion")
		}
		if index == 1 {
			ownerID := string(value.Value)
			if recordcodec.ValidateID(ids.KindAttach, ownerID) != nil {
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
		successorKey := attachrecord.AttachKey(successorNameOwnerID)
		successor, successorErr := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
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
		successorRecord, decodeErr := attachrecord.DecodeAttachRecord(primary.Value)
		if decodeErr != nil || primary.Key != successorKey || successorRecord.ID != successorNameOwnerID ||
			successorRecord.EnvironmentID != input.EnvironmentID || successorRecord.Name != input.AttachName {
			return errs.New(errs.KindInternal, "attach detach replay successor name owner is corrupt")
		}
	}
	grants, err := repository.store.Range(ctx, etcdstore.RangeRequest{
		Prefix: attachrecord.AttachGrantedByPrefix(task.Target), Limit: 1, Revision: revision,
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
	dependentGrants, err := repository.store.Range(ctx, etcdstore.RangeRequest{
		Prefix: attachrecord.AttachDependentGrantPrefix(task.Target),
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
	storedDependentGrants, err := attachrecord.ReadAttachDependentGrantIndex(dependentGrants, task.Target)
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
		if replayTarget == nil || idempotencyrecord.DecodeReplayTargetReference(replayTarget.Value, markerKey) != nil {
			return errs.New(errs.KindStateConflict, "attach detach replay target is missing or corrupt")
		}
	}
	return nil
}

func taskOwnsAttachLifecycle(task TaskRecord) (bool, error) {
	if task.Executor != taskjournal.TaskExecutorAgent || (task.Type != taskjournal.TaskAttach && task.Type != taskjournal.TaskDetach) {
		return false, nil
	}
	if recordcodec.ValidateID(ids.KindAttach, task.Target) != nil {
		return false, errs.New(errs.KindInternal, "Attach Task has an invalid durable target")
	}
	return true, nil
}

func encodeAttachTaskChange(change attachTaskChange, record attachrecord.Record) (attachTaskChange, error) {
	value, err := attachrecord.EncodeAttachRecord(record)
	if err != nil {
		return attachTaskChange{}, err
	}
	change.mutates = true
	change.values = append(change.values, value)
	change.mutations = append(change.mutations, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: attachrecord.AttachKey(record.ID), Value: value})
	return change, nil
}

func clearAttachTaskChange(change attachTaskChange) {
	for _, value := range change.values {
		clear(value)
	}
}
