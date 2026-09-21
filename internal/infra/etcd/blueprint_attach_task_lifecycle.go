package etcd

import (
	"bytes"
	"context"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	environmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"time"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type blueprintAttachTaskChange struct {
	applies    bool
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
	values     [][]byte
}

func (repository *TaskRepository) prepareBlueprintAttachTaskClaim(
	ctx context.Context,
	task TaskRecord,
	revision int64,
) (blueprintAttachTaskChange, error) {
	intentValue, intent, found, err := repository.readBlueprintAttachTaskIntent(ctx, task, revision)
	if err != nil || !found {
		return blueprintAttachTaskChange{}, err
	}
	if intent.Status != taskjournal.TaskStatusPending {
		return blueprintAttachTaskChange{}, errs.New(
			errs.KindStateConflict,
			"Blueprint Attach Task intent is not pending",
		)
	}
	return repository.prepareBlueprintAttachCandidateTransition(
		ctx, task, intentValue, intent, revision,
		func(record attachrecord.Record) (attachrecord.Record, error) {
			return attachrecord.MarkAttachProvisioning(record, task.ID)
		},
	)
}

func (repository *TaskRepository) prepareBlueprintAttachTaskAcknowledgement(
	ctx context.Context,
	task TaskRecord,
	terminalStatus taskjournal.TaskStatus,
	terminalAt time.Time,
	revision int64,
) (blueprintAttachTaskChange, error) {
	intentValue, intent, found, err := repository.readBlueprintAttachTaskIntent(ctx, task, revision)
	if err != nil || !found {
		return blueprintAttachTaskChange{}, err
	}
	terminalIntent, err := terminalBlueprintAttachTaskIntent(intent, terminalStatus, terminalAt)
	if err != nil {
		return blueprintAttachTaskChange{}, err
	}
	succeeded := terminalStatus == taskjournal.TaskStatusCompleted
	change, err := repository.prepareBlueprintAttachCandidateTransition(
		ctx, task, intentValue, intent, revision,
		func(record attachrecord.Record) (attachrecord.Record, error) {
			if terminalStatus == taskjournal.TaskStatusAborted && record.Status == core.AttachPending {
				return attachrecord.AbortPendingAttachProvisioning(record, task.ID)
			}
			return attachrecord.CompleteAttachProvisioning(record, task.ID, succeeded)
		},
	)
	if err != nil {
		return blueprintAttachTaskChange{}, err
	}
	intentBytes, err := encodeBlueprintAttachTaskIntent(terminalIntent)
	if err != nil {
		clearBlueprintAttachTaskChange(change)
		return blueprintAttachTaskChange{}, err
	}
	change.values = append(change.values, intentBytes)
	change.mutations = append(change.mutations, etcdstore.Mutation{
		Type: etcdstore.MutationPut, Key: blueprintAttachTaskIntentKey(task.ID), Value: intentBytes,
	})
	if intent.OwnsEnvironmentFence {
		active, activeErr := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
			Keys: []string{environmentchanges.ComponentTaskActiveEnvironmentKey(intent.EnvironmentID)}, Revision: revision,
		})
		if activeErr != nil || active == nil || len(active.Values) != 1 ||
			active.Values[0] == nil || !bytes.Equal(active.Values[0].Value, []byte(task.ID)) {
			clearBlueprintAttachTaskChange(change)
			if activeErr != nil {
				return blueprintAttachTaskChange{}, activeErr
			}
			return blueprintAttachTaskChange{}, errs.New(
				errs.KindStateConflict,
				"Blueprint Attach Task lost its Environment fence",
			)
		}
		change.conditions = append(change.conditions, etcdstore.Condition{
			Key: environmentchanges.ComponentTaskActiveEnvironmentKey(intent.EnvironmentID), ModRevision: active.Values[0].ModRevision,
		})
		change.mutations = append(change.mutations, etcdstore.Mutation{
			Type: etcdstore.MutationDelete, Key: environmentchanges.ComponentTaskActiveEnvironmentKey(intent.EnvironmentID),
		})
	}
	return change, nil
}

func (repository *TaskRepository) prepareBlueprintAttachTaskRetry(
	ctx context.Context,
	source TaskRecord,
	retry TaskRecord,
	revision int64,
) (blueprintAttachTaskChange, error) {
	intentValue, intent, found, err := repository.readBlueprintAttachTaskIntent(ctx, source, revision)
	if err != nil || !found {
		return blueprintAttachTaskChange{}, err
	}
	if source.FinishedAt == nil || intent.TerminalAt == nil || intent.Status != source.Status ||
		!intent.TerminalAt.Equal(*source.FinishedAt) || retry.Target != source.Target || retry.RetryOf != source.ID {
		return blueprintAttachTaskChange{}, errs.New(
			errs.KindStateConflict,
			"Blueprint Attach retry changed its pinned Task",
		)
	}
	keys := make([]string, 0, len(intent.Candidates)+1)
	for _, candidate := range intent.Candidates {
		keys = append(keys, attachrecord.AttachKey(candidate.ID))
	}
	if intent.OwnsEnvironmentFence {
		keys = append(keys, environmentchanges.ComponentTaskActiveEnvironmentKey(intent.EnvironmentID))
	}
	state, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return blueprintAttachTaskChange{}, err
	}
	if state == nil || len(state.Values) != len(keys) {
		return blueprintAttachTaskChange{}, errs.New(errs.KindInternal, "Blueprint Attach retry state is incomplete")
	}
	change := blueprintAttachTaskChange{applies: true, conditions: []etcdstore.Condition{
		{Key: blueprintAttachTaskIntentKey(source.ID), ModRevision: intentValue.ModRevision},
		{Key: blueprintAttachTaskIntentKey(retry.ID)},
	}}
	retryCandidates := make([]attachrecord.Record, 0, len(intent.Candidates))
	for index, candidate := range intent.Candidates {
		value := state.Values[index]
		if value == nil {
			clearBlueprintAttachTaskChange(change)
			return blueprintAttachTaskChange{}, errs.New(
				errs.KindStateConflict,
				"Blueprint Attach retry candidate is missing",
			)
		}
		current, decodeErr := attachrecord.DecodeAttachRecord(value.Value)
		if decodeErr != nil || current.ID != candidate.ID || current.TaskID != source.ID {
			clearBlueprintAttachTaskChange(change)
			return blueprintAttachTaskChange{}, errs.New(
				errs.KindStateConflict,
				"Blueprint Attach retry candidate changed",
			)
		}
		retrying, retryErr := attachrecord.RetryAttachOperation(current, retry.ID)
		if retryErr != nil {
			clearBlueprintAttachTaskChange(change)
			return blueprintAttachTaskChange{}, retryErr
		}
		encoded, encodeErr := attachrecord.EncodeAttachRecord(retrying)
		if encodeErr != nil {
			clearBlueprintAttachTaskChange(change)
			return blueprintAttachTaskChange{}, encodeErr
		}
		change.values = append(change.values, encoded)
		change.conditions = append(
			change.conditions,
			etcdstore.Condition{Key: attachrecord.AttachKey(current.ID), ModRevision: value.ModRevision},
		)
		change.mutations = append(
			change.mutations,
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: attachrecord.AttachKey(current.ID), Value: encoded},
		)
		retryCandidates = append(retryCandidates, retrying)
	}
	if intent.OwnsEnvironmentFence {
		active := state.Values[len(state.Values)-1]
		if active != nil {
			clearBlueprintAttachTaskChange(change)
			return blueprintAttachTaskChange{}, errs.New(
				errs.KindStateConflict,
				"Environment already has an active Blueprint Attach retry",
			)
		}
		change.conditions = append(
			change.conditions,
			etcdstore.Condition{Key: environmentchanges.ComponentTaskActiveEnvironmentKey(intent.EnvironmentID)},
		)
		change.mutations = append(change.mutations, etcdstore.Mutation{
			Type: etcdstore.MutationPut, Key: environmentchanges.ComponentTaskActiveEnvironmentKey(intent.EnvironmentID), Value: []byte(retry.ID),
		})
	}
	retryIntent := BlueprintAttachTaskIntent{
		TaskID: retry.ID, EnvironmentID: intent.EnvironmentID, Status: taskjournal.TaskStatusPending,
		OwnsEnvironmentFence: intent.OwnsEnvironmentFence, Candidates: retryCandidates, CreatedAt: retry.CreatedAt,
	}
	intentBytes, err := encodeBlueprintAttachTaskIntent(retryIntent)
	if err != nil {
		clearBlueprintAttachTaskChange(change)
		return blueprintAttachTaskChange{}, err
	}
	change.values = append(change.values, intentBytes)
	change.mutations = append(change.mutations, etcdstore.Mutation{
		Type: etcdstore.MutationPut, Key: blueprintAttachTaskIntentKey(retry.ID), Value: intentBytes,
	})
	return change, nil
}

func (repository *TaskRepository) validateBlueprintAttachTaskAcknowledgementReplay(
	ctx context.Context,
	task TaskRecord,
	terminalStatus taskjournal.TaskStatus,
	revision int64,
) error {
	_, intent, found, err := repository.readBlueprintAttachTaskIntent(ctx, task, revision)
	if err != nil || !found {
		return err
	}
	if task.FinishedAt == nil || intent.Status != terminalStatus || intent.TerminalAt == nil ||
		!intent.TerminalAt.Equal(*task.FinishedAt) {
		return errs.New(errs.KindStateConflict, "Blueprint Attach terminal intent does not match its Task")
	}
	keys := make([]string, 0, len(intent.Candidates)+1)
	for _, candidate := range intent.Candidates {
		keys = append(keys, attachrecord.AttachKey(candidate.ID))
	}
	if intent.OwnsEnvironmentFence {
		keys = append(keys, environmentchanges.ComponentTaskActiveEnvironmentKey(intent.EnvironmentID))
	}
	state, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return err
	}
	if state == nil || len(state.Values) != len(keys) {
		return errs.New(errs.KindInternal, "Blueprint Attach terminal replay is incomplete")
	}
	expectedStatus := core.AttachFailed
	if terminalStatus == taskjournal.TaskStatusCompleted {
		expectedStatus = core.AttachReady
	}
	for index, candidate := range intent.Candidates {
		value := state.Values[index]
		if value == nil {
			return errs.New(errs.KindStateConflict, "Blueprint Attach terminal candidate is missing")
		}
		record, decodeErr := attachrecord.DecodeAttachRecord(value.Value)
		if decodeErr != nil || record.ID != candidate.ID || record.TaskID != task.ID ||
			record.Status != expectedStatus {
			return errs.New(errs.KindStateConflict, "Blueprint Attach terminal candidate does not match its Task")
		}
	}
	if intent.OwnsEnvironmentFence && state.Values[len(state.Values)-1] != nil {
		return errs.New(errs.KindStateConflict, "Blueprint Attach terminal Task retained its Environment fence")
	}
	return nil
}

func (repository *TaskRepository) prepareBlueprintAttachCandidateTransition(
	ctx context.Context,
	task TaskRecord,
	intentValue *etcdstore.KeyValue,
	intent BlueprintAttachTaskIntent,
	revision int64,
	transition func(attachrecord.Record) (attachrecord.Record, error),
) (blueprintAttachTaskChange, error) {
	if task.ID != intent.TaskID || task.Target != intent.EnvironmentID {
		return blueprintAttachTaskChange{}, errs.New(
			errs.KindStateConflict,
			"Blueprint Attach intent has the wrong Task owner",
		)
	}
	keys := make([]string, 0, len(intent.Candidates))
	for _, candidate := range intent.Candidates {
		keys = append(keys, attachrecord.AttachKey(candidate.ID))
	}
	state, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return blueprintAttachTaskChange{}, err
	}
	if state == nil || len(state.Values) != len(keys) {
		return blueprintAttachTaskChange{}, errs.New(errs.KindInternal, "Blueprint Attach candidate read is incomplete")
	}
	change := blueprintAttachTaskChange{
		applies:    true,
		conditions: []etcdstore.Condition{{Key: blueprintAttachTaskIntentKey(task.ID), ModRevision: intentValue.ModRevision}},
	}
	for index, candidate := range intent.Candidates {
		value := state.Values[index]
		if value == nil {
			clearBlueprintAttachTaskChange(change)
			return blueprintAttachTaskChange{}, errs.New(
				errs.KindStateConflict,
				"Blueprint Attach candidate is missing",
			)
		}
		current, decodeErr := attachrecord.DecodeAttachRecord(value.Value)
		if decodeErr != nil || current.ID != candidate.ID || current.EnvironmentID != intent.EnvironmentID ||
			current.TaskID != task.ID || !sameBlueprintAttachFactSets(current.FactSets, candidate.FactSets) {
			clearBlueprintAttachTaskChange(change)
			return blueprintAttachTaskChange{}, errs.New(errs.KindStateConflict, "Blueprint Attach candidate changed")
		}
		next, transitionErr := transition(current)
		if transitionErr != nil {
			clearBlueprintAttachTaskChange(change)
			return blueprintAttachTaskChange{}, transitionErr
		}
		encoded, encodeErr := attachrecord.EncodeAttachRecord(next)
		if encodeErr != nil {
			clearBlueprintAttachTaskChange(change)
			return blueprintAttachTaskChange{}, encodeErr
		}
		change.values = append(change.values, encoded)
		change.conditions = append(
			change.conditions,
			etcdstore.Condition{Key: attachrecord.AttachKey(current.ID), ModRevision: value.ModRevision},
		)
		change.mutations = append(
			change.mutations,
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: attachrecord.AttachKey(current.ID), Value: encoded},
		)
	}
	return change, nil
}

func (repository *TaskRepository) readBlueprintAttachTaskIntent(
	ctx context.Context,
	task TaskRecord,
	revision int64,
) (*etcdstore.KeyValue, BlueprintAttachTaskIntent, bool, error) {
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{blueprintAttachTaskIntentKey(task.ID)}, Revision: revision,
	})
	if err != nil {
		return nil, BlueprintAttachTaskIntent{}, false, err
	}
	if result == nil || len(result.Values) != 1 {
		return nil, BlueprintAttachTaskIntent{}, false, errs.New(
			errs.KindInternal,
			"Blueprint Attach intent read is incomplete",
		)
	}
	if result.Values[0] == nil {
		return nil, BlueprintAttachTaskIntent{}, false, nil
	}
	intent, err := decodeBlueprintAttachTaskIntent(result.Values[0].Value)
	if err != nil {
		return nil, BlueprintAttachTaskIntent{}, false, err
	}
	return result.Values[0], intent, true, nil
}

func clearBlueprintAttachTaskChange(change blueprintAttachTaskChange) {
	for _, value := range change.values {
		clear(value)
	}
}
