package releasework

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func RecoveryEffectKey(attemptID string, sequence uint64) string {
	return EffectEvidencePrefix(attemptID) + fmt.Sprintf("recovery/%020d", sequence)
}

func activeRecoveryEffect(
	input releaseexecution.RecoveryInput,
	saved RecoverySnapshot,
) (releaseexecution.RecoveryReport, error) {
	if saved.Record == nil || saved.Revision <= 0 || !saved.Record.Report.Progress.InFlight {
		return releaseexecution.RecoveryReport{}, invalid()
	}
	report := saved.Record.Report
	ack, err := report.Checkpoint(input)
	if err != nil {
		return releaseexecution.RecoveryReport{}, err
	}
	report.Ack = &ack
	if err := report.Validate(input); err != nil {
		return releaseexecution.RecoveryReport{}, err
	}
	return report, nil
}

func PrepareRecoveryEffect(
	ctx context.Context,
	store Reader,
	view workqueue.ResumeView,
	input releaseexecution.RecoveryInput,
	raw []byte,
	reference workexecution.Input,
) (workqueue.Change, error) {
	if store == nil || !view.ActiveAttempt || !view.CurrentExecutor ||
		!workexecution.SameAssignment(view.Attempt.Attempt, input.Input.Attempt) ||
		view.Work.Task.Task.FinishedAt != nil ||
		view.Work.Task.Task.CollectionStartedAt != nil ||
		view.Checkpoint.Record == nil {
		return workqueue.Change{}, invalid()
	}
	saved, err := LoadRecovery(ctx, store, input, view.Work.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	report, err := activeRecoveryEffect(input, saved)
	if err != nil {
		return workqueue.Change{}, invalid()
	}
	failureHook := report.Progress.Phase == releaseexecution.FailureHooks
	if !failureHook &&
		(saved.Revision != view.Checkpoint.Revision || !releaseexecution.EqualCheckpoint(*view.Checkpoint.Record, *report.Ack)) ||
		failureHook && view.Checkpoint.Record.Sequence < report.Sequence {
		return workqueue.Change{}, invalid()
	}
	evidence, err := releaseexecution.DecodeRecoveryEffect(input, report, raw, reference)
	if err != nil || evidence.RecordedAt.After(time.Now().UTC()) {
		return workqueue.Change{}, invalid()
	}
	attempt := input.Input.Attempt
	key := RecoveryEffectKey(attempt.ID, report.Progress.Sequence)
	read, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.Work.ReadRevision})
	if err != nil {
		return workqueue.Change{}, err
	}
	if read == nil || read.ReadRevision != view.Work.ReadRevision || len(read.Values) != 1 {
		return workqueue.Change{}, invalid()
	}
	defer keyvalue.ClearValues(read.Values)
	change := workqueue.Change{Conditions: []keyvalue.Condition{
		{Key: workqueue.TaskKey(attempt.TaskID), ModRevision: view.Work.Task.Revision},
		{Key: workqueue.ItemKey(attempt.WorkItemID), ModRevision: view.Work.Item.Revision},
		{Key: workqueue.AttemptKey(attempt.ID), ModRevision: view.Attempt.Revision},
		{Key: RecoveryKey(attempt.ID), ModRevision: saved.Revision},
		{Key: workqueue.CheckpointKey(attempt.ID), ModRevision: view.Checkpoint.Revision},
	}}
	if failureHook {
		hook, err := report.Progress.ActiveHook(input)
		if err != nil || evidence.Result.Hook == nil || evidence.RecordedAt.Before(view.Checkpoint.Record.RecordedAt) {
			return workqueue.Change{}, invalid()
		}
		var hookChange workqueue.Change
		if evidence.Result.Hook.InterruptedCleanup != nil {
			if saved.Revision != view.Checkpoint.Revision ||
				!releaseexecution.EqualCheckpoint(*view.Checkpoint.Record, *report.Ack) {
				return workqueue.Change{}, invalid()
			}
			hookChange, err = prepareInterruptedHook(
				ctx,
				store,
				view,
				input,
				saved,
				report,
				*evidence.Result.Hook,
				evidence.RecordedAt,
			)
		} else {
			if view.Checkpoint.Record.Sequence <= report.Sequence || evidence.Result.Hook.Checkpoint == nil {
				return workqueue.Change{}, invalid()
			}
			hookChange.Conditions, err = hookCleanupRecordConditions(
				ctx,
				store,
				view,
				input.Input,
				hook,
				*evidence.Result.Hook.Checkpoint,
			)
		}
		if err != nil {
			return workqueue.Change{}, err
		}
		change.Conditions = append(change.Conditions, hookChange.Conditions...)
		change.Mutations = append(change.Mutations, hookChange.Mutations...)
	}
	if value := read.Values[0]; value != nil {
		if len(change.Mutations) != 0 {
			keyvalue.ClearMutationValues(change.Mutations)
			return workqueue.Change{}, invalid()
		}
		if value.Key != key || value.ModRevision <= 0 || value.ModRevision > view.Work.ReadRevision ||
			!bytes.Equal(value.Value, raw) {
			return workqueue.Change{}, invalid()
		}
		change.Conditions = append(change.Conditions, keyvalue.Condition{Key: key, ModRevision: value.ModRevision})
		return change, nil
	}
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: key})
	change.Mutations = append(
		change.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: append([]byte(nil), raw...)},
	)
	return change, nil
}

func RecoveryEffectCompletionConditions(
	ctx context.Context,
	store Reader,
	view workqueue.ResumeView,
	input releaseexecution.RecoveryInput,
	previous RecoverySnapshot,
	next releaseexecution.RecoveryReport,
) ([]keyvalue.Condition, error) {
	if ctx == nil || store == nil {
		return nil, invalid()
	}
	report, err := activeRecoveryEffect(input, previous)
	if err != nil {
		return nil, err
	}
	if next.Progress.InFlight || next.Progress.Evidence == nil ||
		releaseexecution.ValidateRecoveryTransition(input, &report.Progress, next.Progress) != nil {
		return nil, invalid()
	}
	key := RecoveryEffectKey(input.Input.Attempt.ID, report.Progress.Sequence)
	read, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.Work.ReadRevision})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != view.Work.ReadRevision || len(read.Values) != 1 {
		return nil, invalid()
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	if value == nil || value.Key != key || value.ModRevision <= 0 || value.ModRevision > view.Work.ReadRevision {
		return nil, invalid()
	}
	evidence, err := releaseexecution.DecodeRecoveryEffect(input, report, value.Value, *next.Progress.Evidence)
	if err != nil || next.Progress.UpdatedAt.Before(evidence.RecordedAt) {
		return nil, invalid()
	}
	return []keyvalue.Condition{{Key: key, ModRevision: value.ModRevision},
		{Key: RecoveryKey(input.Input.Attempt.ID), ModRevision: previous.Revision}}, nil
}
