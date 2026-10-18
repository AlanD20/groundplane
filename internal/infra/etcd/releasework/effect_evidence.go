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

func EffectEvidenceKey(attemptID string, sequence uint64) string {
	return EffectEvidencePrefix(attemptID) + fmt.Sprintf("effect/%020d", sequence)
}

func EffectEvidencePrefix(attemptID string) string {
	return "/v1/execution/release-evidence/" + attemptID + "/"
}

// PrepareEffectEvidence writes immutable proof without advancing the Release.
// It must be committed with authenticated executor comparisons by the caller.
// A stopped/superseded Task can still report an already-authorized effect.
func PrepareEffectEvidence(ctx context.Context, store Reader, view workqueue.ResumeView, input releaseexecution.Input, raw []byte, reference workexecution.Input) (workqueue.Change, error) {
	if store == nil || !view.ActiveAttempt || !view.CurrentExecutor || !workexecution.SameAssignment(view.Attempt.Attempt, input.Attempt) ||
		view.Work.Task.Task.FinishedAt != nil || view.Work.Task.Task.CollectionStartedAt != nil || view.Checkpoint.Record == nil {
		return workqueue.Change{}, invalid()
	}
	saved, err := Load(ctx, store, input, view.Work.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	report, err := activeEffectReport(input, saved)
	if err != nil {
		return workqueue.Change{}, err
	}
	if !releaseexecution.EqualCheckpoint(*view.Checkpoint.Record, *report.Ack) || saved.Revision != view.Checkpoint.Revision {
		return workqueue.Change{}, invalid()
	}
	evidence, err := releaseexecution.DecodeEffectEvidence(input, report, raw, reference)
	if err != nil || evidence.RecordedAt.After(time.Now().UTC()) {
		return workqueue.Change{}, invalid()
	}
	key := EffectEvidenceKey(input.Attempt.ID, report.Sequence)
	read, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.Work.ReadRevision})
	if err != nil {
		return workqueue.Change{}, err
	}
	if read == nil || read.ReadRevision != view.Work.ReadRevision || len(read.Values) != 1 {
		return workqueue.Change{}, invalid()
	}
	defer keyvalue.ClearValues(read.Values)
	change := workqueue.Change{Conditions: []keyvalue.Condition{
		{Key: workqueue.TaskKey(input.Attempt.TaskID), ModRevision: view.Work.Task.Revision},
		{Key: workqueue.ItemKey(input.Attempt.WorkItemID), ModRevision: view.Work.Item.Revision},
		{Key: workqueue.AttemptKey(input.Attempt.ID), ModRevision: view.Attempt.Revision},
		{Key: ProgressKey(input.Attempt.ID), ModRevision: saved.Revision},
		{Key: workqueue.CheckpointKey(input.Attempt.ID), ModRevision: view.Checkpoint.Revision},
	}}
	if value := read.Values[0]; value != nil {
		if value.Key != key || value.ModRevision <= 0 || !bytes.Equal(value.Value, raw) {
			return workqueue.Change{}, invalid()
		}
		change.Conditions = append(change.Conditions, keyvalue.Condition{Key: key, ModRevision: value.ModRevision})
		return change, nil
	}
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: key})
	change.Mutations = []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: append([]byte(nil), raw...)}}
	return change, nil
}

func EffectCompletionConditions(ctx context.Context, store Reader, view workqueue.ResumeView, input releaseexecution.Input, previous Snapshot, next releaseexecution.CheckpointReport) ([]keyvalue.Condition, error) {
	if ctx == nil || store == nil {
		return nil, invalid()
	}
	report, err := activeEffectReport(input, previous)
	if err != nil {
		return nil, err
	}
	if next.Progress.InFlight || next.Progress.Evidence == nil || releaseexecution.ValidateTransition(input, &report.Progress, next.Progress) != nil {
		return nil, invalid()
	}
	key := EffectEvidenceKey(input.Attempt.ID, report.Sequence)
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
	evidence, err := releaseexecution.DecodeEffectEvidence(input, report, value.Value, *next.Progress.Evidence)
	if err != nil || next.Progress.UpdatedAt.Before(evidence.RecordedAt) {
		return nil, invalid()
	}
	return []keyvalue.Condition{{Key: key, ModRevision: value.ModRevision}, {Key: ProgressKey(input.Attempt.ID), ModRevision: previous.Revision}}, nil
}

func activeEffectReport(input releaseexecution.Input, saved Snapshot) (releaseexecution.CheckpointReport, error) {
	if saved.Record == nil || saved.Revision <= 0 || !saved.Record.Progress.InFlight ||
		(saved.Record.Progress.Phase != releaseexecution.Rollout && saved.Record.Progress.Phase != releaseexecution.Configuration) {
		return releaseexecution.CheckpointReport{}, invalid()
	}
	report := releaseexecution.CheckpointReport{Sequence: saved.Record.CheckpointSequence, StageID: saved.Record.StageID, Progress: saved.Record.Progress}
	ack, err := report.Checkpoint(input.Attempt)
	if err != nil {
		return releaseexecution.CheckpointReport{}, err
	}
	report.Ack = &ack
	if report.ValidateEnvelope(input.Attempt) != nil {
		return releaseexecution.CheckpointReport{}, invalid()
	}
	return report, nil
}
