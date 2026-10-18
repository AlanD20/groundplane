package backingprovisionwork

import (
	"bytes"
	"context"
	"strconv"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backingprovision"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func ReportPrefix(attemptID string) string {
	return "/v1/execution/backing-provision/reports/" + attemptID + "/"
}
func ReportKey(attemptID string, sequence uint64) string {
	return ReportPrefix(attemptID) + strconv.FormatUint(sequence, 10)
}

// PrepareProgress joins the generic checkpoint and the bounded typed evidence.
// The operation owner must add file/hook proofs and current resource ownership
// before committing an effect grant. No report stores the captured artifact.
func (repository *Repository) PrepareProgress(ctx context.Context, view workqueue.ResumeView,
	progress backingprovision.Progress, at time.Time,
) (workqueue.Change, workexecution.Checkpoint, error) {
	input, capture, err := repository.Load(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, workexecution.Checkpoint{}, err
	}
	defer clear(input.Wire)
	procedure, err := input.Procedure()
	if err != nil {
		return workqueue.Change{}, workexecution.Checkpoint{}, err
	}
	layout, err := backingprovision.CaptureLayout(view.Attempt.Attempt, procedure)
	if err != nil {
		return workqueue.Change{}, workexecution.Checkpoint{}, err
	}
	raw, err := backingprovision.EncodeProgress(view.Attempt.Attempt, layout, progress)
	if err != nil {
		return workqueue.Change{}, workexecution.Checkpoint{}, err
	}
	defer clear(raw)
	next, err := progress.Checkpoint(view.Attempt.Attempt, layout)
	if err != nil {
		return workqueue.Change{}, workexecution.Checkpoint{}, err
	}
	key := ReportKey(view.Attempt.Attempt.ID, next.Sequence)
	keys := []string{key}
	if view.Checkpoint.Record != nil {
		keys = append(keys, ReportKey(view.Attempt.Attempt.ID, view.Checkpoint.Record.Sequence))
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: view.Work.ReadRevision})
	if err != nil {
		return workqueue.Change{}, workexecution.Checkpoint{}, err
	}
	if read == nil || read.ReadRevision != view.Work.ReadRevision || len(read.Values) != len(keys) {
		return workqueue.Change{}, workexecution.Checkpoint{}, invalid("backing creation checkpoint read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	for index, value := range read.Values {
		if value != nil && (value.Key != keys[index] || value.ModRevision <= 0 || value.ModRevision > view.Work.ReadRevision) {
			return workqueue.Change{}, workexecution.Checkpoint{}, invalid("backing creation report ownership is invalid")
		}
	}
	var previous *backingprovision.Progress
	if cursor := view.Checkpoint.Record; cursor != nil {
		if read.Values[1] == nil {
			return workqueue.Change{}, workexecution.Checkpoint{}, invalid("backing creation checkpoint lost its captured report")
		}
		saved, err := backingprovision.DecodeProgress(view.Attempt.Attempt, layout, read.Values[1].Value)
		if err != nil {
			return workqueue.Change{}, workexecution.Checkpoint{}, err
		}
		checkpoint, err := saved.Checkpoint(view.Attempt.Attempt, layout)
		if err != nil || checkpoint != *cursor {
			return workqueue.Change{}, workexecution.Checkpoint{}, invalid("backing creation report differs from its acknowledged cursor")
		}
		previous = &saved
	}
	if existing := read.Values[0]; existing != nil {
		if !bytes.Equal(existing.Value, raw) || view.Checkpoint.Record == nil || next.Sequence > view.Checkpoint.Record.Sequence {
			return workqueue.Change{}, workexecution.Checkpoint{}, invalid("backing creation report sequence already contains different evidence")
		}
		return workqueue.Change{Conditions: []keyvalue.Condition{capture,
			{Key: key, ModRevision: existing.ModRevision},
			{Key: workqueue.CheckpointKey(view.Attempt.Attempt.ID), ModRevision: view.Checkpoint.Revision}}}, next, nil
	}
	if !view.ActiveAttempt || view.Work.Task.Task.FinishedAt != nil || at.Before(progress.UpdatedAt) {
		return workqueue.Change{}, workexecution.Checkpoint{}, invalid("backing creation no longer accepts new execution evidence")
	}
	if progress.InFlight && (!view.CurrentExecutor || view.Work.Item.Item.ForwardRevoked || view.Work.Task.Task.Stop != nil ||
		!at.Before(view.Attempt.Attempt.Deadline)) {
		return workqueue.Change{}, workexecution.Checkpoint{}, invalid("backing creation cannot authorize another stage after stop or replacement")
	}
	if err := backingprovision.ValidateTransition(view.Attempt.Attempt, layout, previous, progress); err != nil {
		return workqueue.Change{}, workexecution.Checkpoint{}, err
	}
	if !progress.InFlight && progress.Last != nil && progress.Last.Stage == backingprovision.AfterStart {
		expected, err := backingprovision.HookSuccessEvidence(view.Attempt.Attempt, layout, progress.Last.ContainerID)
		if err != nil || expected != progress.Last.HookResult {
			return workqueue.Change{}, workexecution.Checkpoint{}, invalid("backing creation hook receipt differs from its captured execution")
		}
	}
	change, err := workqueue.PrepareCheckpoint(view.Work.Item, view.Attempt, view.Checkpoint, next)
	if err != nil {
		return workqueue.Change{}, workexecution.Checkpoint{}, err
	}
	change.Conditions = append(change.Conditions, capture, keyvalue.Condition{Key: key},
		keyvalue.Condition{Key: workqueue.TaskKey(view.Work.Task.Task.ID), ModRevision: view.Work.Task.Revision})
	if progress.InFlight {
		change.Conditions = append(change.Conditions,
			keyvalue.Condition{Key: workqueue.ExecutorKey(view.Attempt.Attempt.Executor), ModRevision: view.ExecutorRevision},
			keyvalue.Condition{Key: workqueue.HostForwardFenceKey(view.Attempt.Attempt.Executor.HostID)})
	}
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: bytes.Clone(raw)})
	return change, next, nil
}

func (repository *Repository) LoadProgress(ctx context.Context, view workqueue.ResumeView) (*backingprovision.Progress, error) {
	input, _, err := repository.Load(ctx, view.Work)
	if err != nil {
		return nil, err
	}
	defer clear(input.Wire)
	procedure, err := input.Procedure()
	if err != nil {
		return nil, err
	}
	layout, err := backingprovision.CaptureLayout(view.Attempt.Attempt, procedure)
	if err != nil {
		return nil, err
	}
	if view.Checkpoint.Record == nil {
		return nil, nil
	}
	key := ReportKey(view.Attempt.Attempt.ID, view.Checkpoint.Record.Sequence)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.Work.ReadRevision})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != view.Work.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil {
		return nil, invalid("backing creation checkpoint report is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	if value.Key != key || value.ModRevision <= 0 || value.ModRevision > view.Work.ReadRevision {
		return nil, invalid("backing creation checkpoint report revision is invalid")
	}
	progress, err := backingprovision.DecodeProgress(view.Attempt.Attempt, layout, value.Value)
	if err != nil {
		return nil, err
	}
	checkpoint, err := progress.Checkpoint(view.Attempt.Attempt, layout)
	if err != nil || checkpoint != *view.Checkpoint.Record {
		return nil, invalid("backing creation saved progress differs from its authoritative checkpoint")
	}
	return &progress, nil
}
