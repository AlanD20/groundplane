package attachremoval

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareRemovalFailure records the original failure independently of cleanup.
// The operation owner must join its capture/resource guards and native attempt
// settlement. Only a saved between-command boundary proves effects settled.
func PrepareRemovalFailure(ctx context.Context, reader Reader, view workqueue.ResumeView,
	input attachexecution.RemovalInput, reason attachexecution.FailureReason, at time.Time, interrupted bool,
) (workqueue.Change, workexecution.Completion, error) {
	if !view.ActiveAttempt || view.CurrentExecutor == interrupted || view.ExecutorRevision <= 0 ||
		view.Attempt.Attempt.FinishedAt != nil || !removalOperation(view.Work.Item.Item.Operation) ||
		!workexecution.SameAssignment(view.Attempt.Attempt, input.Attempt) {
		return workqueue.Change{}, workexecution.Completion{}, errs.New(errs.KindStateConflict, "Attach failure no longer owns its active attempt")
	}
	authority, err := input.Authority()
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	checkpoint := view.Checkpoint.Record
	change := workqueue.Change{Conditions: []keyvalue.Condition{
		{Key: WorkFailureKey(input.Attempt.ID)},
		{Key: workqueue.TaskKey(input.Attempt.TaskID), ModRevision: view.Work.Task.Revision},
		{Key: workqueue.ItemKey(input.Attempt.WorkItemID), ModRevision: view.Work.Item.Revision},
		{Key: workqueue.AttemptKey(input.Attempt.ID), ModRevision: view.Attempt.Revision},
		{Key: workqueue.CheckpointKey(input.Attempt.ID), ModRevision: view.Checkpoint.Revision},
		{Key: workqueue.ExecutorKey(input.Attempt.Executor), ModRevision: view.ExecutorRevision}}}
	settled := checkpoint == nil
	if checkpoint != nil {
		key := WorkReportKey(input.Attempt.ID, checkpoint.Sequence)
		read, err := reader.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.Work.ReadRevision})
		if read != nil {
			defer keyvalue.ClearValues(read.Values)
		}
		if err != nil {
			return workqueue.Change{}, workexecution.Completion{}, err
		}
		if read == nil || read.ReadRevision != view.Work.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil ||
			read.Values[0].Key != key || read.Values[0].ModRevision <= 0 {
			return workqueue.Change{}, workexecution.Completion{}, recordcodec.CorruptRecord()
		}
		report, err := attachexecution.DecodeRemovalReport(input, read.Values[0].Value)
		if err != nil {
			return workqueue.Change{}, workexecution.Completion{}, err
		}
		expected, err := report.Checkpoint(input)
		if err != nil || !sameCheckpoint(expected, *checkpoint) || report.Progress.Complete(input) {
			return workqueue.Change{}, workexecution.Completion{}, errs.New(errs.KindStateConflict, "Attach failure differs from saved progress; completed work must report completion")
		}
		settled = !report.Progress.InFlight
		change.Conditions = append(change.Conditions, keyvalue.Condition{Key: key, ModRevision: read.Values[0].ModRevision})
	}
	failure := attachexecution.RemovalFailure{AttemptID: input.Attempt.ID, Input: input.Attempt.Input, Authority: authority,
		Reason: reason, Checkpoint: checkpoint, RecordedAt: at}
	raw, evidence, err := attachexecution.EncodeRemovalFailure(input.Attempt, failure)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	outcome := workexecution.Failed
	if view.Work.Task.Task.Stop != nil {
		outcome = view.Work.Task.Task.Stop.Outcome
	} else if view.Work.Item.Item.ForwardRevoked {
		outcome = workexecution.Superseded
	}
	change.Mutations = []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: WorkFailureKey(input.Attempt.ID), Value: raw}}
	return change, workexecution.Completion{Outcome: outcome, Evidence: evidence, EffectsSettled: settled}, nil
}

func LoadRemovalFailure(ctx context.Context, reader Reader, view workqueue.ResumeView) ([]byte, error) {
	attempt := view.Attempt.Attempt
	if !removalOperation(view.Work.Item.Item.Operation) || attempt.FinishedAt == nil || attempt.Result == nil {
		return nil, errs.New(errs.KindStateConflict, "Attach failure has no retained terminal attempt")
	}
	key := WorkFailureKey(attempt.ID)
	read, err := reader.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.Work.ReadRevision})
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != view.Work.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil ||
		read.Values[0].Key != key || read.Values[0].ModRevision <= 0 {
		return nil, recordcodec.CorruptRecord()
	}
	if _, err := attachexecution.DecodeRemovalFailure(attempt, read.Values[0].Value, *attempt.Result); err != nil {
		return nil, err
	}
	return append([]byte(nil), read.Values[0].Value...), nil
}

func (repository *InputRepository) LoadDetachFailure(ctx context.Context, view workqueue.ResumeView) ([]byte, error) {
	if view.Work.Item.Item.Operation != workexecution.Detach {
		return nil, recordcodec.CorruptRecord()
	}
	return LoadRemovalFailure(ctx, repository.store, view)
}

func (repository *InputRepository) PrepareDetachFailure(ctx context.Context, view workqueue.ResumeView,
	input attachexecution.RemovalInput, reason attachexecution.FailureReason, at time.Time, interrupted bool,
) (workqueue.Change, workexecution.Completion, error) {
	capture, err := repository.LoadDetachCapture(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	defer capture.Clear()
	if err := validateDetachDeliverySource(ctx, capture, view, input); err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	change, completion, err := PrepareRemovalFailure(ctx, repository.store, view, input, reason, at, interrupted)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	resource, err := repository.PrepareDetachFailed(ctx, view.Work)
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	change.Conditions = append(change.Conditions, capture.Conditions...)
	change.Conditions = append(change.Conditions, resource.Conditions...)
	change.Mutations = append(change.Mutations, resource.Mutations...)
	return change, completion, nil
}
