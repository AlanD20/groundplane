package attachremoval

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareDetachInspection settles only execution uncertainty. The original
// failure stays independently recorded even if inspection proves the consumer
// disconnected. No adapter command or arbitrary hook is replayed here.
func (repository *InputRepository) PrepareDetachInspection(ctx context.Context, view workqueue.ResumeView,
	report attachexecution.RemovalInspection, at time.Time, interrupted bool,
) (workqueue.Change, workexecution.Completion, error) {
	attempt := view.Attempt.Attempt
	if !view.ActiveAttempt || view.CurrentExecutor == interrupted || view.ExecutorRevision <= 0 ||
		attempt.FinishedAt != nil || attempt.Direction != workexecution.Recovery ||
		view.Work.Item.Item.Operation != workexecution.Detach || report.ObservedAt.After(at) ||
		interrupted && report.State != attachexecution.InspectionInterrupted {
		return workqueue.Change{}, workexecution.Completion{}, errs.New(
			errs.KindStateConflict,
			"Detach inspection no longer owns its recovery attempt",
		)
	}
	input, guards, err := repository.DetachRecoveryInput(ctx, view.Work, attempt)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	defer attachexecution.ClearRecoveryInput(&input)
	raw, evidence, err := attachexecution.EncodeRemovalInspection(input, report)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	authority, err := repository.DetachCaptureAdmission(ctx, view.Work)
	if err != nil {
		clear(raw)
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	resource, err := repository.PrepareDetachFailed(ctx, view.Work)
	if err != nil {
		clear(raw)
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	change := workqueue.Change{Conditions: append(guards, authority.Conditions...), Mutations: resource.Mutations}
	change.Conditions = append(change.Conditions, resource.Conditions...)
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: WorkInspectionKey(attempt.ID)},
		keyvalue.Condition{Key: workqueue.TaskKey(attempt.TaskID), ModRevision: view.Work.Task.Revision},
		keyvalue.Condition{Key: workqueue.ItemKey(attempt.WorkItemID), ModRevision: view.Work.Item.Revision},
		keyvalue.Condition{Key: workqueue.AttemptKey(attempt.ID), ModRevision: view.Attempt.Revision},
		keyvalue.Condition{Key: workqueue.ExecutorKey(attempt.Executor), ModRevision: view.ExecutorRevision})
	change.Mutations = append(
		change.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: WorkInspectionKey(attempt.ID), Value: raw},
	)
	if attempt.StartedAt != nil {
		checkpoint, err := report.Checkpoint(input)
		if err != nil {
			keyvalue.ClearMutationValues(change.Mutations)
			return workqueue.Change{}, workexecution.Completion{}, err
		}
		cursor, err := workqueue.PrepareCheckpoint(view.Work.Item, view.Attempt, view.Checkpoint, checkpoint)
		if err != nil {
			keyvalue.ClearMutationValues(change.Mutations)
			return workqueue.Change{}, workexecution.Completion{}, err
		}
		change.Conditions = append(change.Conditions, cursor.Conditions...)
		change.Mutations = append(change.Mutations, cursor.Mutations...)
		if report.Consumer != nil && report.Consumer.Detached {
			capture, err := repository.LoadDetachCapture(ctx, view.Work)
			if err != nil {
				keyvalue.ClearMutationValues(change.Mutations)
				return workqueue.Change{}, workexecution.Completion{}, err
			}
			defer capture.Clear()
			if capture.Input.Snapshot.Runtime == nil || input.Consumer == nil {
				keyvalue.ClearMutationValues(change.Mutations)
				return workqueue.Change{}, workexecution.Completion{}, recordcodec.CorruptRecord()
			}
			acknowledgement := serviceruntimerecord.Acknowledgement{
				TaskID:           attempt.TaskID,
				WorkItemID:       attempt.WorkItemID,
				AttemptID:        attempt.ID,
				Executor:         attempt.Executor,
				Input:            attempt.Input,
				Checkpoint:       checkpoint,
				RenderGeneration: capture.Input.Snapshot.Runtime.Source.RenderGeneration,
				AcknowledgedAt:   at,
			}
			settlement, err := repository.prepareConsumerAcknowledgement(
				ctx,
				view,
				capture.Input,
				acknowledgement,
				input.Consumer.Candidate,
			)
			if err != nil {
				keyvalue.ClearMutationValues(change.Mutations)
				return workqueue.Change{}, workexecution.Completion{}, err
			}
			change.Conditions = append(change.Conditions, settlement.Conditions...)
			change.Mutations = append(change.Mutations, settlement.Mutations...)
		}
	}
	outcome := workexecution.Failed
	if report.ExecutionStopped() {
		outcome = workexecution.Succeeded
	}
	return change, workexecution.Completion{
		Outcome:        outcome,
		Evidence:       evidence,
		EffectsSettled: report.ExecutionStopped(),
	}, nil
}

func (repository *InputRepository) PrepareInterruptedDetachInspection(ctx context.Context, view workqueue.ResumeView,
	at time.Time,
) (workqueue.Change, workexecution.Completion, error) {
	source, err := repository.LoadDetachRecovery(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	report := attachexecution.RemovalInspection{AttemptID: view.Attempt.Attempt.ID, ForwardID: source.Source.Forward.ID,
		Failure: source.Source.Evidence, CommandOrdinal: uint32(source.Source.Failure.Checkpoint.Sequence / 2),
		State: attachexecution.InspectionInterrupted, ObservedAt: at}
	return repository.PrepareDetachInspection(ctx, view, report, at, true)
}

func (repository *InputRepository) LoadDetachInspection(
	ctx context.Context,
	view workqueue.ResumeView,
) ([]byte, error) {
	if view.Work.Item.Item.Operation != workexecution.Detach {
		return nil, recordcodec.CorruptRecord()
	}
	input, _, err := repository.DetachRecoveryInput(ctx, view.Work, view.Attempt.Attempt)
	if err != nil {
		return nil, err
	}
	defer attachexecution.ClearRecoveryInput(&input)
	return LoadRemovalInspection(ctx, repository.store, view, input)
}

// LoadRemovalInspection verifies retained cleanup evidence independently of its
// diagnostic history. The operation owner supplies the captured recovery input.
func LoadRemovalInspection(ctx context.Context, reader Reader, view workqueue.ResumeView,
	input attachexecution.RecoveryInput,
) ([]byte, error) {
	attempt := view.Attempt.Attempt
	if !removalOperation(view.Work.Item.Item.Operation) || attempt.Direction != workexecution.Recovery ||
		attempt.FinishedAt == nil || attempt.Result == nil || !workexecution.SameAssignment(input.Attempt, attempt) {
		return nil, errs.New(errs.KindStateConflict, "Attach inspection has no retained terminal attempt")
	}
	key := WorkInspectionKey(attempt.ID)
	read, err := reader.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.Work.ReadRevision})
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != view.Work.ReadRevision || len(read.Values) != 1 ||
		read.Values[0] == nil || read.Values[0].Key != key || read.Values[0].ModRevision <= 0 {
		return nil, recordcodec.CorruptRecord()
	}
	if _, err := attachexecution.DecodeRemovalInspection(input, read.Values[0].Value, *attempt.Result); err != nil {
		return nil, err
	}
	return append([]byte(nil), read.Values[0].Value...), nil
}
