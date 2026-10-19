package backupwork

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// PrepareCheckpoint prepares receipt, source cursor and native attempt
// checkpoint as one change. The Controller joins source-run/Recovery Point
// mutations and authenticated-session guards before committing or acknowledging.
func (repository *InputRepository) PrepareCheckpoint(ctx context.Context, view workqueue.ResumeView,
	request *agentpb.BackupWorkCheckpointRequest, evidence CheckpointEvidence, at time.Time,
) (workqueue.Change, *agentpb.BackupWorkCheckpointAck, error) {
	admitted, err := repository.InspectCheckpoint(ctx, view, request)
	if err != nil {
		return workqueue.Change{}, nil, err
	}
	if admitted.Replay != nil {
		return workqueue.Change{Conditions: admitted.Guards}, admitted.Replay, nil
	}
	attempt, item, task := view.Attempt.Attempt, view.Work.Item.Item, view.Work.Task.Task
	if at.IsZero() || attempt.StartedAt == nil || at.Before(*attempt.StartedAt) {
		return workqueue.Change{}, nil, conflict("Backup checkpoint timestamp is invalid")
	}
	report := admitted.Request
	grantsPublication := admitted.Assignment.Input.Operation == agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_RESTORE &&
		report.GetConfig().GetTransferCompleted() != nil
	if (checkpointGrantsEffect(report) || grantsPublication) &&
		(task.Stop != nil || item.ForwardRevoked || !at.Before(attempt.Deadline) || !at.Before(task.Deadline)) {
		return workqueue.Change{}, nil, conflict("Backup cannot start another effect after execution was stopped")
	}
	next, err := advanceCheckpoint(admitted, attempt.Executor, evidence, view.Work.ReadRevision)
	if err != nil {
		return workqueue.Change{}, nil, err
	}
	change, err := repository.prepareCheckpointWrite(
		ctx,
		view,
		admitted,
		admitted.Assignment.Input,
		len(admitted.Assignment.Sources),
		next,
		evidence.Conditions,
		at,
	)
	return change, nil, err
}

func (repository *InputRepository) prepareCheckpointWrite(ctx context.Context, view workqueue.ResumeView,
	admitted CheckpointAdmission, input *agentpb.BackupWorkInput, sourceCount int,
	next *agentpb.BackupStepResume, evidenceGuards []keyvalue.Condition, at time.Time,
) (workqueue.Change, error) {
	attempt, item, task := view.Attempt.Attempt, view.Work.Item.Item, view.Work.Task.Task
	report := admitted.Request
	progress, _, err := repository.prepareProgressAdvance(ctx, view, report, sourceCount, next)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clear(progress)
	raw, err := encodeCheckpointReceipt(report, next)
	if err != nil {
		return workqueue.Change{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(raw)
	digest, err := executionplan.BackupWorkCheckpointDigest(report)
	if err != nil {
		return workqueue.Change{}, err
	}
	sequence := uint64(1)
	if view.Checkpoint.Record != nil {
		if view.Checkpoint.Record.Sequence == ^uint64(0) {
			return workqueue.Change{}, conflict("Backup execution checkpoint sequence is exhausted")
		}
		sequence = view.Checkpoint.Record.Sequence + 1
	}
	checkpoint := workexecution.Checkpoint{AttemptID: attempt.ID, Sequence: sequence,
		StageID: input.Steps[report.SourceOrdinal].StepId, RecordedAt: at,
		Evidence: workexecution.Input{ID: attempt.Input.ID, SHA256: hex.EncodeToString(digest)}}
	change, err := workqueue.PrepareCheckpoint(view.Work.Item, view.Attempt, view.Checkpoint, checkpoint)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, admitted.Guards...)
	change.Conditions = append(change.Conditions, evidenceGuards...)
	change.Conditions = append(change.Conditions,
		keyvalue.Condition{Key: workqueue.TaskKey(task.ID), ModRevision: view.Work.Task.Revision})
	change.Mutations = append(
		change.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: admitted.ReceiptKey, Value: append([]byte(nil), raw...)},
		keyvalue.Mutation{
			Type:  keyvalue.MutationPut,
			Key:   ProgressKey(item.ID),
			Value: append([]byte(nil), progress...),
		},
	)
	return change, nil
}

func (repository *InputRepository) prepareProgressAdvance(ctx context.Context, view workqueue.ResumeView,
	report *agentpb.BackupWorkCheckpointRequest, sourceCount int, next *agentpb.BackupStepResume,
) ([]byte, keyvalue.Condition, error) {
	key := ProgressKey(view.Work.Item.Item.ID)
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.Work.ReadRevision},
	)
	if err != nil {
		return nil, keyvalue.Condition{}, err
	}
	if read == nil {
		return nil, keyvalue.Condition{}, conflict("Backup progress is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision != view.Work.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil ||
		read.Values[0].Key != key || read.Values[0].ModRevision <= 0 || read.Values[0].ModRevision > read.ReadRevision {
		return nil, keyvalue.Condition{}, conflict("Backup progress snapshot changed")
	}
	record, err := recordcodec.Decode[Progress](read.Values[0].Value, "backup-work-progress")
	if err != nil {
		return nil, keyvalue.Condition{}, err
	}
	if len(record.Sources) != sourceCount || record.Input != *view.Work.Item.Item.Input ||
		record.TaskID != view.Work.Task.Task.ID || record.WorkItemID != view.Work.Item.Item.ID || record.Schema != 1 {
		return nil, keyvalue.Condition{}, conflict("Backup progress differs from admitted work")
	}
	resume, err := (proto.MarshalOptions{Deterministic: true}).Marshal(next)
	if err != nil {
		return nil, keyvalue.Condition{}, errs.Wrap(errs.KindInternal, err)
	}
	digest, err := executionplan.BackupWorkCheckpointDigest(report)
	if err != nil {
		return nil, keyvalue.Condition{}, err
	}
	record.Sources[report.SourceOrdinal] = SourceProgress{Resume: resume, CheckpointSHA256: hex.EncodeToString(digest)}
	encoded, err := recordcodec.Encode("backup-work-progress", record)
	if err != nil {
		return nil, keyvalue.Condition{}, err
	}
	if len(encoded) > executionplan.MaximumPlanBytes {
		clear(encoded)
		return nil, keyvalue.Condition{}, conflict("Backup progress exceeds its storage limit")
	}
	return encoded, keyvalue.Condition{Key: key, ModRevision: read.Values[0].ModRevision}, nil
}
