package backup

import (
	"bytes"
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// resumePreparedCapture is shared by capture kinds only after their resource
// run and any database execution guards have been matched. It never authorizes
// cleanup of a helper or invents artifact evidence from files found on disk.
func (owner *AgentWork) resumePreparedCapture(ctx context.Context, identity workexecution.Identity,
	entry *agentpb.BackupRecoveredStage, source backupwork.StagingSource,
) (*agentpb.BackupStagingDisposition, []keyvalue.Condition, error) {
	if entry == nil || source.Input == nil ||
		source.Input.Operation != agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_CAPTURE ||
		uint64(source.Ordinal) >= uint64(len(source.Sources)) ||
		uint64(source.Ordinal) >= uint64(len(source.Input.Steps)) ||
		executionplan.ValidateBackupStagingInventory(
			&agentpb.BackupStagingInventory{Entries: []*agentpb.BackupRecoveredStage{entry}},
		) != nil {
		return nil, nil, invalidCaptureStaging()
	}
	step := source.Input.Steps[source.Ordinal]
	key, err := executionplan.BackupSourceStagingKey(
		source.View.Task.Task.ID,
		step.ExecutionId,
		step.GetCapture().GetPointId(),
	)
	if err != nil || !bytes.Equal(key, entry.RecoveryKeySha256) {
		return nil, nil, invalidCaptureStaging()
	}
	prepared := source.Sources[source.Ordinal].GetCapture().GetPreparedArtifact()
	if prepared == nil || prepared.Evidence == nil || prepared.Finals == nil {
		return nil, nil, invalidCaptureStaging()
	}
	expected := []*agentpb.BackupRecoveredFile{{
		Role:      agentpb.BackupRecoveredFileRole_BACKUP_RECOVERED_FILE_ROLE_SOURCE_PLAINTEXT,
		SizeBytes: prepared.Evidence.SourceSizeBytes, Sha256: append([]byte(nil), prepared.Evidence.SourceSha256...),
	}}
	if !prepared.Finals.GetSameInode() {
		expected = append(expected, &agentpb.BackupRecoveredFile{
			Role:      agentpb.BackupRecoveredFileRole_BACKUP_RECOVERED_FILE_ROLE_STORED_OBJECT,
			SizeBytes: prepared.Evidence.StoredSizeBytes, Sha256: append([]byte(nil), prepared.Evidence.StoredSha256...),
		})
	}
	if len(entry.Files) != len(expected) {
		return nil, nil, invalidCaptureStaging()
	}
	for index, file := range expected {
		if !proto.Equal(file, entry.Files[index]) {
			return nil, nil, invalidCaptureStaging()
		}
	}
	assignment, guards, err := owner.stagingAssignment(ctx, identity, source)
	if err != nil {
		return nil, nil, err
	}
	digest, err := executionplan.BackupWorkStagingResumeSHA256(assignment, identity)
	if err != nil {
		return nil, nil, err
	}
	return &agentpb.BackupStagingDisposition{RecoveryKeySha256: append([]byte(nil), entry.RecoveryKeySha256...),
		Disposition: &agentpb.BackupStagingDisposition_ResumePrepared{ResumePrepared: &agentpb.BackupResumePrepared{
			AssignmentResumeSha256: digest, ExpectedFiles: expected,
			RemainingGrowth: agentpb.BackupRemainingGrowth_BACKUP_REMAINING_GROWTH_NO_GROWTH,
		}}}, guards, nil
}

func (owner *AgentWork) stagingAssignment(ctx context.Context, identity workexecution.Identity,
	source backupwork.StagingSource,
) (*agentpb.BackupWorkAssignment, []keyvalue.Condition, error) {
	item, task := source.View.Item.Item, source.View.Task.Task
	if item.ActiveAttemptID == "" || item.ForwardRevoked || task.Stop != nil || task.FinishedAt != nil {
		return nil, nil, invalidCaptureStaging()
	}
	view, err := owner.queue.ReadResumeAt(
		ctx,
		task.ID,
		item.ID,
		item.ActiveAttemptID,
		identity,
		source.View.ReadRevision,
	)
	if err != nil {
		return nil, nil, err
	}
	attempt := view.Attempt.Attempt
	now := time.Now().UTC()
	if !view.ActiveAttempt || !view.CurrentExecutor || attempt.Direction != workexecution.Forward ||
		attempt.StartedAt == nil || attempt.FinishedAt != nil ||
		!now.Before(attempt.Deadline) || !now.Before(item.Deadline) || !now.Before(task.Deadline) {
		return nil, nil, invalidCaptureStaging()
	}
	assignment, change, err := owner.inputs.LoadAssignment(ctx, view)
	if err != nil {
		return nil, nil, err
	}
	guards := append([]keyvalue.Condition(nil), source.Conditions...)
	guards = append(guards, change.Conditions...)
	guards = append(guards,
		keyvalue.Condition{Key: workqueue.AttemptKey(attempt.ID), ModRevision: view.Attempt.Revision},
		keyvalue.Condition{Key: workqueue.ExecutorKey(identity), ModRevision: view.ExecutorRevision})
	return assignment, guards, nil
}

func invalidCaptureStaging() error {
	return errs.New(
		errs.KindStateConflict,
		"Backup source files require matching captured artifact evidence and a live resume decision",
	)
}
