package agentchannel

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (s *Server) sendBackupSecretSlots(
	ctx context.Context,
	stream agentpb.AgentChannel_ConnectServer,
	task etcd.TaskRecord,
	assignmentID string,
	plan *agentpb.ExecutionPlan,
	step *agentpb.ExecutionStep,
) error {
	if ctx == nil {
		return errs.New(errs.KindInternal, "backup secret slot context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.secrets == nil {
		return errs.New(errs.KindInternal, "backup secret slot resolver is not configured")
	}
	slots, err := s.secrets.ResolveBackupSecretSlots(ctx, task, plan, step)
	defer clearBackupSecretSlots(slots)
	if err != nil {
		return err
	}
	expected, err := expectedBackupSecretSlotPurposes(step.GetBackupSourceCapture())
	if err != nil || len(slots) != len(expected) {
		return errs.New(errs.KindInternal, "backup secret slot resolver returned an invalid purpose set")
	}
	for _, purpose := range expected {
		content, found := slots[purpose]
		if !found {
			return errs.New(errs.KindInternal, "backup secret slot resolver omitted a required purpose")
		}
		if err := sendBackupSecretSlot(
			ctx,
			stream,
			task.ID,
			assignmentID,
			step.GetStepId(),
			purpose,
			content,
		); err != nil {
			return err
		}
	}
	return nil
}

func sendBackupSecretSlot(
	ctx context.Context,
	stream agentpb.AgentChannel_ConnectServer,
	taskID string,
	assignmentID string,
	stepID string,
	purpose agentpb.BackupSecretSlotPurpose,
	content []byte,
) error {
	defer clear(content)
	if ctx == nil {
		return errs.New(errs.KindInternal, "backup secret slot context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if stream == nil || len(content) == 0 {
		return errs.New(errs.KindInternal, "backup secret slot source is invalid")
	}
	total := uint64(len(content))
	chunkCount := uint32((total-1)/executionplan.MaximumBackupSecretChunkBytes + 1)
	header := backupSecretControllerMessage(taskID, assignmentID, stepID, purpose)
	header.GetBackupSecretSlotTransfer().Record = &agentpb.BackupSecretSlotTransfer_Header{
		Header: &agentpb.BackupSecretSlotHeader{TotalBytes: total, ChunkCount: chunkCount},
	}
	if _, err := executionplan.NewBackupSecretSlotValidator(header.GetBackupSecretSlotTransfer()); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	if err := stream.Send(header); err != nil {
		return err
	}
	for sequence := uint32(1); sequence <= chunkCount; sequence++ {
		start := uint64(sequence-1) * executionplan.MaximumBackupSecretChunkBytes
		end := min(start+executionplan.MaximumBackupSecretChunkBytes, total)
		owned := append([]byte(nil), content[start:end]...)
		chunk := backupSecretControllerMessage(taskID, assignmentID, stepID, purpose)
		chunk.GetBackupSecretSlotTransfer().Record = &agentpb.BackupSecretSlotTransfer_Chunk{
			Chunk: &agentpb.BackupSecretSlotChunk{Sequence: sequence, Content: owned},
		}
		sendErr := stream.Send(chunk)
		clear(owned)
		chunk.GetBackupSecretSlotTransfer().GetChunk().Content = nil
		if sendErr != nil {
			return sendErr
		}
	}
	end := backupSecretControllerMessage(taskID, assignmentID, stepID, purpose)
	end.GetBackupSecretSlotTransfer().Record = &agentpb.BackupSecretSlotTransfer_End{
		End: &agentpb.BackupSecretSlotEnd{ChunkCount: chunkCount},
	}
	if err := stream.Send(end); err != nil {
		return err
	}
	return nil
}

func backupSecretControllerMessage(
	taskID string,
	assignmentID string,
	stepID string,
	purpose agentpb.BackupSecretSlotPurpose,
) *agentpb.ControllerMessage {
	return &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_BackupSecretSlotTransfer{
		BackupSecretSlotTransfer: &agentpb.BackupSecretSlotTransfer{
			TaskId: taskID, AssignmentId: assignmentID, StepId: stepID, Purpose: purpose,
		},
	}}
}

func expectedBackupSecretSlotPurposes(
	capture *agentpb.BackupSourceCapture,
) ([]agentpb.BackupSecretSlotPurpose, error) {
	if capture == nil {
		return nil, errs.New(errs.KindInternal, "backup source capture is required")
	}
	purposes := []agentpb.BackupSecretSlotPurpose{
		agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY,
		agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_SECRET_KEY,
	}
	if capture.GetEncryption() == agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE {
		purposes = append(purposes, agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_CURRENT_AGE_IDENTITY)
	} else if capture.GetEncryption() != agentpb.BackupEncryption_BACKUP_ENCRYPTION_NONE {
		return nil, errs.New(errs.KindInternal, "backup source encryption is invalid")
	}
	return purposes, nil
}

func clearBackupSecretSlots(slots map[agentpb.BackupSecretSlotPurpose][]byte) {
	for purpose, content := range slots {
		clear(content)
		delete(slots, purpose)
	}
}
