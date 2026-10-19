package agentchannel

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/backupsecret"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// sendBackupWorkSecrets consumes transient slots opened by the credential grant
// owner. Neither successful framing nor sending a frame grants execution rights.
func sendBackupWorkSecrets(
	ctx context.Context,
	stream agentpb.AgentChannel_ConnectServer,
	input *agentpb.BackupWorkInput,
	attempt *agentpb.WorkAttemptIdentity,
	ordinal uint32,
	identity workexecution.Identity,
	slots map[agentpb.BackupSecretSlotPurpose][]byte,
) error {
	defer clearBackupSecretSlots(slots)
	if ctx == nil || stream == nil || input == nil || attempt == nil ||
		uint64(ordinal) >= uint64(len(input.Steps)) {
		return errs.New(errs.KindInternal, "Backup work credential delivery is incomplete")
	}
	purposes, err := backupsecret.SourcePurposes(input.Steps[ordinal])
	if err != nil {
		return err
	}
	if len(slots) != len(purposes) {
		return errs.New(errs.KindInternal, "Backup credential resolver returned the wrong slot set")
	}
	// Validate every slot before emitting any bytes, so a missing/oversized slot
	// cannot leave the receiver waiting after a partial set was sent.
	for _, purpose := range purposes {
		header := backupWorkSecretHeader(attempt, ordinal, purpose, len(slots[purpose]))
		if _, err := executionplan.NewBackupWorkSecretValidator(header, input, identity); err != nil {
			return err
		}
	}
	for _, purpose := range purposes {
		content := slots[purpose]
		header := backupWorkSecretHeader(attempt, ordinal, purpose, len(content))
		if err := sendBackupWorkSecretFrame(ctx, stream, header); err != nil {
			return err
		}
		count := header.GetHeader().ChunkCount
		for sequence := uint32(1); sequence <= count; sequence++ {
			start := uint64(sequence-1) * executionplan.MaximumBackupSecretChunkBytes
			end := min(start+executionplan.MaximumBackupSecretChunkBytes, uint64(len(content)))
			chunk := content[start:end]
			frame := &agentpb.BackupWorkSecretTransfer{Attempt: proto.CloneOf(attempt),
				SourceOrdinal: ordinal, Purpose: purpose,
				Record: &agentpb.BackupWorkSecretTransfer_Chunk{Chunk: &agentpb.BackupSecretSlotChunk{
					Sequence: sequence, Content: chunk}}}
			err := sendBackupWorkSecretFrame(ctx, stream, frame)
			clear(chunk)
			frame.GetChunk().Content = nil
			if err != nil {
				return err
			}
		}
		if err := sendBackupWorkSecretFrame(ctx, stream, &agentpb.BackupWorkSecretTransfer{
			Attempt: proto.CloneOf(attempt), SourceOrdinal: ordinal, Purpose: purpose,
			Record: &agentpb.BackupWorkSecretTransfer_End{End: &agentpb.BackupSecretSlotEnd{ChunkCount: count}},
		}); err != nil {
			return err
		}
	}
	return nil
}

func clearBackupSecretSlots(slots map[agentpb.BackupSecretSlotPurpose][]byte) {
	for purpose, content := range slots {
		clear(content)
		delete(slots, purpose)
	}
}

func backupWorkSecretHeader(attempt *agentpb.WorkAttemptIdentity, ordinal uint32,
	purpose agentpb.BackupSecretSlotPurpose, size int,
) *agentpb.BackupWorkSecretTransfer {
	total := uint64(size)
	var chunks uint32
	if total > 0 {
		chunks = uint32((total-1)/executionplan.MaximumBackupSecretChunkBytes + 1)
	}
	return &agentpb.BackupWorkSecretTransfer{Attempt: proto.CloneOf(attempt), SourceOrdinal: ordinal, Purpose: purpose,
		Record: &agentpb.BackupWorkSecretTransfer_Header{Header: &agentpb.BackupSecretSlotHeader{
			TotalBytes: total, ChunkCount: chunks}}}
}

func sendBackupWorkSecretFrame(ctx context.Context, stream agentpb.AgentChannel_ConnectServer,
	frame *agentpb.BackupWorkSecretTransfer,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return stream.Send(&agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_BackupWorkSecretTransfer{
		BackupWorkSecretTransfer: frame}})
}
