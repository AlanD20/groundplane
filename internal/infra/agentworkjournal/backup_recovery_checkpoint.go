package agentworkjournal

import (
	"bytes"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"google.golang.org/protobuf/proto"
)

func (record attemptRecord) validateBackupRecoveryCheckpoint() error {
	recovery := record.Backup.Recovery
	request, err := recovery.Checkpoint.Report()
	if err != nil {
		return err
	}
	if request == nil {
		if recovery.CheckpointWrites != 0 || len(recovery.Checkpoint.Acknowledgement) != 0 {
			return invalid()
		}
		return nil
	}
	if recovery.CheckpointWrites == 0 || request.SourceOrdinal != 0 || request.CheckpointSequence < recovery.CheckpointWrites {
		return invalid()
	}
	attempt, err := workexecution.DecodeAgentAttempt(request.Attempt, record.Attempt.Executor)
	if err != nil || !workexecution.SameAssignment(attempt, record.Attempt) {
		return invalid()
	}
	digest, err := executionplan.BackupWorkCheckpointDigest(request)
	if err != nil {
		return err
	}
	ack, err := recovery.Checkpoint.Ack()
	if err != nil || ack == nil {
		return err
	}
	if !proto.Equal(ack.Attempt, request.Attempt) || ack.SourceOrdinal != request.SourceOrdinal ||
		ack.CheckpointSequence != request.CheckpointSequence || !bytes.Equal(ack.RequestSha256, digest) ||
		ack.Committed == nil || ack.Committed.DedupeKeyModRevision <= request.GetPrecedingCheckpoint().GetDedupeKeyModRevision() ||
		ack.Source.GetRestore().GetCheckpointSequence() != ack.CheckpointSequence ||
		!proto.Equal(ack.Source.GetRestore().GetPrecedingCheckpoint(), ack.Committed) {
		return invalid()
	}
	return nil
}
