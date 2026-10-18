package backupwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// ReadRestoreRecoveryCheckpointAcknowledgement proves the recovery report at
// its commit revision, without recasting recovery as a forward assignment or
// replacing the original source cursor with a newer observation.
func (repository *InputRepository) ReadRestoreRecoveryCheckpointAcknowledgement(ctx context.Context,
	request *agentpb.BackupWorkCheckpointRequest, assignment *agentpb.BackupRecoveryAssignment,
	identity workexecution.Identity, revision int64,
) (*agentpb.BackupWorkCheckpointAck, error) {
	validated, err := executionplan.ValidateBackupRestoreRecoveryCheckpoint(request, assignment, identity)
	if err != nil {
		return nil, err
	}
	raw, err := repository.readCheckpointReceipt(ctx, validated, revision)
	if err != nil {
		return nil, err
	}
	defer clear(raw)
	ack, err := checkpointReceiptAcknowledgement(raw, validated, assignment.Input, revision)
	if err != nil {
		return nil, err
	}
	return executionplan.ValidateBackupRestoreRecoveryCheckpointAck(ack, validated, assignment, identity)
}
