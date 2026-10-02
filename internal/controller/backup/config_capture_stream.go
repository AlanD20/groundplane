package backup

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// StreamBackupConfigCapture admits the receiver's durably issued cursor against
// the native ledger and actual pinned source. It does not infer ownership from
// that cursor, re-resolve values, or publish a Recovery Point.
func (service *BackupCheckpointService) StreamBackupConfigCapture(
	ctx context.Context,
	agentID string,
	agentGeneration uint64,
	authority *agentpb.BackupTaskAuthority,
	stepID string,
	receiverCredit *agentpb.BackupConfigCredit,
	transport backupconfigtransfer.StreamTransport,
) error {
	snapshot, err := service.OpenBackupConfigCapture(ctx, agentID, agentGeneration, authority, stepID)
	if err != nil {
		return err
	}
	var step *agentpb.BackupStepAuthority
	for _, candidate := range authority.Steps {
		if candidate.StepId == stepID {
			step = candidate
			break
		}
	}
	if step == nil || receiverCredit == nil {
		return configSnapshotGuardConflict()
	}
	binding := backupconfigtransfer.Binding{TaskID: authority.TaskId, AssignmentID: authority.AssignmentId,
		StepID: stepID, ExecutionID: step.ExecutionId, TransferID: receiverCredit.TransferId,
		Direction: agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_CAPTURE}
	issued, err := backupconfigtransfer.ValidateCredit(binding, receiverCredit)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithDeadline(ctx, time.Unix(0, int64(step.StepDeadlineUnixNano)))
	defer cancel()
	claim, err := service.repository.GetBackupCheckpointAssignment(ctx, authority.TaskId)
	if err != nil {
		return err
	}
	cursor, found, err := service.repository.ReadConfigTransferCursor(ctx, authority.TaskId, authority.AssignmentId,
		stepID, step.ExecutionId, claim.ReadRevision)
	if err != nil {
		return err
	}
	var native []*agentpb.BackupConfigCredit
	if found {
		owner := cursor.Record.Owner
		if owner.Binding != binding || owner.AgentID != agentID || owner.AgentGeneration != agentGeneration ||
			owner.AssignmentGeneration != authority.AssignmentGeneration ||
			owner.AuthoritySHA256 != hex.EncodeToString(step.StepDigest) {
			return configSnapshotGuardConflict()
		}
		if err := service.repository.VisitConfigTransferCredits(ctx, cursor, func(credit *agentpb.BackupConfigCredit) error {
			native = append(native, credit)
			return nil
		}); err != nil {
			return err
		}
	} else {
		initial, err := backupconfigtransfer.InitialCredit(binding)
		if err != nil {
			return err
		}
		if !proto.Equal(initial, issued) {
			return configSnapshotGuardConflict()
		}
		if err := transport.CommitCredit(ctx, initial); err != nil {
			return err
		}
		native = []*agentpb.BackupConfigCredit{initial}
	}
	if found && len(native) != 0 && proto.Equal(native[len(native)-1], issued) {
		if err := transport.CommitCredit(ctx, issued); err != nil {
			return err
		}
	}
	_, err = snapshot.StreamCapture(ctx, binding, transport, native, issued)
	return err
}
