package checkpointdelivery

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type BackupFailureExchange func(context.Context, *agentpb.BackupWorkFailureRequest) (*agentpb.BackupWorkFailureAck, error)

// Fail delivers pending command evidence before persisting a stopped failure.
// It never calls the source executor, including when replaying a lost ack.
func (delivery *Backup) Fail(ctx context.Context, reason executionplan.BackupFailureReason,
	exchange BackupFailureExchange,
) (workexecution.Attempt, error) {
	if exchange == nil {
		return workexecution.Attempt{}, errs.New(errs.KindInternal, "Backup failure reporting is unavailable")
	}
	if err := delivery.Resume(ctx); err != nil {
		return workexecution.Attempt{}, err
	}
	delivery.mu.Lock()
	defer delivery.mu.Unlock()
	record, err := delivery.journal.PrepareBackupFailure(ctx, delivery.assignment, delivery.attempt, reason, time.Now().UTC())
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if record.Completion != nil {
		return *record.Completion, nil
	}
	ack, err := exchange(ctx, &agentpb.BackupWorkFailureRequest{Attempt: delivery.assignment.Attempt, Reason: string(record.FailureReason)})
	if err != nil {
		return workexecution.Attempt{}, err
	}
	return delivery.journal.AcknowledgeBackupFailure(ctx, delivery.attempt, delivery.assignment.Input, ack)
}
