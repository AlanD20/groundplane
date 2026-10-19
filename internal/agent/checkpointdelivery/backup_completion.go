package checkpointdelivery

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type BackupCompletionExchange func(context.Context, *agentpb.BackupWorkCompletionRequest) (*agentpb.BackupWorkCompletionAck, error)

// Complete requires all local reports acknowledged. The Controller separately
// proves all captured sources settled; only its saved receipt retires evidence.
func (delivery *Backup) Complete(
	ctx context.Context,
	exchange BackupCompletionExchange,
) (workexecution.Attempt, error) {
	delivery.mu.Lock()
	defer delivery.mu.Unlock()
	if exchange == nil {
		return workexecution.Attempt{}, errs.New(errs.KindInternal, "Backup completion exchange is unavailable")
	}
	attempt, record, err := delivery.journal.LoadBackup(ctx, delivery.assignment, delivery.attempt.Executor)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if record == nil || attempt.StartedAt == nil || !attempt.StartedAt.Equal(*delivery.attempt.StartedAt) {
		return workexecution.Attempt{}, errs.New(
			errs.KindStateConflict,
			"Backup has no matching durable execution to complete",
		)
	}
	if record.Completion != nil {
		return *record.Completion, nil
	}
	evidence, err := record.CompletionEvidence(delivery.attempt.Input.ID)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	digest, err := hex.DecodeString(evidence.SHA256)
	if err != nil {
		return workexecution.Attempt{}, errs.Wrap(errs.KindInternal, err)
	}
	ack, err := exchange(ctx, &agentpb.BackupWorkCompletionRequest{
		Attempt:  proto.CloneOf(delivery.assignment.Attempt),
		Evidence: &agentpb.WorkInputReference{InputId: evidence.ID, Sha256: digest},
	})
	if err != nil {
		return workexecution.Attempt{}, err
	}
	return delivery.journal.CompleteBackup(ctx, delivery.attempt, delivery.assignment.Input, ack)
}
