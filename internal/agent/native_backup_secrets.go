package agent

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (exchange *nativeExchange) RegisterBackupSecrets(assignment *agentpb.BackupWorkAssignment) error {
	exchange.mu.Lock()
	defer exchange.mu.Unlock()
	if exchange.closed {
		return errs.New(errs.KindStateConflict, "Backup credential session is closed")
	}
	return exchange.backupSecrets.Register(assignment, exchange.identity)
}

func (exchange *nativeExchange) RegisterBackupRecoverySecrets(assignment *agentpb.BackupRecoveryAssignment) error {
	exchange.mu.Lock()
	defer exchange.mu.Unlock()
	if exchange.closed {
		return errs.New(errs.KindStateConflict, "Backup credential session is closed")
	}
	return exchange.backupSecrets.RegisterRecovery(assignment, exchange.identity)
}

func (exchange *nativeExchange) RequestBackupSecrets(
	ctx context.Context,
	attempt *agentpb.WorkAttemptIdentity,
	ordinal uint32,
) error {
	if ctx == nil {
		return errs.New(errs.KindValidationFailed, "Backup credential request context is required")
	}
	exchange.mu.Lock()
	_, err := workexecution.DecodeAgentAttempt(attempt, exchange.identity)
	closed := exchange.closed
	exchange.mu.Unlock()
	if err != nil {
		return err
	}
	if closed {
		return errs.New(errs.KindStateConflict, "Backup credential session is closed")
	}
	message := &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_BackupWorkSecretRequest{
		BackupWorkSecretRequest: &agentpb.BackupWorkSecretRequest{
			Attempt:       proto.CloneOf(attempt),
			SourceOrdinal: ordinal,
		}}}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-exchange.done:
		return errs.New(errs.KindStateConflict, "Backup credential session is closed")
	case exchange.outgoing <- message:
		return nil
	}
}

func (exchange *nativeExchange) ReleaseBackupSecrets(attemptID string) {
	exchange.backupSecrets.Release(attemptID)
}

func (exchange *nativeExchange) ConsumeBackupSecret(ctx context.Context, attempt *agentpb.WorkAttemptIdentity,
	ordinal uint32, purpose agentpb.BackupSecretSlotPurpose, consume func([]byte) error,
) error {
	return exchange.backupSecrets.Consume(ctx, attempt, ordinal, purpose, consume)
}

func (exchange *nativeExchange) acceptBackupSecrets(
	ctx context.Context,
	transfer *agentpb.BackupWorkSecretTransfer,
) error {
	if chunk := transfer.GetChunk(); chunk != nil {
		defer func() {
			clear(chunk.Content)
			chunk.Content = nil
		}()
	}
	exchange.mu.Lock()
	defer exchange.mu.Unlock()
	if exchange.closed {
		return errs.New(errs.KindStateConflict, "Backup credential session is closed")
	}
	return exchange.backupSecrets.Accept(ctx, transfer)
}
