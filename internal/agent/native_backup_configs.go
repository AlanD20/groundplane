package agent

import (
	"context"

	"github.com/AlanD20/groundplane/internal/agent/backupconfiguration"
	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (exchange *nativeExchange) RegisterBackupConfigs(assignment *agentpb.BackupWorkAssignment) error {
	exchange.mu.Lock()
	defer exchange.mu.Unlock()
	if exchange.closed {
		return errs.New(errs.KindStateConflict, "Config transfer session is closed")
	}
	return exchange.backupConfigs.Register(assignment, exchange.identity)
}

func (exchange *nativeExchange) ReleaseBackupConfigs(attemptID string) {
	exchange.backupConfigs.Release(attemptID)
}

func (exchange *nativeExchange) acceptBackupConfig(ctx context.Context, message *agentpb.ControllerMessage) (bool, error) {
	frame, credit := message.GetBackupConfigTransfer(), message.GetBackupConfigCredit()
	if frame == nil && credit == nil {
		return false, nil
	}
	if frame != nil {
		defer backupconfigtransfer.ClearFrame(frame)
	}
	if err := executionplan.RejectUnknown(message); err != nil {
		return true, err
	}
	exchange.mu.Lock()
	defer exchange.mu.Unlock()
	if exchange.closed {
		return true, errs.New(errs.KindStateConflict, "Config transfer session is closed")
	}
	if frame != nil {
		return true, exchange.backupConfigs.AcceptFrame(ctx, frame)
	}
	return true, exchange.backupConfigs.AcceptCredit(ctx, credit)
}

func (exchange *nativeExchange) CaptureConfigStream(binding backupconfigtransfer.Binding) (*backupconfiguration.CaptureStream, error) {
	return exchange.backupConfigs.Stream(binding, func(ctx context.Context, credit *agentpb.BackupConfigCredit) error {
		return exchange.sendConfig(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_BackupConfigCredit{BackupConfigCredit: proto.CloneOf(credit)}})
	})
}

func (exchange *nativeExchange) RestoreConfigStream(ctx context.Context, binding backupconfigtransfer.Binding) (*backupconfiguration.RestoreStream, error) {
	return exchange.backupConfigs.RestoreStream(ctx, binding, func(ctx context.Context, frame *agentpb.BackupConfigTransfer) error {
		owned := proto.CloneOf(frame)
		err := exchange.sendConfig(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_BackupConfigTransfer{BackupConfigTransfer: owned}})
		if err != nil {
			backupconfigtransfer.ClearFrame(owned)
		}
		return err
	})
}

func (exchange *nativeExchange) sendConfig(ctx context.Context, message *agentpb.AgentMessage) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-exchange.done:
		return errs.New(errs.KindRequestFailed, "Agent Config transfer stream closed")
	case exchange.outgoing <- message:
		return nil
	}
}
