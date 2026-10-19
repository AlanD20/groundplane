package agent

import (
	"context"
	"fmt"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (exchange *nativeExchange) BackupCheckpoint(ctx context.Context, input *agentpb.BackupWorkInput,
	request *agentpb.BackupWorkCheckpointRequest,
) (*agentpb.BackupWorkCheckpointAck, error) {
	exchange.mu.Lock()
	identity := exchange.identity
	exchange.mu.Unlock()
	owned, err := executionplan.ValidateBackupWorkCheckpoint(request, input, identity)
	if err != nil {
		return nil, err
	}
	response, err := exchange.request(ctx, &agentpb.AgentMessage{
		Payload: &agentpb.AgentMessage_BackupWorkCheckpointRequest{BackupWorkCheckpointRequest: owned},
	}, owned.Attempt, "backup_checkpoint", fmt.Sprintf("%d/%d", owned.SourceOrdinal, owned.CheckpointSequence))
	if err != nil {
		return nil, err
	}
	return executionplan.ValidateBackupWorkCheckpointAck(response.GetBackupWorkCheckpointAck(), owned, input, identity)
}

func (exchange *nativeExchange) CompleteBackup(
	ctx context.Context,
	request *agentpb.BackupWorkCompletionRequest,
) (*agentpb.BackupWorkCompletionAck, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{
		Payload: &agentpb.AgentMessage_BackupWorkCompletionRequest{BackupWorkCompletionRequest: request},
	}, request.GetAttempt(), "backup_completion", "")
	if err != nil {
		return nil, err
	}
	return response.GetBackupWorkCompletionAck(), nil
}

func (exchange *nativeExchange) BackupRecoveryCheckpoint(
	ctx context.Context,
	assignment *agentpb.BackupRecoveryAssignment,
	request *agentpb.BackupWorkCheckpointRequest,
) (*agentpb.BackupWorkCheckpointAck, error) {
	exchange.mu.Lock()
	identity := exchange.identity
	exchange.mu.Unlock()
	owned, err := executionplan.ValidateBackupRestoreRecoveryCheckpoint(request, assignment, identity)
	if err != nil {
		return nil, err
	}
	response, err := exchange.request(ctx, &agentpb.AgentMessage{
		Payload: &agentpb.AgentMessage_BackupWorkCheckpointRequest{BackupWorkCheckpointRequest: owned},
	}, owned.Attempt, "backup_checkpoint", fmt.Sprintf("%d/%d", owned.SourceOrdinal, owned.CheckpointSequence))
	if err != nil {
		return nil, err
	}
	return executionplan.ValidateBackupRestoreRecoveryCheckpointAck(
		response.GetBackupWorkCheckpointAck(),
		owned,
		assignment,
		identity,
	)
}

func (exchange *nativeExchange) FailBackup(
	ctx context.Context,
	request *agentpb.BackupWorkFailureRequest,
) (*agentpb.BackupWorkFailureAck, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{
		Payload: &agentpb.AgentMessage_BackupWorkFailureRequest{BackupWorkFailureRequest: request},
	}, request.GetAttempt(), "backup_failure", "")
	if err != nil {
		return nil, err
	}
	return response.GetBackupWorkFailureAck(), nil
}

func (exchange *nativeExchange) RecoverBackup(
	ctx context.Context,
	request *agentpb.BackupRecoveryRequest,
) (*agentpb.BackupRecoveryAck, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{
		Payload: &agentpb.AgentMessage_BackupRecoveryRequest{BackupRecoveryRequest: request},
	}, request.GetAttempt(), "backup_prune_recovery", "")
	if err != nil {
		return nil, err
	}
	return response.GetBackupRecoveryAck(), nil
}
