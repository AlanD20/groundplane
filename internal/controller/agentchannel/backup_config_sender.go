package agentchannel

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (s *Server) sendBackupConfigCapture(
	stream agentpb.AgentChannel_ConnectServer,
	claim etcd.TaskAssignment,
	assignment *agentpb.TaskAssignment,
	stepID string,
	credits *configTransferExchange,
) error {
	initial, err := credits.read(stream.Context(), stepID)
	if err != nil {
		return err
	}
	transport := &configCaptureChannelTransport{stream: stream, credits: credits, checkpointer: s.checkpoints,
		agentID: claim.Assignment.Record.AgentID, agentGeneration: claim.Assignment.Record.AgentGeneration,
		authority: assignment.BackupAuthority,
		binding: backupconfigtransfer.Binding{TaskID: initial.TaskId, AssignmentID: initial.AssignmentId,
			StepID: initial.StepId, ExecutionID: initial.ExecutionId, TransferID: initial.TransferId, Direction: initial.Direction}}
	return s.checkpoints.StreamBackupConfigCapture(stream.Context(), transport.agentID, transport.agentGeneration,
		assignment.BackupAuthority, stepID, initial, transport)
}

type configCaptureChannelTransport struct {
	stream          agentpb.AgentChannel_ConnectServer
	credits         *configTransferExchange
	checkpointer    BackupCheckpointer
	agentID         string
	agentGeneration uint64
	authority       *agentpb.BackupTaskAuthority
	binding         backupconfigtransfer.Binding
}

func (transport *configCaptureChannelTransport) SendFrame(
	ctx context.Context,
	frame *agentpb.BackupConfigTransfer,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return transport.stream.Send(&agentpb.ControllerMessage{
		Payload: &agentpb.ControllerMessage_BackupConfigTransfer{BackupConfigTransfer: frame},
	})
}

func (transport *configCaptureChannelTransport) ReadCredit(ctx context.Context) (*agentpb.BackupConfigCredit, error) {
	return transport.credits.read(ctx, transport.binding.StepID)
}

func (transport *configCaptureChannelTransport) CommitCredit(
	ctx context.Context,
	credit *agentpb.BackupConfigCredit,
) error {
	if err := transport.checkpointer.CommitBackupConfigCredit(ctx, transport.agentID, transport.agentGeneration,
		transport.authority, transport.binding, credit); err != nil {
		return err
	}
	return transport.stream.Send(&agentpb.ControllerMessage{
		Payload: &agentpb.ControllerMessage_BackupConfigCredit{BackupConfigCredit: credit},
	})
}
