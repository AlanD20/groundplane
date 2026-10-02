package agentchannel

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (server *Server) receiveBackupConfigRestore(
	stream agentpb.AgentChannel_ConnectServer,
	claim etcd.TaskAssignment,
	assignment *agentpb.TaskAssignment,
	stepID string,
	exchange *configTransferExchange,
) error {
	transport := &configRestoreChannelTransport{stream: stream, exchange: exchange, stepID: stepID}
	return server.checkpoints.StreamBackupConfigRestore(stream.Context(), claim.Assignment.Record.AgentID,
		claim.Assignment.Record.AgentGeneration, assignment.BackupAuthority, stepID, transport)
}

type configRestoreChannelTransport struct {
	stream   agentpb.AgentChannel_ConnectServer
	exchange *configTransferExchange
	stepID   string
}

func (transport *configRestoreChannelTransport) ReadFrame(ctx context.Context) (*agentpb.BackupConfigTransfer, error) {
	return transport.exchange.readRestoreFrame(ctx, transport.stepID)
}

func (transport *configRestoreChannelTransport) SendCredit(
	ctx context.Context,
	credit *agentpb.BackupConfigCredit,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := transport.exchange.prepareRestoreCredit(credit); err != nil {
		return err
	}
	return transport.stream.Send(&agentpb.ControllerMessage{
		Payload: &agentpb.ControllerMessage_BackupConfigCredit{BackupConfigCredit: credit},
	})
}
