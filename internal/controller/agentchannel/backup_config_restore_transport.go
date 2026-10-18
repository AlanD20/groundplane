package agentchannel

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (server *Server) receiveBackupConfigRestore(
	stream agentpb.AgentChannel_ConnectServer,
	authorization agentregistration.WorkAuthorization,
	assignment *agentpb.BackupWorkAssignment,
	ordinal uint32,
	exchange *configTransferExchange,
) error {
	binding, err := backupconfigtransfer.Bind(assignment, authorization.Identity(), ordinal,
		agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_RESTORE)
	if err != nil {
		return err
	}
	transport := &configRestoreChannelTransport{stream: stream, exchange: exchange, ordinal: ordinal}
	return server.backupWorkReports.StreamConfigRestore(stream.Context(), authorization, binding, transport)
}

type configRestoreChannelTransport struct {
	stream   agentpb.AgentChannel_ConnectServer
	exchange *configTransferExchange
	ordinal  uint32
}

func (transport *configRestoreChannelTransport) ReadFrame(ctx context.Context) (*agentpb.BackupConfigTransfer, error) {
	return transport.exchange.readRestoreFrame(ctx, transport.ordinal)
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
