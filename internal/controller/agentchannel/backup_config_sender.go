package agentchannel

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (server *Server) sendBackupConfigCapture(stream agentpb.AgentChannel_ConnectServer,
	authorization agentregistration.WorkAuthorization, assignment *agentpb.BackupWorkAssignment,
	ordinal uint32, credits *configTransferExchange,
) error {
	if server.backupWorkReports == nil || credits == nil {
		return errs.New(errs.KindInternal, "native Config capture transport is unavailable")
	}
	binding, err := backupconfigtransfer.Bind(assignment, authorization.Identity(), ordinal,
		agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_CAPTURE)
	if err != nil {
		return err
	}
	initial, err := credits.read(stream.Context(), ordinal)
	if err != nil {
		return err
	}
	transport := &configCaptureChannelTransport{stream: stream, credits: credits,
		reports: server.backupWorkReports, authorization: authorization, binding: binding}
	return server.backupWorkReports.StreamConfigCapture(stream.Context(), authorization, binding, initial, transport)
}

type configCaptureChannelTransport struct {
	stream        agentpb.AgentChannel_ConnectServer
	credits       *configTransferExchange
	reports       BackupWorkReporter
	authorization agentregistration.WorkAuthorization
	binding       backupconfigtransfer.Binding
}

func (transport *configCaptureChannelTransport) SendFrame(
	ctx context.Context,
	frame *agentpb.BackupConfigTransfer,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return transport.stream.Send(&agentpb.ControllerMessage{
		Payload: &agentpb.ControllerMessage_BackupConfigTransfer{BackupConfigTransfer: frame}})
}

func (transport *configCaptureChannelTransport) ReadCredit(ctx context.Context) (*agentpb.BackupConfigCredit, error) {
	return transport.credits.read(ctx, transport.binding.SourceOrdinal)
}

func (transport *configCaptureChannelTransport) CommitCredit(
	ctx context.Context,
	credit *agentpb.BackupConfigCredit,
) error {
	if err := transport.reports.CommitConfigCaptureCredit(ctx, transport.authorization, transport.binding, credit); err != nil {
		return err
	}
	return transport.stream.Send(&agentpb.ControllerMessage{
		Payload: &agentpb.ControllerMessage_BackupConfigCredit{BackupConfigCredit: credit}})
}
