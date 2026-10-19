package agentchannel

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"

	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupvolumemanifest"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type BackupWorkReporter interface {
	PrepareConfigRecovery(
		context.Context,
		agentregistration.WorkAuthorization,
		*agentpb.WorkAttemptIdentity,
		uint32,
	) (*agentpb.BackupConfigRecoveryPrepareAck, error)
	StreamConfigRestore(
		context.Context,
		agentregistration.WorkAuthorization,
		backupconfigtransfer.Binding,
		backupconfigtransfer.ReceiverTransport,
	) error
	StreamConfigCapture(
		context.Context,
		agentregistration.WorkAuthorization,
		backupconfigtransfer.Binding,
		*agentpb.BackupConfigCredit,
		backupconfigtransfer.StreamTransport,
	) error
	CommitConfigCaptureCredit(
		context.Context,
		agentregistration.WorkAuthorization,
		backupconfigtransfer.Binding,
		*agentpb.BackupConfigCredit,
	) error
	ResolveSourceRecovery(
		context.Context,
		workexecution.Identity,
		*agentpb.BackupRecoveredStage,
	) (*agentpb.BackupStagingDisposition, []keyvalue.Condition, error)
	ResolveVolumeRecovery(
		context.Context,
		workexecution.Identity,
		*agentpb.BackupRecoveredVolumeRestore,
	) (*agentpb.BackupVolumeRestoreRecoveryDisposition, []keyvalue.Condition, error)
	ReadRestoreNewVolumeManifest(
		context.Context,
		agentregistration.WorkAuthorization,
		*agentpb.WorkAttemptIdentity,
		uint32,
	) (backupvolumemanifest.Complete, error)
	BeginVolumeManifest(
		context.Context,
		agentregistration.WorkAuthorization,
		*agentpb.WorkAttemptIdentity,
		uint32,
		agentpb.BackupVolumeManifestDirection,
	) (*agentpb.BackupVolumeManifestAckCredit, bool, error)
	AcceptVolumeManifestFrame(
		context.Context,
		agentregistration.WorkAuthorization,
		*agentpb.WorkAttemptIdentity,
		*agentpb.BackupVolumeManifestTransfer,
	) (*agentpb.BackupVolumeManifestAckCredit, error)
	Checkpoint(
		context.Context,
		agentregistration.WorkAuthorization,
		*agentpb.BackupWorkCheckpointRequest,
	) (*agentpb.BackupWorkCheckpointAck, error)
	Complete(
		context.Context,
		agentregistration.WorkAuthorization,
		*agentpb.BackupWorkCompletionRequest,
	) (*agentpb.BackupWorkCompletionAck, error)
	Fail(
		context.Context,
		agentregistration.WorkAuthorization,
		*agentpb.BackupWorkFailureRequest,
	) (*agentpb.BackupWorkFailureAck, error)
	Recover(
		context.Context,
		agentregistration.WorkAuthorization,
		*agentpb.BackupRecoveryRequest,
	) (*agentpb.BackupRecoveryAck, error)
}

func (server *Server) ConfigureBackupWork(reports BackupWorkReporter) error {
	if server == nil || reports == nil || server.backupWorkReports != nil {
		return errs.New(errs.KindInternal, "native Backup reporting configuration is invalid")
	}
	server.backupWorkReports = reports
	return nil
}

func (server *Server) backupWorkReport(
	session *Session,
	generation uint64,
	message *agentpb.AgentMessage,
) (bool, *agentpb.ControllerMessage, error) {
	if message.GetBackupWorkCheckpointRequest() == nil && message.GetBackupConfigRecoveryPrepareRequest() == nil {
		return false, nil, nil
	}
	if server.backupWorkReports == nil {
		return true, nil, status.Error(codes.Internal, "native Backup reporting is not configured")
	}
	authorization, err := server.authenticatedWorkIdentity(session.ctx, session, generation)
	if err != nil {
		return true, nil, taskStoreStatus(err)
	}
	if request := message.GetBackupConfigRecoveryPrepareRequest(); request != nil {
		ack, err := server.backupWorkReports.PrepareConfigRecovery(
			session.ctx,
			authorization,
			request.Attempt,
			request.SourceOrdinal,
		)
		if err != nil {
			return true, nil, taskStoreStatus(err)
		}
		return true, &agentpb.ControllerMessage{
			Payload: &agentpb.ControllerMessage_BackupConfigRecoveryPrepareAck{BackupConfigRecoveryPrepareAck: ack},
		}, nil
	}
	ack, err := server.backupWorkReports.Checkpoint(
		session.ctx,
		authorization,
		message.GetBackupWorkCheckpointRequest(),
	)
	if err != nil {
		return true, nil, taskStoreStatus(err)
	}
	return true, &agentpb.ControllerMessage{
		Payload: &agentpb.ControllerMessage_BackupWorkCheckpointAck{BackupWorkCheckpointAck: ack},
	}, nil
}
