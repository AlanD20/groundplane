package agentchannel

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type BackupWorkSecretResolver interface {
	ResolveWorkSecretSlots(context.Context, agentregistration.WorkAuthorization, *agentpb.WorkAttemptIdentity, uint32) (*agentpb.BackupWorkInput, map[agentpb.BackupSecretSlotPurpose][]byte, error)
}

func (server *Server) ConfigureBackupWorkSecrets(resolver BackupWorkSecretResolver) error {
	if server == nil || resolver == nil || server.backupWorkSecrets != nil {
		return errs.New(errs.KindInternal, "native Backup credential configuration is invalid")
	}
	server.backupWorkSecrets = resolver
	return nil
}

func (server *Server) deliverBackupWorkSecrets(stream agentpb.AgentChannel_ConnectServer, session *Session,
	generation uint64, request *agentpb.BackupWorkSecretRequest,
) error {
	if request == nil || executionplan.RejectUnknown(request) != nil {
		return errs.New(errs.KindValidationFailed, "Backup credential request is invalid")
	}
	if server.backupWorkSecrets == nil {
		return errs.New(errs.KindInternal, "native Backup credentials are not configured")
	}
	authorization, err := server.authenticatedWorkIdentity(session.ctx, session, generation)
	if err != nil {
		return err
	}
	assignment, slots, err := server.backupWorkSecrets.ResolveWorkSecretSlots(session.ctx, authorization, request.Attempt, request.SourceOrdinal)
	defer clearBackupSecretSlots(slots)
	if err != nil {
		return err
	}
	return sendBackupWorkSecrets(session.ctx, stream, assignment, request.Attempt, request.SourceOrdinal, authorization.Identity(), slots)
}
