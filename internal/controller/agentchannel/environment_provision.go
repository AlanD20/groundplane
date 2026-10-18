package agentchannel

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type EnvironmentProvisionReporter interface {
	ReportEnvironmentProvision(context.Context, agentregistration.WorkAuthorization, *agentpb.EnvironmentProvisionResultRequest) (*agentpb.EnvironmentProvisionResultAck, error)
}

func (server *Server) ConfigureEnvironmentProvision(owner EnvironmentProvisionReporter) error {
	if server == nil || owner == nil || server.environmentProvision != nil {
		return errs.New(errs.KindInternal, "Environment provisioning reporting is missing or already configured")
	}
	server.environmentProvision = owner
	return nil
}

func (server *Server) environmentProvisionReport(session *Session, generation uint64, message *agentpb.AgentMessage) (bool, *agentpb.ControllerMessage, error) {
	request := message.GetEnvironmentProvisionResultRequest()
	if request == nil {
		return false, nil, nil
	}
	if server.environmentProvision == nil {
		return true, nil, taskStoreStatus(errs.New(errs.KindInternal, "Environment provisioning reporting is unavailable"))
	}
	authorization, err := server.authenticatedWorkIdentity(session.ctx, session, generation)
	if err != nil {
		return true, nil, taskStoreStatus(err)
	}
	ack, err := server.environmentProvision.ReportEnvironmentProvision(session.ctx, authorization, request)
	if err != nil {
		return true, nil, taskStoreStatus(err)
	}
	return true, &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_EnvironmentProvisionResultAck{EnvironmentProvisionResultAck: ack}}, nil
}
