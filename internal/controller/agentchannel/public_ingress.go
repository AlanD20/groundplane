package agentchannel

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type PublicIngressReporter interface {
	ReportPublicIngress(
		context.Context,
		agentregistration.WorkAuthorization,
		*agentpb.PublicIngressResultRequest,
	) (*agentpb.PublicIngressResultAck, error)
}

func (server *Server) ConfigurePublicIngress(owner PublicIngressReporter) error {
	if server == nil || owner == nil || server.publicIngress != nil {
		return errs.New(errs.KindInternal, "public ingress reporting is missing or already configured")
	}
	server.publicIngress = owner
	return nil
}

func (server *Server) reportPublicIngress(
	ctx context.Context,
	authorization agentregistration.WorkAuthorization,
	request *agentpb.PublicIngressResultRequest,
) (*agentpb.ControllerMessage, error) {
	if server.publicIngress == nil {
		return nil, errs.New(errs.KindStateConflict, "public ingress reporting is unavailable")
	}
	ack, err := server.publicIngress.ReportPublicIngress(ctx, authorization, request)
	if err != nil {
		return nil, err
	}
	return &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_PublicIngressResultAck{
		PublicIngressResultAck: ack,
	}}, nil
}
