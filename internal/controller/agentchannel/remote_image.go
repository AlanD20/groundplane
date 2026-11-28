package agentchannel

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type RemoteImageReporter interface {
	ReportRemoteImage(
		context.Context,
		agentregistration.WorkAuthorization,
		*agentpb.RemoteImageResultRequest,
	) (*agentpb.RemoteImageResultAck, error)
}

func (server *Server) ConfigureRemoteImage(owner RemoteImageReporter) error {
	if server == nil || owner == nil || server.remoteImage != nil {
		return errs.New(errs.KindInternal, "remote image Fetch reporting is missing or already configured")
	}
	server.remoteImage = owner
	return nil
}

func (server *Server) reportRemoteImage(
	ctx context.Context,
	authorization agentregistration.WorkAuthorization,
	request *agentpb.RemoteImageResultRequest,
) (*agentpb.ControllerMessage, error) {
	if server.remoteImage == nil {
		return nil, errs.New(errs.KindStateConflict, "remote image Fetch reporting is unavailable")
	}
	ack, err := server.remoteImage.ReportRemoteImage(ctx, authorization, request)
	if err != nil {
		return nil, err
	}
	return &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_RemoteImageResultAck{
		RemoteImageResultAck: ack,
	}}, nil
}
