package agentchannel

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type EnvironmentProvisionReporter interface {
	ReportEnvironmentProvision(
		context.Context,
		agentregistration.WorkAuthorization,
		*agentpb.EnvironmentProvisionResultRequest,
	) (*agentpb.EnvironmentProvisionResultAck, error)
}

func (server *Server) ConfigureEnvironmentProvision(owner EnvironmentProvisionReporter) error {
	if server == nil || owner == nil || server.environmentProvision != nil {
		return errs.New(errs.KindInternal, "Environment provisioning reporting is missing or already configured")
	}
	server.environmentProvision = owner
	return nil
}
