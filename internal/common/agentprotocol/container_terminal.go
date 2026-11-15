package agentprotocol

import (
	"strings"

	"github.com/AlanD20/groundplane/internal/common/serviceobservation"
	"github.com/AlanD20/groundplane/internal/common/terminalsession"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func ContainerOptionsFromWire(request *agentpb.ContainerTerminalOpen) (terminalsession.ContainerOptions, error) {
	if request == nil || len(request.Sources) == 0 || len(request.Sources) > 2 ||
		len(request.ContainerId) != 64 || strings.Trim(request.ContainerId, "0123456789abcdef") != "" ||
		request.Columns > 500 || request.Rows > 300 || request.InitialCredit == 0 || request.InitialCredit > 64 {
		return terminalsession.ContainerOptions{}, errs.New(
			errs.KindValidationFailed,
			"container terminal request is invalid",
		)
	}
	first := request.Sources[0]
	for _, source := range request.Sources {
		if serviceobservation.ValidateTarget(source) != nil || source.ServiceId != first.ServiceId ||
			source.EnvironmentId != first.EnvironmentId {
			return terminalsession.ContainerOptions{}, errs.New(
				errs.KindValidationFailed,
				"container terminal sources are inconsistent",
			)
		}
	}
	options := terminalsession.ContainerOptions{
		SessionID:              request.SessionId,
		User:                   request.User,
		Command:                append([]string(nil), request.Command...),
		WorkingDirectory:       request.WorkingDirectory,
		Size:                   terminalsession.Size{Columns: uint16(request.Columns), Rows: uint16(request.Rows)},
		InputInactivitySeconds: int(request.InputInactivitySeconds),
	}
	return options, options.Validate()
}
