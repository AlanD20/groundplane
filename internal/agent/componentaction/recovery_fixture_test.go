package componentaction

import (
	context "context"
	errors "errors"

	managedconfighelpercontainer "github.com/AlanD20/groundplane/internal/infra/docker/managedconfighelpercontainer"
	agentpb "github.com/AlanD20/groundplane/proto/agentpb"
)

type componentActionRecoveringManagedExecutor struct {
	calls    int
	response *agentpb.ManagedConfigHelperResponse
}

func (*componentActionRecoveringManagedExecutor) Validate(
	context.Context,
	managedconfighelpercontainer.ValidatorImage,
	[]string,
	[]byte,
) error {
	return nil
}

func (executor *componentActionRecoveringManagedExecutor) Execute(
	context.Context,
	*agentpb.ManagedConfigHelperRequest,
) (*agentpb.ManagedConfigHelperResponse, error) {
	executor.calls++
	if executor.calls == 1 {
		return nil, errors.New("managed-config response was lost")
	}
	return executor.response, nil
}
