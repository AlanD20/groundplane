package composeruntime

import (
	context "context"

	composehelper "github.com/AlanD20/groundplane/internal/infra/docker/composehelper"
	agentpb "github.com/AlanD20/groundplane/proto/agentpb"
)

func completedComposeHelper() *fakeComposeHelper {
	return &fakeComposeHelper{response: &agentpb.ComposeHelperResponse{
		Schema: composehelper.SchemaVersion, Outcome: agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED,
		Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE,
	}}
}

type fakeComposeHelper struct {
	response *agentpb.ComposeHelperResponse
	err      error
	request  *agentpb.ComposeHelperRequest
}

func (helper *fakeComposeHelper) Execute(
	_ context.Context,
	request *agentpb.ComposeHelperRequest,
) (*agentpb.ComposeHelperResponse, error) {
	helper.request = request
	return helper.response, helper.err
}
