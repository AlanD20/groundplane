package composehelper

import "github.com/AlanD20/groundplane/proto/agentpb"

func completedResponse() *agentpb.ComposeHelperResponse {
	return &agentpb.ComposeHelperResponse{
		Schema: SchemaVersion, Outcome: agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED,
		Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE,
	}
}

func failedResponse(exitCode int32) *agentpb.ComposeHelperResponse {
	return &agentpb.ComposeHelperResponse{
		Schema: SchemaVersion, Outcome: agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_FAILED,
		ExitCode: exitCode, Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPOSE_FAILED,
	}
}
