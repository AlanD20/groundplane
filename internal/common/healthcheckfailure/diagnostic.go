// Package healthcheckfailure owns safe explanations of healthcheck execution
// failures. It never receives command output or operator-supplied command text.
package healthcheckfailure

import "github.com/AlanD20/groundplane/proto/agentpb"

func Detail(diagnostic agentpb.ComposeHelperDiagnostic) string {
	switch diagnostic {
	case agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_HEALTHCHECK_PGREP_MISSING:
		return "healthcheck cannot run: pgrep is not installed in the container; use an image with pgrep or choose an available healthcheck"
	case agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_HEALTHCHECK_CURL_MISSING:
		return "healthcheck cannot run: curl is not installed in the container; use an image with curl or choose an available healthcheck"
	case agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_HEALTHCHECK_NC_MISSING:
		return "healthcheck cannot run: nc is not installed in the container; use an image with nc or choose an available healthcheck"
	case agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_HEALTHCHECK_COMMAND_MISSING:
		return "healthcheck cannot run: a required command was not found in the container; check the image and healthcheck command"
	case agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_HEALTHCHECK_COMMAND_NOT_EXECUTABLE:
		return "healthcheck cannot run: a required command is not executable in the container; check executable permissions and the healthcheck command"
	default:
		return ""
	}
}
