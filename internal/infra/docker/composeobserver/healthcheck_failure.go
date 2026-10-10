package composeobserver

import (
	"strconv"
	"strings"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/moby/moby/api/types/container"
)

// Preserve only a closed failure classification. Raw healthcheck output can
// contain credentials; it must not enter observations or Task history.
func healthcheckDiagnostic(config *container.Config, state *container.State) agentpb.ComposeHelperDiagnostic {
	if config == nil || config.Healthcheck == nil || state == nil || state.Health == nil ||
		state.Health.Status == container.Healthy || len(state.Health.Log) == 0 {
		return agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_UNSPECIFIED
	}
	latest := state.Health.Log[len(state.Health.Log)-1]
	if latest == nil {
		return agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_UNSPECIFIED
	}
	test := config.Healthcheck.Test
	// A command that cannot start has no process exit status. Docker records -1
	// and the runtime error; do not confuse it with a probe timeout (also -1).
	if executable := healthcheckStartExecutable(config, test); latest.ExitCode == -1 && executable != "" {
		command := "exec: " + strconv.Quote(executable) + ": "
		if strings.Contains(latest.Output, command+"executable file not found in $PATH") ||
			strings.Contains(latest.Output, command+"stat "+executable+": no such file or directory") {
			if test[0] == "CMD" {
				return missingHealthcheckExecutable(test)
			}
			return agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_HEALTHCHECK_COMMAND_MISSING
		}
		if strings.Contains(latest.Output, command+"permission denied") {
			return agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_HEALTHCHECK_COMMAND_NOT_EXECUTABLE
		}
	}
	if latest.ExitCode == 126 {
		return agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_HEALTHCHECK_COMMAND_NOT_EXECUTABLE
	}
	if latest.ExitCode != 127 {
		return agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_UNSPECIFIED
	}
	return missingHealthcheckExecutable(test)
}

func healthcheckStartExecutable(config *container.Config, test []string) string {
	if len(test) < 2 {
		return ""
	}
	switch test[0] {
	case "CMD":
		return test[1]
	case "CMD-SHELL":
		if len(config.Shell) > 0 {
			return config.Shell[0]
		}
		return "/bin/sh"
	default:
		return ""
	}
}

func missingHealthcheckExecutable(test []string) agentpb.ComposeHelperDiagnostic {
	if len(test) > 1 && test[0] == "CMD" {
		switch test[1] {
		case "pgrep":
			return agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_HEALTHCHECK_PGREP_MISSING
		case "curl":
			return agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_HEALTHCHECK_CURL_MISSING
		case "nc":
			return agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_HEALTHCHECK_NC_MISSING
		}
	}
	if len(test) == 2 && test[0] == "CMD-SHELL" {
		switch {
		case quotedHealthcheckArgument(test[1], "pgrep -f -- ", " >/dev/null"):
			return agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_HEALTHCHECK_PGREP_MISSING
		case quotedHealthcheckArgument(test[1], "curl -fsS -- ", " >/dev/null"):
			return agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_HEALTHCHECK_CURL_MISSING
		case typedTCPHealthcheck(test[1]):
			return agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_HEALTHCHECK_NC_MISSING
		}
	}
	return agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_HEALTHCHECK_COMMAND_MISSING
}

func quotedHealthcheckArgument(command, prefix, suffix string) bool {
	if !strings.HasPrefix(command, prefix) || !strings.HasSuffix(command, suffix) {
		return false
	}
	return singleQuotedHealthcheckValue(strings.TrimSuffix(strings.TrimPrefix(command, prefix), suffix))
}

func singleQuotedHealthcheckValue(value string) bool {
	if len(value) < 2 || value[0] != '\'' || value[len(value)-1] != '\'' {
		return false
	}
	// GP escapes a literal quote using this exact shell sequence. Anything else
	// outside the single-quoted value could be another command, not this tool.
	return !strings.Contains(strings.ReplaceAll(value[1:len(value)-1], "'\"'\"'", ""), "'")
}

func typedTCPHealthcheck(command string) bool {
	if !strings.HasPrefix(command, "nc -z -- ") {
		return false
	}
	host, port, ok := strings.Cut(strings.TrimPrefix(command, "nc -z -- "), "' '")
	return ok && singleQuotedHealthcheckValue(host+"'") && singleQuotedHealthcheckValue("'"+port)
}
