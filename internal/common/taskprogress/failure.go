// Package taskprogress provides output-free failure observations for operation
// diagnostics and software handoff.
package taskprogress

import (
	"context"
	"errors"
	"io/fs"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Failure is an output-free observation, not a new error type or execution
// authority. Its closed code catalog never contains arbitrary error messages.
type Failure struct {
	Code     string `json:"code"`
	ExitCode int32  `json:"exit_code,omitempty"`
}

var failureSummaries = map[string]string{
	"step_timed_out":                     "The operation exceeded its time limit.",
	"execution_canceled":                 "Execution was canceled before this operation completed.",
	"permission_denied":                  "The operation was denied access to a required file or directory.",
	"file_not_found":                     "A required file or directory was not found.",
	"image_unavailable":                  "The required workload image could not be resolved on the Agent host.",
	"image_resolution_busy":              "Image selection could not finish because another image operation holds it.",
	"state_conflict":                     "GP could not confirm the resource state required by this operation.",
	"resource_in_use":                    "The resource is still referenced or held by another operation.",
	"scope_unauthorized":                 "The operation is not authorized for this resource scope.",
	"dependency_unavailable":             "A required dependency could not be reached or did not respond.",
	"execution_input_rejected":           "The executor rejected the operation's input. Inspect the Task and Controller/Agent logs; this does not necessarily mean the Blueprint is invalid.",
	"internal_execution_error":           "GP encountered an internal execution error. The private cause is available in Controller/Agent logs for this Task.",
	"process_failed":                     "The operation's process returned a nonzero exit code. Its output is not retained in Task history.",
	"config_rejected":                    "The workload rejected its configuration.",
	"compose_failed":                     "Docker Compose could not complete the container operation.",
	"component_config_rejected":          "The Component rejected its rendered configuration.",
	"component_activation_failed":        "The Component could not activate the prepared configuration.",
	"healthcheck_pgrep_missing":          "Healthcheck cannot run: pgrep is not installed in the container.",
	"healthcheck_curl_missing":           "Healthcheck cannot run: curl is not installed in the container.",
	"healthcheck_nc_missing":             "Healthcheck cannot run: nc is not installed in the container.",
	"healthcheck_command_missing":        "Healthcheck cannot run: a required command was not found in the container.",
	"healthcheck_command_not_executable": "Healthcheck cannot run: a required command is not executable in the container.",
}

func (failure *Failure) Validate() error {
	if failure == nil {
		return nil
	}
	if _, found := failureSummaries[failure.Code]; !found {
		return errs.New(errs.KindValidationFailed, "Task failure diagnostic code is invalid")
	}
	return nil
}

func (failure *Failure) Summary() string {
	if failure == nil {
		return ""
	}
	return failureSummaries[failure.Code]
}

func CloneFailure(failure *Failure) *Failure {
	if failure == nil {
		return nil
	}
	owned := *failure
	return &owned
}

func FailuresEqual(left, right *Failure) bool {
	return left == nil && right == nil || left != nil && right != nil && *left == *right
}

func FailureFromAgent(value *agentpb.StepFailure) (*Failure, error) {
	if value == nil {
		return nil, nil
	}
	failure := &Failure{Code: value.GetCode(), ExitCode: value.GetExitCode()}
	return failure, failure.Validate()
}

// CaptureFailure classifies known causes without copying their messages. Helper
// diagnostics take precedence because they identify an observed specific cause.
func CaptureFailure(err error, exitCode int32, diagnostic agentpb.ComposeHelperDiagnostic) *agentpb.StepFailure {
	failure := captureFailure(err, exitCode, helperFailureCode(diagnostic))
	if failure == nil {
		return nil
	}
	return &agentpb.StepFailure{Code: failure.Code, ExitCode: failure.ExitCode}
}

// CaptureControllerFailure uses the same safe classification without inventing
// an Agent report, subprocess exit code, or completed recovery evidence.
func CaptureControllerFailure(err error) *Failure {
	return captureFailure(err, 0, "")
}

func captureFailure(err error, exitCode int32, code string) *Failure {
	if err == nil && exitCode == 0 && code == "" {
		return nil
	}
	if code == "" || code == "compose_failed" {
		kind, _ := errs.KindOf(err)
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			code = "step_timed_out"
		case errors.Is(err, context.Canceled):
			code = "execution_canceled"
		case errors.Is(err, fs.ErrPermission):
			code = "permission_denied"
		case errors.Is(err, fs.ErrNotExist):
			code = "file_not_found"
		case exitCode != 0 && code == "":
			code = "process_failed"
		case kind == errs.KindWorkloadImageResolutionUnavailable:
			code = "image_unavailable"
		case kind == errs.KindWorkloadImageResolutionBusy:
			code = "image_resolution_busy"
		case kind == errs.KindStateConflict:
			code = "state_conflict"
		case kind == errs.KindResourceInUse:
			code = "resource_in_use"
		case kind == errs.KindScopeUnauthorized:
			code = "scope_unauthorized"
		case kind == errs.KindStorageUnavailable || kind == errs.KindRequestUnavailable:
			code = "dependency_unavailable"
		case kind == errs.KindValidationFailed:
			code = "execution_input_rejected"
		default:
			if code == "" {
				code = "internal_execution_error"
			}
		}
	}
	return &Failure{Code: code, ExitCode: exitCode}
}

func helperFailureCode(diagnostic agentpb.ComposeHelperDiagnostic) string {
	switch diagnostic {
	case agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_CONFIG_REJECTED:
		return "config_rejected"
	case agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPOSE_FAILED:
		return "compose_failed"
	case agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPONENT_CONFIG_REJECTED:
		return "component_config_rejected"
	case agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPONENT_ACTIVATION_FAILED:
		return "component_activation_failed"
	case agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_HEALTHCHECK_PGREP_MISSING:
		return "healthcheck_pgrep_missing"
	case agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_HEALTHCHECK_CURL_MISSING:
		return "healthcheck_curl_missing"
	case agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_HEALTHCHECK_NC_MISSING:
		return "healthcheck_nc_missing"
	case agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_HEALTHCHECK_COMMAND_MISSING:
		return "healthcheck_command_missing"
	case agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_HEALTHCHECK_COMMAND_NOT_EXECUTABLE:
		return "healthcheck_command_not_executable"
	default:
		return ""
	}
}
