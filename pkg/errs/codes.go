package errs

// The full Code catalog, grouped by domain. Public domain codes use dot
// namespaces so their stable API representation matches api-cli.md.
//
// Every new Code added here must also be added to api-cli.md's error
// examples if it's part of the public contract (see the coding-standards
// checklist: "code added to the RFC 7807 surface").
const (
	// --- not-found, one per addressable resource ---
	CodeTenantNotFound         Code = "tenant.not_found"
	CodeProjectNotFound        Code = "project.not_found"
	CodeEnvironmentNotFound    Code = "environment.not_found"
	CodeServiceNotFound        Code = "service.not_found"
	CodeBackingServiceNotFound Code = "backing_service.not_found"
	CodeAttachNotFound         Code = "attach.not_found"
	CodeTaskNotFound           Code = "task.not_found"
	CodeTaskTimedOut           Code = "task.timed_out"
	CodeSecretNotFound         Code = "secret.not_found"
	CodeConnectorNotFound      Code = "connector.not_found"
	CodeRunnerNotFound         Code = "runner.not_found"
	CodeAgentNotFound          Code = "agent.not_found"
	CodeReleaseGroupNotFound   Code = "release_group.not_found"
	CodeComponentNotFound      Code = "component.not_found"
	CodeRecoveryPointNotFound  Code = "recovery_point.not_found"

	// --- conflict / in-flight ---
	CodeDeployInFlight    Code = "deploy.in_flight"
	CodeTaskNotRetryable  Code = "task.not_retryable"
	CodeTaskRetryInFlight Code = "task.retry_in_flight"
	CodeSlugConflict      Code = "slug.conflict"

	// --- declared-deferred / not-yet-implemented product surface ---
	CodeStrategyNotImplemented Code = "strategy.not_implemented"
	CodeRotationNotImplemented Code = "rotation.not_implemented"
	CodeAdapterManualOnly      Code = "adapter.manual_only"

	// --- validation / scope ---
	CodeValidationFailed      Code = "validation.failed"
	CodeScopeUnauthorized     Code = "scope.unauthorized"      // cross-tenant access attempt
	CodeConnectorScopeInvalid Code = "connector.scope_invalid" // a connector without exactly one environment owner was attempted

	// --- framework request parsing and transport ---
	// These strings are the request-error catalog locked by api-cli.md.
	// The framework supplies the exact HTTP status for these failures;
	// requestProblem preserves it rather than deriving a replacement.
	CodeRequestNotFound             Code = "request.not_found"
	CodeRequestMethodNotAllowed     Code = "request.method_not_allowed"
	CodeRequestNotAcceptable        Code = "request.not_acceptable"
	CodeRequestUnsupportedMediaType Code = "request.unsupported_media_type"
	CodeRequestFailed               Code = "request.failed"

	// --- scaffolding / catch-all ---
	CodeNotImplemented Code = "not_implemented" // this boilerplate's stub handlers; HTTPStatus() special-cases it to 501
	CodeInternal       Code = "internal"
)

// classify maps every Code above to its Class. This is the ONE place a
// new Code's semantics are decided — HTTP status, retry behavior, and
// IsNotFound/IsRetryable all derive from this table, never from a
// per-call-site judgment call.
func classify(code Code) Class {
	switch code {
	case CodeTenantNotFound, CodeProjectNotFound, CodeEnvironmentNotFound, CodeServiceNotFound,
		CodeBackingServiceNotFound, CodeAttachNotFound, CodeTaskNotFound, CodeSecretNotFound,
		CodeConnectorNotFound, CodeRunnerNotFound, CodeAgentNotFound, CodeReleaseGroupNotFound,
		CodeComponentNotFound, CodeRecoveryPointNotFound, CodeRequestNotFound:
		return ClassNotFound

	case CodeDeployInFlight, CodeTaskNotRetryable, CodeTaskRetryInFlight, CodeSlugConflict:
		return ClassConflict

	case CodeRequestMethodNotAllowed:
		return ClassMethodNotAllowed

	case CodeRequestNotAcceptable:
		return ClassNotAcceptable

	case CodeRequestUnsupportedMediaType:
		return ClassUnsupportedMediaType

	case CodeStrategyNotImplemented, CodeRotationNotImplemented, CodeAdapterManualOnly,
		CodeValidationFailed, CodeScopeUnauthorized, CodeConnectorScopeInvalid:
		return ClassValidation

	case CodeTaskTimedOut:
		return ClassRetryable

	case CodeNotImplemented, CodeInternal, CodeRequestFailed:
		return ClassInternal

	default:
		// An unregistered Code is a programming error, not a client
		// error — fail toward 500/internal rather than guessing.
		return ClassInternal
	}
}
