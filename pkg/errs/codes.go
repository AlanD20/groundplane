package errs

// The full Code catalog, grouped by domain, dot-namespaced. Two codes
// are flat and verbatim because api-cli.md and mvp.md already lock their
// exact string ("deploy_in_flight", "strategy_not_implemented" appear as
// literal RFC 7807 `code` examples in both docs) — renaming them to fit
// the dot convention would break that lock, so they're kept as-is.
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
	CodeAddonNotFound          Code = "addon.not_found"
	CodeRecoveryPointNotFound  Code = "recovery_point.not_found"

	// --- conflict / in-flight ---
	CodeDeployInFlight Code = "deploy_in_flight" // locked verbatim — see api-cli.md, "Errors"
	CodeSlugConflict   Code = "slug.conflict"

	// --- declared-deferred / not-yet-implemented product surface ---
	CodeStrategyNotImplemented Code = "strategy_not_implemented" // locked verbatim — "rolling" is declared-deferred
	CodeRotationNotImplemented Code = "rotation.not_implemented"
	CodeAdapterManualOnly      Code = "adapter.manual_only"

	// --- validation / scope ---
	CodeValidationFailed      Code = "validation.failed"
	CodeScopeUnauthorized     Code = "scope.unauthorized"      // cross-tenant access attempt
	CodeConnectorScopeInvalid Code = "connector.scope_invalid" // a project-scoped connector was attempted — connectors are environment- or platform-scoped only (see blueprint.md, "Envelope and placement")

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
		CodeAddonNotFound, CodeRecoveryPointNotFound:
		return ClassNotFound

	case CodeDeployInFlight, CodeSlugConflict:
		return ClassConflict

	case CodeStrategyNotImplemented, CodeRotationNotImplemented, CodeAdapterManualOnly,
		CodeValidationFailed, CodeScopeUnauthorized, CodeConnectorScopeInvalid:
		return ClassValidation

	case CodeTaskTimedOut:
		return ClassRetryable

	case CodeNotImplemented, CodeInternal:
		return ClassInternal

	default:
		// An unregistered Code is a programming error, not a client
		// error — fail toward 500/internal rather than guessing.
		return ClassInternal
	}
}
