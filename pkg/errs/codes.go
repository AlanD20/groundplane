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
	CodeRunnerSlugConflict     Code = "runner.slug_conflict"
	CodeAgentNotFound          Code = "agent.not_found"
	CodeReleaseGroupNotFound   Code = "release_group.not_found"
	CodeComponentNotFound      Code = "component.not_found"
	CodeRecoveryPointNotFound  Code = "recovery_point.not_found"

	// --- conflict / in-flight ---
	CodeDeployInFlight        Code = "deploy.in_flight"
	CodeTaskNotRetryable      Code = "task.not_retryable"
	CodeTaskNotAbortable      Code = "task.not_abortable"
	CodeTaskRetryInFlight     Code = "task.retry_in_flight"
	CodeSlugConflict          Code = "slug.conflict"
	CodeNameConflict          Code = "name.conflict"
	CodeStateConflict         Code = "state.conflict"
	CodeResourceInUse         Code = "resource.in_use"
	CodeCursorExpired         Code = "cursor.expired"
	CodeIdempotencyInProgress Code = "idempotency.in_progress"

	// --- malformed intent / retryable storage ---
	CodeIdempotencyMismatch                Code = "idempotency.mismatch"
	CodeStorageUnavailable                 Code = "storage.unavailable"
	CodeWorkloadImageResolutionBusy        Code = "workload.image_resolution_busy"
	CodeWorkloadImageResolutionUnavailable Code = "workload.image_resolution_unavailable"

	// --- not-found additions required by the durable record catalog ---
	CodeZoneNotFound              Code = "zone.not_found"
	CodeRouteNotFound             Code = "route.not_found"
	CodeVolumeNotFound            Code = "volume.not_found"
	CodeEntryNotFound             Code = "entry.not_found"
	CodeScriptNotFound            Code = "script.not_found"
	CodeScriptRetryUnsafe         Code = "script.retry_unsafe"
	CodeReleaseNotFound           Code = "release.not_found"
	CodeReleaseRecoveryRequired   Code = "release.recovery_required"
	CodeReleaseDeadlineTooShort   Code = "release.deadline_too_short"
	CodeReleasePlanTooLarge       Code = "release.plan_too_large"
	CodeReleaseGroupTagRequired   Code = "release_group.tag_required"
	CodeRollbackNoPreviousRelease Code = "rollback.no_previous_release"
	CodeRollbackSourceExpired     Code = "rollback.source_expired"
	CodeBackupSourceNotFound      Code = "backup_source.not_found"

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
	// Internal request Kinds bind each accepted framework status; an unknown
	// framework status fails closed as request.failed/500.
	CodeRequestNotFound             Code = "request.not_found"
	CodeRequestMethodNotAllowed     Code = "request.method_not_allowed"
	CodeRequestNotAcceptable        Code = "request.not_acceptable"
	CodeRequestUnsupportedMediaType Code = "request.unsupported_media_type"
	CodeRequestFailed               Code = "request.failed"

	// --- scaffolding / catch-all ---
	CodeNotImplemented Code = "not_implemented" // scaffold stub handlers; KindNotImplemented owns HTTP 501
	CodeInternal       Code = "internal"
)

// Kind is the closed internal error identity. Constructors accept Kind, never
// the public Code string, so a caller cannot choose a Code without also
// choosing its one accepted class and HTTP status.
type Kind uint16

const (
	kindInvalid Kind = iota
	KindTenantNotFound
	KindProjectNotFound
	KindEnvironmentNotFound
	KindServiceNotFound
	KindBackingServiceNotFound
	KindAttachNotFound
	KindTaskNotFound
	KindTaskTimedOut
	KindSecretNotFound
	KindConnectorNotFound
	KindRunnerNotFound
	KindRunnerSlugConflict
	KindAgentNotFound
	KindReleaseGroupNotFound
	KindComponentNotFound
	KindRecoveryPointNotFound
	KindDeployInFlight
	KindTaskNotRetryable
	KindTaskNotAbortable
	KindTaskRetryInFlight
	KindSlugConflict
	KindNameConflict
	KindStateConflict
	KindResourceInUse
	KindCursorExpired
	KindIdempotencyInProgress
	KindIdempotencyMismatch
	KindStorageUnavailable
	KindWorkloadImageResolutionBusy
	KindWorkloadImageResolutionUnavailable
	KindZoneNotFound
	KindRouteNotFound
	KindVolumeNotFound
	KindEntryNotFound
	KindScriptNotFound
	KindScriptRetryUnsafe
	KindReleaseNotFound
	KindReleaseRecoveryRequired
	KindReleaseDeadlineTooShort
	KindReleasePlanTooLarge
	KindReleaseGroupTagRequired
	KindRollbackNoPreviousRelease
	KindRollbackSourceExpired
	KindBackupSourceNotFound
	KindStrategyNotImplemented
	KindRotationNotImplemented
	KindAdapterManualOnly
	KindValidationFailed
	KindMalformedRequest
	KindScopeUnauthorized
	KindConnectorScopeInvalid
	KindRequestNotFound
	KindRequestMethodNotAllowed
	KindRequestNotAcceptable
	KindRequestUnsupportedMediaType
	KindRequestTooLarge
	KindRequestUnavailable
	KindRequestFailed
	KindNotImplemented
	KindInternal
	kindLimit
)

type descriptor struct {
	Code   Code
	Class  Class
	Status int
}

var kindDescriptors = [kindLimit]descriptor{
	KindTenantNotFound:                     {CodeTenantNotFound, ClassNotFound, 404},
	KindProjectNotFound:                    {CodeProjectNotFound, ClassNotFound, 404},
	KindEnvironmentNotFound:                {CodeEnvironmentNotFound, ClassNotFound, 404},
	KindServiceNotFound:                    {CodeServiceNotFound, ClassNotFound, 404},
	KindBackingServiceNotFound:             {CodeBackingServiceNotFound, ClassNotFound, 404},
	KindAttachNotFound:                     {CodeAttachNotFound, ClassNotFound, 404},
	KindTaskNotFound:                       {CodeTaskNotFound, ClassNotFound, 404},
	KindTaskTimedOut:                       {CodeTaskTimedOut, ClassRetryable, 503},
	KindSecretNotFound:                     {CodeSecretNotFound, ClassNotFound, 404},
	KindConnectorNotFound:                  {CodeConnectorNotFound, ClassNotFound, 404},
	KindRunnerNotFound:                     {CodeRunnerNotFound, ClassNotFound, 404},
	KindRunnerSlugConflict:                 {CodeRunnerSlugConflict, ClassConflict, 409},
	KindAgentNotFound:                      {CodeAgentNotFound, ClassNotFound, 404},
	KindReleaseGroupNotFound:               {CodeReleaseGroupNotFound, ClassNotFound, 404},
	KindComponentNotFound:                  {CodeComponentNotFound, ClassNotFound, 404},
	KindRecoveryPointNotFound:              {CodeRecoveryPointNotFound, ClassNotFound, 404},
	KindDeployInFlight:                     {CodeDeployInFlight, ClassConflict, 409},
	KindTaskNotRetryable:                   {CodeTaskNotRetryable, ClassConflict, 409},
	KindTaskNotAbortable:                   {CodeTaskNotAbortable, ClassConflict, 409},
	KindTaskRetryInFlight:                  {CodeTaskRetryInFlight, ClassConflict, 409},
	KindSlugConflict:                       {CodeSlugConflict, ClassConflict, 409},
	KindNameConflict:                       {CodeNameConflict, ClassConflict, 409},
	KindStateConflict:                      {CodeStateConflict, ClassConflict, 409},
	KindResourceInUse:                      {CodeResourceInUse, ClassConflict, 409},
	KindCursorExpired:                      {CodeCursorExpired, ClassConflict, 409},
	KindIdempotencyInProgress:              {CodeIdempotencyInProgress, ClassConflict, 409},
	KindIdempotencyMismatch:                {CodeIdempotencyMismatch, ClassBadRequest, 400},
	KindStorageUnavailable:                 {CodeStorageUnavailable, ClassRetryable, 503},
	KindWorkloadImageResolutionBusy:        {CodeWorkloadImageResolutionBusy, ClassConflict, 409},
	KindWorkloadImageResolutionUnavailable: {CodeWorkloadImageResolutionUnavailable, ClassRetryable, 503},
	KindZoneNotFound:                       {CodeZoneNotFound, ClassNotFound, 404},
	KindRouteNotFound:                      {CodeRouteNotFound, ClassNotFound, 404},
	KindVolumeNotFound:                     {CodeVolumeNotFound, ClassNotFound, 404},
	KindEntryNotFound:                      {CodeEntryNotFound, ClassNotFound, 404},
	KindScriptNotFound:                     {CodeScriptNotFound, ClassNotFound, 404},
	KindScriptRetryUnsafe:                  {CodeScriptRetryUnsafe, ClassConflict, 409},
	KindReleaseNotFound:                    {CodeReleaseNotFound, ClassNotFound, 404},
	KindReleaseRecoveryRequired:            {CodeReleaseRecoveryRequired, ClassConflict, 409},
	KindReleaseDeadlineTooShort:            {CodeReleaseDeadlineTooShort, ClassValidation, 422},
	KindReleasePlanTooLarge:                {CodeReleasePlanTooLarge, ClassValidation, 422},
	KindReleaseGroupTagRequired:            {CodeReleaseGroupTagRequired, ClassValidation, 422},
	KindRollbackNoPreviousRelease:          {CodeRollbackNoPreviousRelease, ClassConflict, 409},
	KindRollbackSourceExpired:              {CodeRollbackSourceExpired, ClassConflict, 409},
	KindBackupSourceNotFound:               {CodeBackupSourceNotFound, ClassNotFound, 404},
	KindStrategyNotImplemented:             {CodeStrategyNotImplemented, ClassValidation, 422},
	KindRotationNotImplemented:             {CodeRotationNotImplemented, ClassValidation, 422},
	KindAdapterManualOnly:                  {CodeAdapterManualOnly, ClassValidation, 422},
	KindValidationFailed:                   {CodeValidationFailed, ClassValidation, 422},
	KindMalformedRequest:                   {CodeValidationFailed, ClassBadRequest, 400},
	KindScopeUnauthorized:                  {CodeScopeUnauthorized, ClassValidation, 422},
	KindConnectorScopeInvalid:              {CodeConnectorScopeInvalid, ClassValidation, 422},
	KindRequestNotFound:                    {CodeRequestNotFound, ClassNotFound, 404},
	KindRequestMethodNotAllowed:            {CodeRequestMethodNotAllowed, ClassMethodNotAllowed, 405},
	KindRequestNotAcceptable:               {CodeRequestNotAcceptable, ClassNotAcceptable, 406},
	KindRequestUnsupportedMediaType:        {CodeRequestUnsupportedMediaType, ClassUnsupportedMediaType, 415},
	KindRequestTooLarge:                    {CodeRequestFailed, ClassBadRequest, 413},
	KindRequestUnavailable:                 {CodeRequestFailed, ClassRetryable, 503},
	KindRequestFailed:                      {CodeRequestFailed, ClassInternal, 500},
	KindNotImplemented:                     {CodeNotImplemented, ClassInternal, 501},
	KindInternal:                           {CodeInternal, ClassInternal, 500},
}

func descriptorFor(kind Kind) (descriptor, bool) {
	if kind <= kindInvalid || kind >= kindLimit {
		return kindDescriptors[KindInternal], false
	}
	value := kindDescriptors[kind]
	if value.Code == "" {
		return kindDescriptors[KindInternal], false
	}
	return value, true
}

func kindForProblem(code Code, status int) (Kind, bool) {
	for kind := Kind(1); kind < kindLimit; kind++ {
		value, ok := descriptorFor(kind)
		if ok && value.Code == code && value.Status == status {
			return kind, true
		}
	}
	return kindInvalid, false
}
