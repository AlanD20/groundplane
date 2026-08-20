package api

import "github.com/AlanD20/groundplane/pkg/errs"

// Re-exported so API consumers can decode the stable RFC problem DTO and code
// values. Domain errors are constructed from closed internal errs.Kind values;
// pkg/api does not expose constructors or error classifications.
type (
	Problem = errs.Problem
	Code    = errs.Code
)

const (
	CodeTenantNotFound              = errs.CodeTenantNotFound
	CodeProjectNotFound             = errs.CodeProjectNotFound
	CodeEnvironmentNotFound         = errs.CodeEnvironmentNotFound
	CodeServiceNotFound             = errs.CodeServiceNotFound
	CodeBackingServiceNotFound      = errs.CodeBackingServiceNotFound
	CodeAttachNotFound              = errs.CodeAttachNotFound
	CodeTaskNotFound                = errs.CodeTaskNotFound
	CodeTaskTimedOut                = errs.CodeTaskTimedOut
	CodeSecretNotFound              = errs.CodeSecretNotFound
	CodeConnectorNotFound           = errs.CodeConnectorNotFound
	CodeRunnerNotFound              = errs.CodeRunnerNotFound
	CodeAgentNotFound               = errs.CodeAgentNotFound
	CodeReleaseGroupNotFound        = errs.CodeReleaseGroupNotFound
	CodeComponentNotFound           = errs.CodeComponentNotFound
	CodeRecoveryPointNotFound       = errs.CodeRecoveryPointNotFound
	CodeDeployInFlight              = errs.CodeDeployInFlight
	CodeTaskNotRetryable            = errs.CodeTaskNotRetryable
	CodeTaskRetryInFlight           = errs.CodeTaskRetryInFlight
	CodeSlugConflict                = errs.CodeSlugConflict
	CodeNameConflict                = errs.CodeNameConflict
	CodeStateConflict               = errs.CodeStateConflict
	CodeResourceInUse               = errs.CodeResourceInUse
	CodeCursorExpired               = errs.CodeCursorExpired
	CodeIdempotencyInProgress       = errs.CodeIdempotencyInProgress
	CodeIdempotencyMismatch         = errs.CodeIdempotencyMismatch
	CodeStorageUnavailable          = errs.CodeStorageUnavailable
	CodeZoneNotFound                = errs.CodeZoneNotFound
	CodeRouteNotFound               = errs.CodeRouteNotFound
	CodeVolumeNotFound              = errs.CodeVolumeNotFound
	CodeEntryNotFound               = errs.CodeEntryNotFound
	CodeScriptNotFound              = errs.CodeScriptNotFound
	CodeReleaseNotFound             = errs.CodeReleaseNotFound
	CodeBackupSourceNotFound        = errs.CodeBackupSourceNotFound
	CodeStrategyNotImplemented      = errs.CodeStrategyNotImplemented
	CodeRotationNotImplemented      = errs.CodeRotationNotImplemented
	CodeAdapterManualOnly           = errs.CodeAdapterManualOnly
	CodeValidationFailed            = errs.CodeValidationFailed
	CodeScopeUnauthorized           = errs.CodeScopeUnauthorized
	CodeConnectorScopeInvalid       = errs.CodeConnectorScopeInvalid
	CodeRequestNotFound             = errs.CodeRequestNotFound
	CodeRequestMethodNotAllowed     = errs.CodeRequestMethodNotAllowed
	CodeRequestNotAcceptable        = errs.CodeRequestNotAcceptable
	CodeRequestUnsupportedMediaType = errs.CodeRequestUnsupportedMediaType
	CodeRequestFailed               = errs.CodeRequestFailed
	CodeNotImplemented              = errs.CodeNotImplemented
	CodeInternal                    = errs.CodeInternal
)
