package api

import "github.com/sample-tenant/groundplane/pkg/errs"

// Re-exported so API consumers (the CLI's apiclient, external scripts)
// can decode RFC 7807 problem+json and construct/inspect domain errors
// without importing pkg/errs directly — pkg/api is the public contract
// surface; pkg/errs is its error vocabulary. See docs/standards.md,
// section 1's import matrix: pkg/api imports pkg/errs, never the reverse.
type (
	Problem = errs.Problem
	Code    = errs.Code
	Class   = errs.Class
)

const (
	CodeTenantNotFound         = errs.CodeTenantNotFound
	CodeProjectNotFound        = errs.CodeProjectNotFound
	CodeEnvironmentNotFound    = errs.CodeEnvironmentNotFound
	CodeServiceNotFound        = errs.CodeServiceNotFound
	CodeBackingServiceNotFound = errs.CodeBackingServiceNotFound
	CodeAttachNotFound         = errs.CodeAttachNotFound
	CodeTaskNotFound           = errs.CodeTaskNotFound
	CodeTaskTimedOut           = errs.CodeTaskTimedOut
	CodeSecretNotFound         = errs.CodeSecretNotFound
	CodeConnectorNotFound      = errs.CodeConnectorNotFound
	CodeRunnerNotFound         = errs.CodeRunnerNotFound
	CodeAgentNotFound          = errs.CodeAgentNotFound
	CodeReleaseGroupNotFound   = errs.CodeReleaseGroupNotFound
	CodeAddonNotFound          = errs.CodeAddonNotFound
	CodeRecoveryPointNotFound  = errs.CodeRecoveryPointNotFound
	CodeDeployInFlight         = errs.CodeDeployInFlight
	CodeSlugConflict           = errs.CodeSlugConflict
	CodeStrategyNotImplemented = errs.CodeStrategyNotImplemented
	CodeRotationNotImplemented = errs.CodeRotationNotImplemented
	CodeAdapterManualOnly      = errs.CodeAdapterManualOnly
	CodeValidationFailed       = errs.CodeValidationFailed
	CodeScopeUnauthorized      = errs.CodeScopeUnauthorized
	CodeConnectorScopeInvalid  = errs.CodeConnectorScopeInvalid
	CodeNotImplemented         = errs.CodeNotImplemented
	CodeInternal               = errs.CodeInternal
)
