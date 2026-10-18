package taskplanning

import (
	"context"

	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareReleasePreflightRunner uses the captured pending Release selected by
// admission. It is not manual Script execution against a serving Release.
func PrepareReleasePreflightRunner(ctx context.Context, input ScriptRunnerInput, operation domain.OperationKind) (*PreparedScriptRunner, error) {
	when := input.Sources.Script.Record.Desired.When
	if input.Candidate != nil || operation != domain.OperationDeploy && operation != domain.OperationRollback ||
		operation == domain.OperationDeploy && when != core.ScriptPreDeploy ||
		operation == domain.OperationRollback && when != core.ScriptPreRollback {
		return nil, errs.New(errs.KindValidationFailed, "Group preflight requires the captured operation's before hook")
	}
	return prepareScriptRunner(ctx, input)
}
