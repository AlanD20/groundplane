package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// LoadPlanningAttaches reads the complete binding set at the runtime capture's
// fixed revision. The caller's Environment epoch fences its later publication.
func (ledger *ReleaseLedger) LoadPlanningAttaches(
	ctx context.Context,
	scope ReleasePlanningScope,
) ([]Versioned[AttachRecord], error) {
	if ctx == nil || ledger == nil || ledger.store == nil || scope.ReadRevision <= 0 ||
		scope.EnvironmentEpochRevision <= 0 ||
		ids.Validate(ids.KindEnvironment, scope.Environment.Record.ID) != nil {
		return nil, errs.New(errs.KindValidationFailed, "runtime Attach capture scope is invalid")
	}
	return loadEnvironmentAttachesAtRevision(ctx, ledger.store, scope.Environment.Record.ID, scope.ReadRevision)
}
