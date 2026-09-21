package releasequeries

import (
	"context"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// LoadPlanningAttaches reads the complete binding set at the runtime capture's
// fixed revision. The caller's Environment epoch fences its later publication.
func (ledger *Reader) LoadPlanningAttaches(
	ctx context.Context,
	scope ReleasePlanningScope,
) ([]etcdstore.Versioned[attachrecord.Record], error) {
	if ctx == nil || ledger == nil || ledger.store == nil || scope.ReadRevision <= 0 ||
		scope.EnvironmentEpochRevision <= 0 ||
		ids.Validate(ids.KindEnvironment, scope.Environment.Record.ID) != nil {
		return nil, errs.New(errs.KindValidationFailed, "runtime Attach capture scope is invalid")
	}
	return attachrecord.LoadEnvironmentAttachesAtRevision(ctx, ledger.store, scope.Environment.Record.ID, scope.ReadRevision)
}
