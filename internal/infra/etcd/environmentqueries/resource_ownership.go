package environmentqueries

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Select ownership from declared identities before reading derived runtime.
// An unrelated Environment can have a valid desired head whose projection is
// still awaiting its own Attach or Component execution.
func environmentOwnsDesiredResource(
	ctx context.Context, store snapshotReader, environmentID string,
	kind ids.Kind, resourceID string, revision int64,
) (bool, error) {
	owned, found, err := blueprints.ReadCurrentOwnedIdentities(ctx, store, environmentID, revision)
	if err != nil || !found {
		return false, err
	}
	var identities []projectionrecord.OwnedIdentity
	switch kind {
	case ids.KindService:
		identities = owned.Record.Services
	case ids.KindNetwork:
		identities = owned.Record.Networks
	case ids.KindRoute:
		identities = owned.Record.Routes
	case ids.KindEnvEntry:
		identities = owned.Record.Entries
	default:
		return false, errs.New(errs.KindInternal, "unsupported desired resource lookup")
	}
	return slices.ContainsFunc(identities, func(identity projectionrecord.OwnedIdentity) bool {
		return identity.ID == resourceID
	}), nil
}
