package serviceremovalreferences

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backingchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/edgechecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasegroups"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareDeclarationRemoval checks live references without manufacturing a
// rendered Service or Compose projection for an unexecuted declaration.
// The caller additionally fences desired input and the Environment mutation epoch.
func PrepareDeclarationRemoval(
	ctx context.Context, store referenceStore, environmentID, serviceID string, revision int64,
) (Guards, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return Guards{}, err
	}
	if ids.Validate(ids.KindEnvironment, environmentID) != nil || ids.Validate(ids.KindService, serviceID) != nil ||
		revision <= 0 {
		return Guards{}, errs.New(errs.KindValidationFailed, "Service declaration removal identity is invalid")
	}
	check, err := backingchecks.RequireServiceIdle(ctx, store, serviceID, revision)
	if err != nil {
		return Guards{}, err
	}
	edge, err := edgechecks.RequireServiceIdle(ctx, store, serviceID, revision)
	if err != nil {
		return Guards{}, err
	}
	for _, prefix := range []string{
		"/v1/indexes/attaches/by-service/service/" + serviceID + "/",
		"/v1/indexes/attaches/by-backing-service/service/" + serviceID + "/",
	} {
		page, err := store.Range(ctx, keyvalue.RangeRequest{Prefix: prefix, Limit: 1, Revision: revision})
		if err != nil {
			return Guards{}, err
		}
		if page == nil || page.ReadRevision != revision {
			return Guards{}, errs.New(errs.KindInternal, "Service declaration reference read is incomplete")
		}
		inUse := page.More || len(page.Values) != 0
		keyvalue.ClearRangeValues(page.Values)
		if inUse {
			return Guards{}, errs.New(errs.KindResourceInUse, "Service is referenced by an Attach")
		}
	}
	if err := rejectReleaseGroupReferences(ctx, store, environmentID, serviceID, revision); err != nil {
		return Guards{}, err
	}
	group, rewrite, err := releasegroups.LoadReleaseGroupCollectionEpoch(ctx, store, environmentID, revision)
	clear(rewrite.Value)
	if err != nil {
		return Guards{}, err
	}
	return Guards{conditions: []keyvalue.Condition{check, edge, group}}, nil
}
