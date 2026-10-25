package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backingservices"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentqueries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Aggregate cleanup must not bypass an operator's before-stop hook. The existing
// Remove containers operation owns that hook and its interruption/retry outcome.
func (repository *HierarchyDeletionRepository) protectBackingStopHook(
	ctx context.Context, kind hierarchydeletion.HierarchyDeletionTargetKind, projectID string, revision int64,
) ([]keyvalue.Condition, error) {
	if kind != hierarchydeletion.HierarchyDeletionTargetBacking {
		return nil, nil
	}
	reader, err := backingservices.NewRepository(repository.store)
	if err != nil {
		return nil, err
	}
	backing, err := reader.GetBackingServiceAtRevision(ctx, projectID, revision)
	if err != nil {
		return nil, err
	}
	service, err := environmentqueries.FindServiceAtRevision(ctx, repository.store, backing.Record.ServiceID, revision)
	if err != nil {
		return nil, err
	}
	keys := []string{blueprints.EnvironmentBlueprintHeadKey(backing.Record.EnvironmentID),
		services.ServiceRuntimeKey(
			backing.Record.ServiceID,
		), services.ServiceLifecycleActiveKey(backing.Record.ServiceID)}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 3 || read.Values[0] == nil ||
		read.Values[1] == nil {
		return nil, hierarchydeletion.CorruptHierarchyDeletion()
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[2] != nil {
		return nil, errs.New(
			errs.KindResourceInUse,
			"Finish the Backing Service's active runtime Task before deleting it",
		)
	}
	if hooks := service.Record.Desired.Hooks; hooks != nil && hooks.BeforeStop != nil &&
		service.Record.Runtime.RuntimeIntent != core.ServiceRuntimeIntentAbsent {
		return nil, errs.New(errs.KindResourceInUse,
			"Use Remove containers first to complete this Backing Service's before-stop hook, then delete the instance")
	}
	return []keyvalue.Condition{{Key: keys[0], ModRevision: read.Values[0].ModRevision},
		{Key: keys[1], ModRevision: read.Values[1].ModRevision}, {Key: keys[2]}}, nil
}
