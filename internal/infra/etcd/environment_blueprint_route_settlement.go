package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// SettleBlueprintRoute acknowledges the Route in the immutable authored input
// before the router needs it. It does not claim that the host is serving it.
func (repository *EnvironmentBlueprintRepository) SettleBlueprintRoute(
	ctx context.Context, parent TaskRecord, unit blueprintunits.Unit,
	route projectionrecord.EnvironmentRouteProjection,
) error {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return err
	}
	if repository == nil || repository.HierarchyRepository == nil ||
		validateBlueprintParentClaimTask(parent) != nil || parent.Status != taskjournal.TaskStatusRunning ||
		parent.Params[blueprints.EnvironmentDesiredRevisionParam] != parent.ID ||
		unit.Target.Kind != ids.KindRoute || unit.Removal {
		return errs.New(errs.KindValidationFailed, "Blueprint Route settlement identity is invalid")
	}
	ledger, err := blueprintunits.NewRepository(repository.store)
	if err != nil {
		return err
	}
	snapshot, err := ledger.Load(ctx, parent.Owner.EnvironmentID)
	if err != nil {
		return err
	}
	if snapshot.HeadTaskID != parent.ID {
		return errs.New(errs.KindStateConflict, "Blueprint Route parent is no longer current")
	}
	parentKey := taskjournal.TaskStorageKey(parent.ID)
	claimKey := taskjournal.BlueprintParentClaimKey(parent.ID)
	rootKey := blueprints.EnvironmentBlueprintRootKey(parent.Owner.EnvironmentID, parent.ID)
	identitiesKey := blueprints.EnvironmentBlueprintOwnedIdentitiesKey(parent.Owner.EnvironmentID, parent.ID)
	keys := []string{parentKey, claimKey, rootKey, identitiesKey}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: snapshot.ReadRevision})
	if err != nil {
		return err
	}
	if read == nil || read.ReadRevision != snapshot.ReadRevision || len(read.Values) != len(keys) {
		return errs.New(errs.KindInternal, "Blueprint Route settlement evidence is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	for index, value := range read.Values {
		if value == nil || value.Key != keys[index] {
			return errs.New(errs.KindStateConflict, "Blueprint Route settlement authority is unavailable")
		}
	}
	storedParent, parentErr := DecodeTaskRecord(read.Values[0].Value)
	claimID, claimErr := idempotency.DecodeTaskReference(read.Values[1].Value)
	if parentErr != nil || claimErr != nil || claimID != parent.ID ||
		validateBlueprintParentClaimTask(storedParent) != nil ||
		storedParent.Status != taskjournal.TaskStatusRunning ||
		storedParent.ID != parent.ID || storedParent.Owner != parent.Owner ||
		storedParent.PlanID != parent.PlanID || storedParent.PlanHash != parent.PlanHash {
		return errs.New(errs.KindStateConflict, "Blueprint Route parent claim changed")
	}
	desired, found, err := blueprints.ReadDesiredInputRevision(
		ctx,
		repository.store,
		parent.Owner.EnvironmentID,
		parent.ID,
		snapshot.ReadRevision,
	)
	if err != nil {
		return err
	}
	owned, err := projectionrecord.DecodeEnvironmentOwnedIdentities(read.Values[3].Value)
	if err != nil {
		return err
	}
	if !found || desired.Revision != read.Values[2].ModRevision ||
		desired.Record.EnvironmentID != parent.Owner.EnvironmentID || desired.Record.RevisionID != parent.ID ||
		desired.Record.RenderGeneration != uint64(parent.RenderGeneration) ||
		owned.EnvironmentID != parent.Owner.EnvironmentID || owned.RevisionID != parent.ID ||
		owned.RenderGeneration != desired.Record.RenderGeneration ||
		!authoredRouteSelectionMatches(desired.Record, owned, unit, route) {
		return errs.New(errs.KindStateConflict, "Blueprint Route authored selection changed")
	}
	receipt, err := blueprintunits.PrepareControllerRouteSettlement(
		snapshot, unit, ids.New(ids.KindOperation), read.Values[2].ModRevision,
	)
	if err != nil {
		return err
	}
	defer receipt.Clear()
	conditions := append(receipt.Conditions(),
		keyvalue.Condition{Key: parentKey, ModRevision: read.Values[0].ModRevision},
		keyvalue.Condition{Key: claimKey, ModRevision: read.Values[1].ModRevision},
		keyvalue.Condition{Key: taskjournal.BlueprintParentAbortKey(parent.ID)},
		keyvalue.Condition{Key: rootKey, ModRevision: read.Values[2].ModRevision},
		keyvalue.Condition{Key: identitiesKey, ModRevision: read.Values[3].ModRevision},
	)
	result, err := repository.store.Transact(ctx, conditions, receipt.Mutations())
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "Blueprint Route settlement raced")
	}
	return nil
}

func authoredRouteSelectionMatches(desired projectionrecord.EnvironmentDesiredInput,
	owned projectionrecord.EnvironmentOwnedIdentities, unit blueprintunits.Unit,
	route projectionrecord.EnvironmentRouteProjection) bool {
	if route.EnvironmentID != desired.EnvironmentID || route.Desired.ID != unit.Target.ID ||
		route.DesiredGeneration != desired.RenderGeneration || route.Desired.Validate() != nil {
		return false
	}
	name := route.Desired.Host + "\x00" + route.Desired.Path
	ownedRoute := false
	for _, identity := range owned.Routes {
		if identity.ID == route.Desired.ID && identity.Name == name {
			ownedRoute = true
		}
	}
	if !ownedRoute {
		return false
	}
	for _, spec := range desired.Input.Routes {
		path := spec.Path
		if path == "" {
			path = "/"
		}
		if spec.Hostname != route.Desired.Host || path != route.Desired.Path ||
			spec.TargetPort != route.Desired.TargetPort || spec.Exposure != route.Desired.Exposure {
			continue
		}
		for _, identity := range owned.Services {
			if identity.Name == spec.Target && identity.ID == route.Desired.TargetServiceID {
				return true
			}
		}
	}
	return false
}
