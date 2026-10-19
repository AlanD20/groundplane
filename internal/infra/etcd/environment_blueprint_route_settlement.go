package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareBlueprintRouteSettlement acknowledges the Route in the immutable authored input
// before the router needs it. It does not claim that the host is serving it.
func (repository *EnvironmentBlueprintRepository) PrepareBlueprintRouteSettlement(
	ctx context.Context, work workqueue.View, unit blueprintunits.Unit,
	route projectionrecord.EnvironmentRouteProjection, at time.Time,
) (workqueue.Change, error) {
	parent := work.Task.Task
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return workqueue.Change{}, err
	}
	if repository == nil || repository.HierarchyRepository == nil ||
		parent.Validate() != nil || parent.Action != workexecution.ActionApply || work.ReadRevision <= 0 ||
		unit.Target.Kind != ids.KindRoute || unit.Removal {
		return workqueue.Change{}, errs.New(errs.KindValidationFailed, "Blueprint Route settlement identity is invalid")
	}
	ledger, err := blueprintunits.NewRepository(repository.store)
	if err != nil {
		return workqueue.Change{}, err
	}
	snapshot, err := ledger.LoadAtRevision(ctx, parent.Scope.EnvironmentID, work.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	if snapshot.HeadTaskID != parent.ID {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint Route parent is no longer current")
	}
	parentKey := workqueue.TaskKey(parent.ID)
	workKey := workqueue.ItemKey(work.Item.Item.ID)
	rootKey := blueprints.EnvironmentBlueprintRootKey(parent.Scope.EnvironmentID, parent.ID)
	identitiesKey := blueprints.EnvironmentBlueprintOwnedIdentitiesKey(parent.Scope.EnvironmentID, parent.ID)
	keys := []string{parentKey, workKey, rootKey, identitiesKey}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: snapshot.ReadRevision})
	if err != nil {
		return workqueue.Change{}, err
	}
	if read == nil || read.ReadRevision != snapshot.ReadRevision || len(read.Values) != len(keys) {
		return workqueue.Change{}, errs.New(errs.KindInternal, "Blueprint Route settlement evidence is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	for index, value := range read.Values {
		if value == nil || value.Key != keys[index] {
			return workqueue.Change{}, errs.New(
				errs.KindStateConflict,
				"Blueprint Route settlement authority is unavailable",
			)
		}
	}
	if read.Values[0].ModRevision != work.Task.Revision || read.Values[1].ModRevision != work.Item.Revision {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint Route work changed")
	}
	desired, found, err := blueprints.ReadDesiredInputRevision(
		ctx,
		repository.store,
		parent.Scope.EnvironmentID,
		parent.ID,
		snapshot.ReadRevision,
	)
	if err != nil {
		return workqueue.Change{}, err
	}
	owned, err := projectionrecord.DecodeEnvironmentOwnedIdentities(read.Values[3].Value)
	if err != nil {
		return workqueue.Change{}, err
	}
	if !found || desired.Revision != read.Values[2].ModRevision ||
		desired.Record.EnvironmentID != parent.Scope.EnvironmentID || desired.Record.RevisionID != parent.ID ||
		owned.EnvironmentID != parent.Scope.EnvironmentID || owned.RevisionID != parent.ID ||
		owned.RenderGeneration != desired.Record.RenderGeneration ||
		!authoredRouteSelectionMatches(desired.Record, owned, unit, route) {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint Route authored selection changed")
	}
	encoded, err := projectionrecord.EncodeEnvironmentDesiredInputStorage(desired.Record)
	if err != nil {
		return workqueue.Change{}, err
	}
	digest := sha256.Sum256(encoded)
	clear(encoded)
	if hex.EncodeToString(digest[:]) != parent.Intent.SHA256 {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Blueprint Route desired input differs from captured intent",
		)
	}
	change, err := blueprintunits.PrepareDefinitionWorkSettlement(snapshot, work, unit, read.Values[2].ModRevision, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	for index, key := range keys {
		change.Conditions = append(
			change.Conditions,
			keyvalue.Condition{Key: key, ModRevision: read.Values[index].ModRevision},
		)
	}
	return change, nil
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
