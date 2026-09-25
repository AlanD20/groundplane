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

// SettleBlueprintRoute acknowledges only the Route already in the exact
// sealed projection. It does not perform HTTP-router host effects.
func (repository *EnvironmentBlueprintRepository) SettleBlueprintRoute(
	ctx context.Context, parent TaskRecord, unit blueprintunits.Unit,
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
	projectionKey := blueprints.EnvironmentBlueprintEffectiveProjectionKey(parent.Owner.EnvironmentID, parent.ID)
	keys := []string{parentKey, claimKey, projectionKey}
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
	projection, err := projectionrecord.DecodeEnvironmentComposeProjectionStorage(read.Values[2].Value)
	if err != nil || projection.EnvironmentID != parent.Owner.EnvironmentID ||
		projection.RevisionID != parent.ID ||
		projection.RenderGeneration != uint64(parent.RenderGeneration) {
		return errs.New(errs.KindStateConflict, "Blueprint Route projection changed")
	}
	found := false
	for _, route := range projection.DesiredRoutes {
		if route.Desired.ID == unit.Target.ID && route.EnvironmentID == parent.Owner.EnvironmentID {
			found = true
			break
		}
	}
	if !found {
		return errs.New(errs.KindStateConflict, "Blueprint Route is absent from projection")
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
		keyvalue.Condition{Key: projectionKey, ModRevision: read.Values[2].ModRevision},
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
