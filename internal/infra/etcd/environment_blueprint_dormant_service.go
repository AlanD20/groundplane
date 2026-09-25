package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// TrySettleBlueprintDormantService acknowledges desired configuration without
// starting a Service whose independent runtime intent is stopped or absent.
// Running Services return false and retain the Agent Release path.
func (repository *EnvironmentBlueprintRepository) TrySettleBlueprintDormantService(
	ctx context.Context,
	parent TaskRecord,
	unit blueprintunits.Unit,
) (bool, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return false, err
	}
	if repository == nil || repository.HierarchyRepository == nil ||
		validateBlueprintParentClaimTask(parent) != nil || parent.Status != taskjournal.TaskStatusRunning ||
		parent.Params[blueprints.EnvironmentDesiredRevisionParam] != parent.ID ||
		unit.Target.Kind != ids.KindService || unit.Removal {
		return false, errs.New(errs.KindValidationFailed, "Blueprint dormant Service identity is invalid")
	}
	ledger, err := blueprintunits.NewRepository(repository.store)
	if err != nil {
		return false, err
	}
	snapshot, err := ledger.Load(ctx, parent.Owner.EnvironmentID)
	if err != nil {
		return false, err
	}
	if snapshot.HeadTaskID != parent.ID {
		return false, errs.New(errs.KindStateConflict, "Blueprint dormant Service parent is no longer current")
	}
	parentKey := taskjournal.TaskStorageKey(parent.ID)
	claimKey := taskjournal.BlueprintParentClaimKey(parent.ID)
	projectionKey := blueprints.EnvironmentBlueprintEffectiveProjectionKey(parent.Owner.EnvironmentID, parent.ID)
	runtimeKey := servicerecord.ServiceRuntimeKey(unit.Target.ID)
	keys := []string{parentKey, claimKey, projectionKey, runtimeKey}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: snapshot.ReadRevision})
	if err != nil {
		return false, err
	}
	if read == nil || read.ReadRevision != snapshot.ReadRevision || len(read.Values) != len(keys) {
		return false, errs.New(errs.KindInternal, "Blueprint dormant Service evidence is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	for index, value := range read.Values {
		if value != nil && value.Key != keys[index] {
			return false, errs.New(errs.KindInternal, "Blueprint dormant Service evidence is corrupt")
		}
	}
	if read.Values[0] == nil || read.Values[1] == nil || read.Values[2] == nil {
		return false, errs.New(errs.KindStateConflict, "Blueprint dormant Service parent authority is unavailable")
	}
	storedParent, parentErr := DecodeTaskRecord(read.Values[0].Value)
	claimID, claimErr := idempotency.DecodeTaskReference(read.Values[1].Value)
	if parentErr != nil || claimErr != nil || claimID != parent.ID ||
		validateBlueprintParentClaimTask(storedParent) != nil ||
		storedParent.Status != taskjournal.TaskStatusRunning ||
		storedParent.ID != parent.ID || storedParent.Owner != parent.Owner ||
		storedParent.PlanID != parent.PlanID || storedParent.PlanHash != parent.PlanHash {
		return false, errs.New(errs.KindStateConflict, "Blueprint dormant Service parent claim changed")
	}
	projection, err := projectionrecord.DecodeEnvironmentComposeProjectionStorage(read.Values[2].Value)
	if err != nil || projection.EnvironmentID != parent.Owner.EnvironmentID ||
		projection.RevisionID != parent.ID ||
		projection.RenderGeneration != uint64(parent.RenderGeneration) {
		return false, errs.New(errs.KindStateConflict, "Blueprint dormant Service projection changed")
	}
	var projected *servicerecord.EnvironmentServiceProjection
	for index := range projection.DesiredServices {
		if projection.DesiredServices[index].Desired.ID == unit.Target.ID {
			projected = &projection.DesiredServices[index]
			break
		}
	}
	if projected == nil || projected.EnvironmentID != parent.Owner.EnvironmentID {
		return false, errs.New(errs.KindStateConflict, "Blueprint dormant Service is absent from projection")
	}
	if read.Values[3] == nil {
		return false, nil
	}
	runtime, err := servicerecord.DecodeServiceRuntimeRecord(read.Values[3].Value)
	if err != nil || runtime.EnvironmentID != parent.Owner.EnvironmentID ||
		runtime.ServiceID != unit.Target.ID ||
		runtime.BackingNetworkID != projected.BackingNetworkID {
		return false, errs.New(errs.KindStateConflict, "Blueprint dormant Service runtime changed")
	}
	if runtime.Runtime.RuntimeIntent == core.ServiceRuntimeIntentRunning {
		return false, nil
	}
	if runtime.Runtime.RuntimeIntent != core.ServiceRuntimeIntentStopped &&
		runtime.Runtime.RuntimeIntent != core.ServiceRuntimeIntentAbsent {
		return false, errs.New(errs.KindStateConflict, "Blueprint Service runtime intent is invalid")
	}
	receipt, err := blueprintunits.PrepareControllerDormantServiceSettlement(
		snapshot, unit, ids.New(ids.KindOperation), read.Values[3].ModRevision,
	)
	if err != nil {
		return false, err
	}
	defer receipt.Clear()
	conditions := append(receipt.Conditions(),
		keyvalue.Condition{Key: parentKey, ModRevision: read.Values[0].ModRevision},
		keyvalue.Condition{Key: claimKey, ModRevision: read.Values[1].ModRevision},
		keyvalue.Condition{Key: taskjournal.BlueprintParentAbortKey(parent.ID)},
		keyvalue.Condition{Key: projectionKey, ModRevision: read.Values[2].ModRevision},
		keyvalue.Condition{Key: runtimeKey, ModRevision: read.Values[3].ModRevision},
	)
	result, err := repository.store.Transact(ctx, conditions, receipt.Mutations())
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return false, err
	}
	if !result.Succeeded {
		return false, errs.New(errs.KindStateConflict, "Blueprint dormant Service settlement raced")
	}
	return true, nil
}
