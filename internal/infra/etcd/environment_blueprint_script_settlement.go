package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// SettleBlueprintScript proves the parent's active authored Script definition,
// or its omission, without executing the hook or discarding a retained body.
func (repository *EnvironmentBlueprintRepository) SettleBlueprintScript(
	ctx context.Context, parent TaskRecord, unit blueprintunits.Unit,
) error {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return err
	}
	if repository == nil || validateBlueprintParentClaimTask(parent) != nil ||
		parent.Status != taskjournal.TaskStatusRunning || unit.Target.Kind != ids.KindScript {
		return errs.New(errs.KindValidationFailed, "Blueprint Script settlement identity is invalid")
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
		return errs.New(errs.KindStateConflict, "Blueprint Script parent is no longer current")
	}
	environmentID := parent.Owner.EnvironmentID
	keys := []string{
		taskjournal.TaskStorageKey(parent.ID), taskjournal.BlueprintParentClaimKey(parent.ID),
		scriptrecord.ScriptSetActiveKey(environmentID),
		blueprints.EnvironmentBlueprintOwnedIdentitiesKey(environmentID, parent.ID),
		blueprints.EnvironmentBlueprintRootKey(environmentID, parent.ID),
		scriptrecord.ScriptSetScriptKey(environmentID, parent.ID, unit.Target.ID),
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: snapshot.ReadRevision})
	if err != nil {
		return err
	}
	if read == nil || read.ReadRevision != snapshot.ReadRevision || len(read.Values) != len(keys) {
		return errs.New(errs.KindInternal, "Blueprint Script settlement read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	conditions := make([]keyvalue.Condition, 0, len(keys)+2)
	for index, value := range read.Values {
		if (index < 5 && value == nil) || (value != nil && value.Key != keys[index]) {
			return errs.New(errs.KindStateConflict, "Blueprint Script settlement authority is unavailable")
		}
		conditions = append(conditions, keyvalue.Condition{Key: keys[index], ModRevision: keyvalue.RevisionOf(value)})
	}
	storedParent, parentErr := DecodeTaskRecord(read.Values[0].Value)
	claimID, claimErr := idempotency.DecodeTaskReference(read.Values[1].Value)
	set, setErr := scriptrecord.DecodeScriptSetGeneration(read.Values[2].Value)
	identities, identitiesErr := projectionrecord.DecodeEnvironmentOwnedIdentities(read.Values[3].Value)
	desiredInput, desiredFound, desiredErr := blueprints.ReadDesiredInputRevision(
		ctx, repository.store, environmentID, parent.ID, snapshot.ReadRevision,
	)
	desired := desiredInput.Record
	if parentErr != nil || claimErr != nil || setErr != nil || identitiesErr != nil || desiredErr != nil ||
		!desiredFound || desiredInput.Revision != read.Values[4].ModRevision ||
		validateBlueprintParentClaimTask(storedParent) != nil || storedParent.Status != taskjournal.TaskStatusRunning ||
		storedParent.ID != parent.ID || storedParent.Owner != parent.Owner || storedParent.PlanHash != parent.PlanHash ||
		storedParent.PlanID != parent.PlanID || claimID != parent.ID ||
		set.EnvironmentID != environmentID || set.GenerationID != parent.ID ||
		identities.EnvironmentID != environmentID || identities.RevisionID != parent.ID ||
		desired.EnvironmentID != environmentID || desired.RevisionID != parent.ID {
		return errs.New(errs.KindStateConflict, "Blueprint Script settlement revision changed")
	}
	name := ""
	for _, identity := range identities.Scripts {
		if identity.ID == unit.Target.ID {
			name = identity.Name
			break
		}
	}
	if unit.Removal {
		if name != "" || read.Values[5] != nil {
			return errs.New(errs.KindStateConflict, "Blueprint Script is still declared")
		}
	} else {
		if name == "" || read.Values[5] == nil {
			return errs.New(errs.KindStateConflict, "Blueprint Script definition is missing")
		}
		record, err := scriptrecord.DecodeRecord(read.Values[5].Value)
		if err != nil {
			return err
		}
		key, err := scriptrecord.BlueprintAuthoringKey(record)
		spec, found := desired.Input.Scripts[name]
		serviceID := ""
		for _, identity := range identities.Services {
			if identity.Name == spec.Service {
				serviceID = identity.ID
			}
		}
		if err != nil || !found || key != name || record.EnvironmentID != environmentID ||
			record.Desired.ID != unit.Target.ID || record.ScriptSetGeneration != parent.ID ||
			serviceID == "" || record.ServiceID != serviceID || record.Desired.ServiceName != spec.Service ||
			record.Desired.Slug != spec.Slug || record.Desired.When != spec.When || record.Desired.Order != spec.Order {
			return errs.New(errs.KindStateConflict, "Blueprint Script definition differs from its authored input")
		}
		bodyKey := scriptrecord.ScriptSetBodyGenerationKey(environmentID, parent.ID, unit.Target.ID, record.ActiveGeneration)
		bodyRead, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{bodyKey}, Revision: snapshot.ReadRevision})
		if err != nil {
			return err
		}
		if bodyRead == nil || bodyRead.ReadRevision != snapshot.ReadRevision || len(bodyRead.Values) != 1 || bodyRead.Values[0] == nil {
			return errs.New(errs.KindStateConflict, "Blueprint Script body is unavailable")
		}
		defer keyvalue.ClearValues(bodyRead.Values)
		body, err := scriptrecord.DecodeScriptBodyGeneration(bodyRead.Values[0].Value)
		if err != nil || body.ScriptID != unit.Target.ID || body.Generation != record.ActiveGeneration || body.Body != spec.Script {
			return errs.New(errs.KindStateConflict, "Blueprint Script body differs from its authored input")
		}
		conditions = append(conditions, keyvalue.Condition{Key: bodyKey, ModRevision: bodyRead.Values[0].ModRevision})
	}
	receipt, err := blueprintunits.PrepareControllerScriptSettlement(snapshot, unit, ids.New(ids.KindOperation), read.Values[2].ModRevision)
	if err != nil {
		return err
	}
	defer receipt.Clear()
	conditions = append(conditions, receipt.Conditions()...)
	conditions = append(conditions, keyvalue.Condition{Key: taskjournal.BlueprintParentAbortKey(parent.ID)})
	result, err := repository.store.Transact(ctx, conditions, receipt.Mutations())
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "Blueprint Script settlement raced")
	}
	return nil
}
