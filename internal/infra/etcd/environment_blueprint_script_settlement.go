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
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareBlueprintScriptSettlement proves the parent's active authored Script definition,
// or its omission, without executing the hook or discarding a retained body.
func (repository *EnvironmentBlueprintRepository) PrepareBlueprintScriptSettlement(
	ctx context.Context, work workqueue.View, unit blueprintunits.Unit, at time.Time,
) (workqueue.Change, error) {
	parent := work.Task.Task
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return workqueue.Change{}, err
	}
	if repository == nil || parent.Validate() != nil || parent.Action != workexecution.ActionApply ||
		work.ReadRevision <= 0 || unit.Target.Kind != ids.KindScript {
		return workqueue.Change{}, errs.New(errs.KindValidationFailed, "Blueprint Script settlement identity is invalid")
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
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint Script parent is no longer current")
	}
	environmentID := parent.Scope.EnvironmentID
	keys := []string{
		workqueue.TaskKey(parent.ID), workqueue.ItemKey(work.Item.Item.ID),
		scriptrecord.ScriptSetActiveKey(environmentID),
		blueprints.EnvironmentBlueprintOwnedIdentitiesKey(environmentID, parent.ID),
		blueprints.EnvironmentBlueprintRootKey(environmentID, parent.ID),
		scriptrecord.ScriptSetScriptKey(environmentID, parent.ID, unit.Target.ID),
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: snapshot.ReadRevision})
	if err != nil {
		return workqueue.Change{}, err
	}
	if read == nil || read.ReadRevision != snapshot.ReadRevision || len(read.Values) != len(keys) {
		return workqueue.Change{}, errs.New(errs.KindInternal, "Blueprint Script settlement read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	conditions := make([]keyvalue.Condition, 0, len(keys)+2)
	for index, value := range read.Values {
		if (index < 5 && value == nil) || (value != nil && value.Key != keys[index]) {
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint Script settlement authority is unavailable")
		}
		conditions = append(conditions, keyvalue.Condition{Key: keys[index], ModRevision: keyvalue.RevisionOf(value)})
	}
	set, setErr := scriptrecord.DecodeScriptSetGeneration(read.Values[2].Value)
	identities, identitiesErr := projectionrecord.DecodeEnvironmentOwnedIdentities(read.Values[3].Value)
	desiredInput, desiredFound, desiredErr := blueprints.ReadDesiredInputRevision(
		ctx, repository.store, environmentID, parent.ID, snapshot.ReadRevision,
	)
	desired := desiredInput.Record
	if setErr != nil || identitiesErr != nil || desiredErr != nil ||
		!desiredFound || desiredInput.Revision != read.Values[4].ModRevision ||
		read.Values[0].ModRevision != work.Task.Revision || read.Values[1].ModRevision != work.Item.Revision ||
		set.EnvironmentID != environmentID || set.GenerationID != parent.ID ||
		identities.EnvironmentID != environmentID || identities.RevisionID != parent.ID ||
		desired.EnvironmentID != environmentID || desired.RevisionID != parent.ID {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint Script settlement revision changed")
	}
	encoded, err := projectionrecord.EncodeEnvironmentDesiredInputStorage(desired)
	if err != nil {
		return workqueue.Change{}, err
	}
	digest := sha256.Sum256(encoded)
	clear(encoded)
	if hex.EncodeToString(digest[:]) != parent.Intent.SHA256 {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint Script desired input differs from captured intent")
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
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint Script is still declared")
		}
	} else {
		if name == "" || read.Values[5] == nil {
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint Script definition is missing")
		}
		record, err := scriptrecord.DecodeRecord(read.Values[5].Value)
		if err != nil {
			return workqueue.Change{}, err
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
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint Script definition differs from its authored input")
		}
		bodyKey := scriptrecord.ScriptSetBodyGenerationKey(environmentID, parent.ID, unit.Target.ID, record.ActiveGeneration)
		bodyRead, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{bodyKey}, Revision: snapshot.ReadRevision})
		if err != nil {
			return workqueue.Change{}, err
		}
		if bodyRead == nil || bodyRead.ReadRevision != snapshot.ReadRevision || len(bodyRead.Values) != 1 || bodyRead.Values[0] == nil {
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint Script body is unavailable")
		}
		defer keyvalue.ClearValues(bodyRead.Values)
		body, err := scriptrecord.DecodeScriptBodyGeneration(bodyRead.Values[0].Value)
		if err != nil || body.ScriptID != unit.Target.ID || body.Generation != record.ActiveGeneration || body.Body != spec.Script {
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint Script body differs from its authored input")
		}
		conditions = append(conditions, keyvalue.Condition{Key: bodyKey, ModRevision: bodyRead.Values[0].ModRevision})
	}
	change, err := blueprintunits.PrepareDefinitionWorkSettlement(snapshot, work, unit, read.Values[2].ModRevision, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, conditions...)
	return change, nil
}
