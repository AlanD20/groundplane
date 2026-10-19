package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/routepersistence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareBlueprintRouteOmission releases Route metadata only after the exact
// router unit has acknowledged withdrawal. Desired absence is not host proof.
func (repository *EnvironmentBlueprintRepository) PrepareBlueprintRouteOmission(
	ctx context.Context, work workqueue.View, unit blueprintunits.Unit, at time.Time,
) (workqueue.Change, error) {
	parent := work.Task.Task
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return workqueue.Change{}, err
	}
	if repository == nil || repository.HierarchyRepository == nil ||
		parent.Validate() != nil || parent.Action != workexecution.ActionApply || work.ReadRevision <= 0 ||
		unit.Target.Kind != ids.KindRoute || !unit.Removal {
		return workqueue.Change{}, errs.New(errs.KindValidationFailed, "Blueprint Route omission identity is invalid")
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
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint Route omission has been superseded")
	}
	environmentID := parent.Scope.EnvironmentID
	keys := []string{
		workqueue.TaskKey(parent.ID), workqueue.ItemKey(work.Item.Item.ID),
		blueprints.EnvironmentBlueprintRootKey(environmentID, parent.ID),
		blueprints.EnvironmentBlueprintOwnedIdentitiesKey(environmentID, parent.ID),
		workqueue.ClaimKey(workexecution.Claim{ScopeID: environmentID}),
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: snapshot.ReadRevision})
	if err != nil {
		return workqueue.Change{}, err
	}
	if read == nil || read.ReadRevision != snapshot.ReadRevision || len(read.Values) != len(keys) {
		return workqueue.Change{}, errs.New(errs.KindInternal, "Blueprint Route omission snapshot is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	conditions := make([]keyvalue.Condition, 0, len(keys)+2)
	for index, value := range read.Values {
		if index < 4 && value == nil || value != nil && value.Key != keys[index] {
			return workqueue.Change{}, errs.New(
				errs.KindStateConflict,
				"Blueprint Route omission authority is unavailable",
			)
		}
		conditions = append(conditions, keyvalue.Condition{Key: keys[index], ModRevision: keyvalue.RevisionOf(value)})
	}
	if read.Values[4] != nil {
		return workqueue.Change{}, errs.New(
			errs.KindResourceInUse,
			"Blueprint Route withdrawal overlaps an active configuration writer",
		)
	}
	desired, found, desiredErr := blueprints.ReadDesiredInputRevision(
		ctx,
		repository.store,
		environmentID,
		parent.ID,
		snapshot.ReadRevision,
	)
	owned, ownedErr := environmentprojection.DecodeEnvironmentOwnedIdentities(read.Values[3].Value)
	if desiredErr != nil || !found || ownedErr != nil ||
		read.Values[0].ModRevision != work.Task.Revision || read.Values[1].ModRevision != work.Item.Revision ||
		desired.Revision != read.Values[2].ModRevision ||
		desired.Record.EnvironmentID != environmentID || desired.Record.RevisionID != parent.ID ||
		owned.EnvironmentID != environmentID || owned.RevisionID != parent.ID ||
		owned.RenderGeneration != desired.Record.RenderGeneration {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint Route omission revision changed")
	}
	encoded, err := environmentprojection.EncodeEnvironmentDesiredInputStorage(desired.Record)
	if err != nil {
		return workqueue.Change{}, err
	}
	digest := sha256.Sum256(encoded)
	clear(encoded)
	if hex.EncodeToString(digest[:]) != parent.Intent.SHA256 {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Blueprint Route omission input differs from captured intent",
		)
	}
	for _, identity := range owned.Routes {
		if identity.ID == unit.Target.ID {
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint Route is still declared")
		}
	}
	authorities, err := repository.ReadEnvironmentBlueprintRuntimeComponents(ctx, environmentID, snapshot.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	var router *BlueprintRuntimeComponent
	for index := range authorities {
		if authorities[index].Component.Record.Desired.Kind == core.ComponentKindIngressCaddy {
			router = &authorities[index]
		}
	}
	if router == nil || !blueprintRouteWithdrawalAcknowledged(snapshot, owned, *router) {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"router has not acknowledged the Blueprint without this Route",
		)
	}
	conditions = append(
		conditions,
		keyvalue.Condition{
			Key:         components.RecordKey(router.Component.Record.Desired.ID),
			ModRevision: router.Component.Revision,
		},
		keyvalue.Condition{
			Key:         componentplanning.RuntimeKey(router.Component.Record.Desired.ID),
			ModRevision: router.Runtime.Revision,
		},
	)
	retirementConditions, mutations, err := routepersistence.PrepareRetirement(
		ctx,
		repository.store,
		environmentID,
		unit.Target.ID,
		snapshot.ReadRevision,
	)
	if err != nil {
		return workqueue.Change{}, err
	}
	fence, err := environmentfence.LoadOrdinary(ctx, repository.store, environmentID, snapshot.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	epoch, err := fence.EpochRewriteMutation()
	if err != nil {
		return workqueue.Change{}, err
	}
	change, err := blueprintunits.PrepareDefinitionWorkSettlement(snapshot, work, unit, read.Values[2].ModRevision, at)
	if err != nil {
		clear(epoch.Value)
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, conditions...)
	change.Conditions = append(change.Conditions, retirementConditions...)
	change.Conditions = append(change.Conditions, fence.TransactionConditions()...)
	change.Mutations = append(change.Mutations, mutations...)
	change.Mutations = append(change.Mutations, epoch)
	return change, nil
}

func blueprintRouteWithdrawalAcknowledged(
	snapshot blueprintunits.Snapshot,
	owned environmentprojection.EnvironmentOwnedIdentities,
	router BlueprintRuntimeComponent,
) bool {
	id := router.Component.Record.Desired.ID
	var applied *keyvalue.Versioned[blueprintunits.AppliedRecord]
	for index := range snapshot.Applied {
		if snapshot.Applied[index].Record.Target == (blueprintunits.ResourceKey{Kind: ids.KindComponent, ID: id}) {
			applied = &snapshot.Applied[index]
		}
	}
	var receipt *blueprintunits.AppliedRecord
	if applied != nil {
		receipt = &applied.Record
	}
	if !router.Component.Record.Desired.Enabled {
		if len(router.Component.Record.Runtime.GeneratedServices) != 0 ||
			router.Component.Record.Runtime.PinnedIPv4 != "" {
			return false
		}
		if receipt == nil {
			return !router.RuntimePresent
		}
		if receipt.State == blueprintunits.Absent {
			if receipt.ControllerOperationID != "" {
				return blueprintunits.ControllerDisabledComponentPublicationMatches(
					*applied,
					router.Component.Revision,
					router.Runtime.Revision,
				)
			}
			return !router.RuntimePresent || !router.Runtime.Record.Component.Desired.Enabled
		}
	}
	if receipt == nil || snapshot.Desired == nil || receipt.State != blueprintunits.Applied {
		return false
	}
	selected := false
	for _, identity := range owned.Components {
		selected = selected || identity.ID == id
	}
	if !selected {
		return false
	}
	unitMatches := false
	for _, candidate := range snapshot.Desired.Record.Units {
		unitMatches = unitMatches ||
			candidate.Target == receipt.Target && !candidate.Removal && candidate.Fingerprint == receipt.Fingerprint
	}
	if !unitMatches {
		return false
	}
	if receipt.ControllerOperationID != "" {
		return !router.Component.Record.Desired.Enabled &&
			blueprintunits.ControllerDisabledComponentPublicationMatches(
				*applied,
				router.Component.Revision,
				router.Runtime.Revision,
			)
	}
	return router.RuntimePresent && receipt.Execution != nil &&
		*receipt.Execution == router.Runtime.Record.Execution && receipt.ParentTaskID == router.Runtime.Record.RevisionID &&
		router.Runtime.Record.RevisionID == snapshot.HeadTaskID
}
