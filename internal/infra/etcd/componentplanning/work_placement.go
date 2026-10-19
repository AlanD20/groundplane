package componentplanning

import (
	"context"
	"sort"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareWorkPlacement reserves the candidate address without publishing a
// legacy Component Task intent or an Environment-wide execution lock. The
// returned writes join native input publication under the scheduler's claims.
// Old addresses remain reserved until runtime settlement proves their release.
func (repository *Planner) PrepareWorkPlacement(ctx context.Context, work workqueue.View,
	current keyvalue.Versioned[componentrecord.Record], candidate core.Component, authority FrozenProjectionAuthority,
) (componentrecord.Record, workqueue.Change, error) {
	task, item := work.Task.Task, work.Item.Item
	if task.Validate() != nil || item.Validate() != nil || task.Action != workexecution.ActionApply ||
		!task.PublicationComplete || task.Stop != nil || task.FinishedAt != nil || task.CollectionStartedAt != nil ||
		item.TaskID != task.ID || item.Operation != workexecution.Component || item.Placement.Executor != workexecution.Agent ||
		item.Input != nil || item.AttemptNumber != 0 || item.ForwardRevoked ||
		item.Phase != workexecution.Waiting || item.Wait == nil || item.Wait.Reason != workexecution.WaitInput ||
		componentrecord.ValidateComponentVersion(current) != nil || current.ReadRevision != work.ReadRevision ||
		current.Record.Desired.ID != item.TargetID || candidate.ID != item.TargetID ||
		candidate.Owner != core.ComponentOwnerEnvironment || candidate.OwnerID != task.Scope.EnvironmentID ||
		current.Record.Desired.Owner != candidate.Owner || current.Record.Desired.OwnerID != candidate.OwnerID ||
		!current.Record.Desired.Kind.SharesEnvironmentCapability(
			candidate.Kind,
		) || candidate.PinnedIPv4 != "" || candidate.Healthy ||
		authority.environmentID != task.Scope.EnvironmentID || authority.readRevision != work.ReadRevision ||
		authority.unitRuntimeCondition == nil || authority.desiredHeadRevision <= 0 || authority.desiredRevisionID != task.ID ||
		work.Task.Revision <= 0 || work.Item.Revision <= 0 || work.Task.Revision > work.ReadRevision || work.Item.Revision > work.ReadRevision {
		return componentrecord.Record{}, workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Component placement requires captured unstarted Agent work",
		)
	}
	prior, hasPrior, err := environmentchanges.ComponentAddress(current.Record)
	if err != nil {
		return componentrecord.Record{}, workqueue.Change{}, err
	}
	zoneID, hasCandidate, err := projectedComponentCandidateZone(candidate)
	if err != nil {
		return componentrecord.Record{}, workqueue.Change{}, err
	}
	zones := make(map[string]zonerecord.Record)
	predecessorZones, err := authority.ComponentPredecessorZones()
	if err != nil {
		return componentrecord.Record{}, workqueue.Change{}, err
	}
	for _, zone := range predecessorZones {
		zones[zone.Record.Desired.ID] = zone.Record
	}
	desiredZones := make(map[string]zonerecord.Record)
	for _, zone := range authority.desiredZones {
		desiredZones[zone.Record.Desired.ID] = zone.Record
		zones[zone.Record.Desired.ID] = zone.Record
	}
	if hasCandidate {
		if _, found := desiredZones[zoneID]; !found {
			return componentrecord.Record{}, workqueue.Change{}, errs.New(
				errs.KindStateConflict,
				"Component candidate Zone is absent from captured desired state",
			)
		}
	}
	selected := map[string]bool{}
	if hasCandidate {
		selected[zoneID] = true
	}
	if hasPrior {
		selected[prior.ZoneID()] = true
	}
	ordered := make([]string, 0, len(selected))
	for id := range selected {
		ordered = append(ordered, id)
	}
	sort.Strings(ordered)
	change := workqueue.Change{Conditions: []keyvalue.Condition{
		{Key: workqueue.TaskKey(task.ID), ModRevision: work.Task.Revision},
		{Key: workqueue.ItemKey(item.ID), ModRevision: work.Item.Revision},
		{Key: componentrecord.RecordKey(item.TargetID), ModRevision: current.Revision},
		{
			Key:         blueprints.EnvironmentBlueprintHeadKey(task.Scope.EnvironmentID),
			ModRevision: authority.desiredHeadRevision,
		},
		*authority.unitRuntimeCondition,
	}}
	change.Conditions = append(change.Conditions, authority.proxyServiceConditions...)
	for _, id := range ordered {
		zone, found := zones[id]
		if !found || zonerecord.ValidateRecord(zone) != nil || zone.EnvironmentID != task.Scope.EnvironmentID {
			return componentrecord.Record{}, workqueue.Change{}, errs.New(
				errs.KindStateConflict,
				"Component placement Zone is unavailable",
			)
		}
		key := networkreservations.ComponentAddressRegistryKey(id)
		read, err := repository.store.GetMany(
			ctx,
			keyvalue.GetManyRequest{Keys: []string{key}, Revision: work.ReadRevision},
		)
		if err != nil {
			return componentrecord.Record{}, workqueue.Change{}, err
		}
		if read == nil || read.ReadRevision != work.ReadRevision || len(read.Values) != 1 {
			return componentrecord.Record{}, workqueue.Change{}, errs.New(
				errs.KindStateConflict,
				"Component address snapshot is incomplete",
			)
		}
		registry := networkreservations.ComponentAddressRegistry{Reservations: map[string]string{}}
		revision := keyvalue.RevisionOf(read.Values[0])
		if value := read.Values[0]; value != nil {
			registry, err = recordcodec.Decode[networkreservations.ComponentAddressRegistry](
				value.Value,
				"component_address_registry",
			)
			if err != nil || value.Key != key || revision <= 0 || revision > work.ReadRevision ||
				networkreservations.ValidateComponentAddressRegistry(zone, registry) != nil {
				return componentrecord.Record{}, workqueue.Change{}, networkreservations.CorruptComponentAddressRegistry()
			}
		}
		change.Conditions = append(change.Conditions, keyvalue.Condition{Key: key, ModRevision: revision})
		if hasPrior && prior.ZoneID() == id && registry.Reservations[item.TargetID] != prior.Address() {
			return componentrecord.Record{}, workqueue.Change{}, errs.New(
				errs.KindStateConflict,
				"Component predecessor address reservation changed",
			)
		}
		if hasCandidate && zoneID == id {
			next, address, err := registry.Reserve(zone, item.TargetID)
			if err != nil {
				return componentrecord.Record{}, workqueue.Change{}, err
			}
			candidate.PinnedIPv4 = address
			if registry.Reservations[item.TargetID] != address {
				value, err := networkreservations.EncodeComponentAddressRegistry(zone, next)
				if err != nil {
					return componentrecord.Record{}, workqueue.Change{}, err
				}
				change.Mutations = append(
					change.Mutations,
					keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: value},
				)
			}
		}
	}
	record, err := componentrecord.NewRecord(candidate)
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return componentrecord.Record{}, workqueue.Change{}, err
	}
	return record, change, nil
}
