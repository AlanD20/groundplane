package etcd

import (
	"context"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"

	dnsmodel "github.com/AlanD20/groundplane/internal/common/dnsrecords"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/dnsrecords"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/platformcomponents"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type BlueprintDNSPreparation struct {
	Change   workqueue.Change
	Complete bool
}

// PrepareBlueprintDNS waits for the shared resolver acknowledgement and returns
// bounded hostname retirement plus native settlement. The scheduler commits this
// with its resource/dependency fence; this owner never publishes independently.
func (repository *EnvironmentBlueprintRepository) PrepareBlueprintDNS(
	ctx context.Context, work workqueue.View, unit blueprintunits.Unit,
	snapshot blueprintunits.Snapshot, at time.Time,
) (BlueprintDNSPreparation, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return BlueprintDNSPreparation{}, err
	}
	parent, item := work.Task.Task, work.Item.Item
	if repository == nil || repository.HierarchyRepository == nil ||
		parent.Validate() != nil || parent.Action != workexecution.ActionApply ||
		parent.Stop != nil || parent.FinishedAt != nil || parent.CollectionStartedAt != nil ||
		!parent.PublicationComplete || item.Operation != workexecution.ResourceDefinition ||
		item.Placement.Executor != workexecution.Controller || item.TaskID != parent.ID ||
		item.TargetID != unit.Target.ID || item.Phase != workexecution.Ready ||
		item.AttemptNumber != 0 || item.ForwardRevoked ||
		unit.Target.Kind != ids.KindDNSRecord || snapshot.HeadTaskID != parent.ID ||
		snapshot.Desired == nil || snapshot.Desired.Record.ParentTaskID != parent.ID ||
		snapshot.EnvironmentID != parent.Scope.EnvironmentID || snapshot.ReadRevision != work.ReadRevision ||
		!at.Before(parent.Deadline) || !at.Before(item.Deadline) {
		return BlueprintDNSPreparation{}, errs.New(errs.KindStateConflict, "Blueprint DNS is not current ready work")
	}
	expected, err := blueprintunits.DefinitionWorkInput(parent, unit)
	if err != nil || item.Input == nil || *item.Input != expected {
		return BlueprintDNSPreparation{}, errs.New(errs.KindStateConflict, "Blueprint DNS input differs from its captured declaration")
	}
	conditions, authoredRevision, err := repository.blueprintDNSAuthority(ctx, work, unit)
	if err != nil {
		return BlueprintDNSPreparation{}, err
	}
	reader := dnsrecords.NewReader(repository.store)
	selection, err := reader.SelectAuthoredEnvironmentRecords(ctx, snapshot.EnvironmentID, snapshot.ReadRevision)
	if err != nil {
		return BlueprintDNSPreparation{}, err
	}
	conditions = append(conditions, selection.Conditions()...)
	conditions = append(conditions,
		keyvalue.Condition{Key: blueprints.EnvironmentBlueprintHeadKey(snapshot.EnvironmentID), ModRevision: snapshot.HeadRevision},
		keyvalue.Condition{Key: blueprintunits.EpochKey(snapshot.EnvironmentID), ModRevision: snapshot.EpochRevision},
		keyvalue.Condition{Key: blueprintunits.DesiredPlanKey(snapshot.EnvironmentID), ModRevision: snapshot.Desired.Revision},
	)
	var binding *dnsmodel.EnvironmentBinding
	for _, candidate := range selection.Bindings() {
		if candidate.Record.ID == unit.Target.ID {
			selected := candidate
			binding = &selected
		}
	}
	if unit.Removal != (binding == nil) {
		return BlueprintDNSPreparation{}, errs.New(errs.KindStateConflict, "Blueprint DNS declaration changed")
	}
	ack, ready, err := readResolverAcknowledgement(ctx, repository.store, snapshot.ReadRevision)
	if err != nil || !ready {
		return BlueprintDNSPreparation{}, err
	}
	conditions = append(conditions, ack.conditions...)
	if !blueprintDNSBindingAcknowledged(ack.observation, unit.Target.ID, binding) {
		failedTaskID, _, err := failedResolverConfigurationRequest(ctx, repository.store, snapshot.ReadRevision, ack)
		if err != nil {
			return BlueprintDNSPreparation{}, err
		}
		if failedTaskID != "" {
			return BlueprintDNSPreparation{}, errs.New(
				errs.KindValidationFailed,
				"CoreDNS update Task "+failedTaskID+" failed; inspect it, repair the configuration and submit a new Blueprint Apply",
			)
		}
		return BlueprintDNSPreparation{}, nil
	}
	keepHostname := ""
	if binding != nil {
		keepHostname = binding.Record.Hostname()
		claim := dnsrecords.HostnameClaim{
			RecordID: binding.Record.ID, EnvironmentID: snapshot.EnvironmentID,
			Label: binding.Record.Label, Hostname: keepHostname,
		}
		claimRead, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
			Keys: []string{dnsrecords.HostnameClaimKey(keepHostname)}, Revision: snapshot.ReadRevision,
		})
		if err != nil {
			return BlueprintDNSPreparation{}, err
		}
		if claimRead == nil || len(claimRead.Values) != 1 || claimRead.Values[0] == nil {
			return BlueprintDNSPreparation{}, errs.New(errs.KindStateConflict, "Blueprint DNS hostname ownership is unavailable")
		}
		defer keyvalue.ClearValues(claimRead.Values)
		stored, err := dnsrecords.DecodeHostnameClaim(claimRead.Values[0].Value)
		if err != nil || stored != claim {
			return BlueprintDNSPreparation{}, errs.New(errs.KindStateConflict, "Blueprint DNS hostname ownership changed")
		}
		conditions = append(conditions, keyvalue.Condition{
			Key: claimRead.Values[0].Key, ModRevision: claimRead.Values[0].ModRevision,
		})
		keyvalue.ClearValues(claimRead.Values)
	}
	retirementConditions, mutations, more, err := reader.PrepareHostnameRetirement(ctx,
		snapshot.EnvironmentID, unit.Target.ID, keepHostname, ack.observation.ResolvedDNSHosts, snapshot.ReadRevision,
	)
	if err != nil {
		return BlueprintDNSPreparation{}, err
	}
	conditions = append(conditions, retirementConditions...)
	if more && len(mutations) == 0 {
		return BlueprintDNSPreparation{}, nil
	}
	if !more {
		proof := blueprintunits.DNSReceipt{
			ComponentID: ack.observation.ComponentID, TaskID: ack.observation.TaskID,
			ObservationRevision: ack.observationRevision, CorefileSHA256: ack.observation.CorefileSHA256, Binding: binding,
		}
		receipt, err := blueprintunits.PrepareDNSWorkSettlement(snapshot, work, unit, authoredRevision, proof, at)
		if err != nil {
			return BlueprintDNSPreparation{}, err
		}
		conditions = append(conditions, receipt.Conditions...)
		mutations = append(mutations, receipt.Mutations...)
	}
	return BlueprintDNSPreparation{Change: workqueue.Change{Conditions: conditions, Mutations: mutations}, Complete: !more}, nil
}

func blueprintDNSBindingAcknowledged(observation platformcomponents.ComponentObservationRecord,
	recordID string, binding *dnsmodel.EnvironmentBinding,
) bool {
	for _, capture := range observation.EnvironmentDNS {
		if capture.Binding.Record.ID == recordID {
			return binding != nil && capture.Binding == *binding
		}
	}
	return binding == nil
}

func (repository *EnvironmentBlueprintRepository) blueprintDNSAuthority(ctx context.Context,
	work workqueue.View, unit blueprintunits.Unit,
) ([]keyvalue.Condition, int64, error) {
	parent := work.Task.Task
	environmentID := parent.Scope.EnvironmentID
	keys := []string{blueprints.EnvironmentBlueprintRootKey(environmentID, parent.ID),
		blueprints.EnvironmentBlueprintOwnedIdentitiesKey(environmentID, parent.ID)}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: work.ReadRevision})
	if err != nil {
		return nil, 0, err
	}
	if read == nil || read.ReadRevision != work.ReadRevision || len(read.Values) != len(keys) {
		return nil, 0, errs.New(errs.KindInternal, "Blueprint DNS authority read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	conditions := []keyvalue.Condition{
		{Key: workqueue.TaskKey(parent.ID), ModRevision: work.Task.Revision},
		{Key: workqueue.ItemKey(work.Item.Item.ID), ModRevision: work.Item.Revision},
	}
	for index, value := range read.Values {
		if value == nil || value.Key != keys[index] || value.ModRevision <= 0 || value.ModRevision > work.ReadRevision {
			return nil, 0, errs.New(errs.KindStateConflict, "Blueprint DNS authority is unavailable")
		}
		conditions = append(conditions, keyvalue.Condition{Key: value.Key, ModRevision: value.ModRevision})
	}
	root, rootErr := blueprints.DecodeEnvironmentBlueprintSeal(read.Values[0].Value)
	owned, ownedErr := environmentprojection.DecodeEnvironmentOwnedIdentities(read.Values[1].Value)
	if rootErr != nil || ownedErr != nil || root.EnvironmentID != environmentID || root.RevisionID != parent.ID ||
		owned.EnvironmentID != environmentID || owned.RevisionID != parent.ID || owned.RenderGeneration != root.RenderGeneration {
		return nil, 0, errs.New(errs.KindStateConflict, "Blueprint DNS captured ownership changed")
	}
	declared := slices.ContainsFunc(owned.DNSRecords, func(value environmentprojection.OwnedIdentity) bool {
		return value.ID == unit.Target.ID
	})
	omitted := slices.ContainsFunc(owned.PendingOmissions, func(value environmentprojection.OwnedOmission) bool {
		return value.Kind == ids.KindDNSRecord && value.ID == unit.Target.ID
	})
	if !unit.Removal && !declared || unit.Removal && (declared || !omitted) {
		return nil, 0, errs.New(errs.KindStateConflict, "Blueprint DNS unit does not match authored ownership")
	}
	return conditions, read.Values[0].ModRevision, nil
}
