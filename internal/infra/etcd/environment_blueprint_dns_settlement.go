package etcd

import (
	"context"
	"slices"

	dnsmodel "github.com/AlanD20/groundplane/internal/common/dnsrecords"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/dnsrecords"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/platformcomponents"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// TrySettleBlueprintDNS waits for the existing shared resolver Task. It never
// dispatches a no-op Agent child or treats desired publication as host evidence.
func (repository *EnvironmentBlueprintRepository) TrySettleBlueprintDNS(
	ctx context.Context, parent TaskRecord, unit blueprintunits.Unit,
) (bool, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return false, err
	}
	if repository == nil || repository.HierarchyRepository == nil ||
		validateBlueprintParentClaimTask(parent) != nil || parent.Status != taskjournal.TaskStatusRunning ||
		parent.Params[blueprints.EnvironmentDesiredRevisionParam] != parent.ID || unit.Target.Kind != ids.KindDNSRecord {
		return false, errs.New(errs.KindValidationFailed, "Blueprint DNS settlement identity is invalid")
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
		return false, errs.New(errs.KindStateConflict, "Blueprint DNS Apply has been superseded")
	}
	conditions, authoredRevision, err := repository.blueprintDNSAuthority(ctx, parent, unit, snapshot.ReadRevision)
	if err != nil {
		return false, err
	}
	reader := dnsrecords.NewReader(repository.store)
	selection, err := reader.SelectAuthoredEnvironmentRecords(ctx, snapshot.EnvironmentID, snapshot.ReadRevision)
	if err != nil {
		return false, err
	}
	conditions = append(conditions, selection.Conditions()...)
	var binding *dnsmodel.EnvironmentBinding
	for _, candidate := range selection.Bindings() {
		if candidate.Record.ID == unit.Target.ID {
			selected := candidate
			binding = &selected
		}
	}
	if unit.Removal != (binding == nil) {
		return false, errs.New(errs.KindStateConflict, "Blueprint DNS declaration changed")
	}
	ack, ready, err := readResolverAcknowledgement(ctx, repository.store, snapshot.ReadRevision)
	if err != nil || !ready {
		return false, err
	}
	conditions = append(conditions, ack.conditions...)
	if !blueprintDNSBindingAcknowledged(ack.observation, unit.Target.ID, binding) {
		failedTaskID, _, err := failedResolverConfigurationRequest(ctx, repository.store, snapshot.ReadRevision, ack)
		if err != nil {
			return false, err
		}
		if failedTaskID != "" {
			return false, errs.New(
				errs.KindValidationFailed,
				"CoreDNS update Task "+failedTaskID+" failed; inspect it, repair the configuration and submit a new Blueprint Apply",
			)
		}
		return false, nil
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
			return false, err
		}
		if claimRead == nil || len(claimRead.Values) != 1 || claimRead.Values[0] == nil {
			return false, errs.New(errs.KindStateConflict, "Blueprint DNS hostname ownership is unavailable")
		}
		stored, err := dnsrecords.DecodeHostnameClaim(claimRead.Values[0].Value)
		if err != nil || stored != claim {
			return false, errs.New(errs.KindStateConflict, "Blueprint DNS hostname ownership changed")
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
		return false, err
	}
	conditions = append(conditions, retirementConditions...)
	if more && len(mutations) == 0 {
		return false, nil
	}
	if !more {
		proof := blueprintunits.DNSReceipt{
			ComponentID: ack.observation.ComponentID, TaskID: ack.observation.TaskID,
			ObservationRevision: ack.observationRevision, CorefileSHA256: ack.observation.CorefileSHA256, Binding: binding,
		}
		receipt, err := blueprintunits.PrepareControllerDNSSettlement(
			snapshot,
			unit,
			ids.New(ids.KindOperation),
			authoredRevision,
			proof,
		)
		if err != nil {
			return false, err
		}
		defer receipt.Clear()
		conditions = append(conditions, receipt.Conditions()...)
		mutations = append(mutations, receipt.Mutations()...)
	}
	result, err := repository.store.Transact(ctx, conditions, mutations)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return false, err
	}
	if !result.Succeeded {
		return false, errs.New(errs.KindStateConflict, "Blueprint DNS settlement raced a configuration change")
	}
	return !more, nil
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
	parent TaskRecord, unit blueprintunits.Unit, revision int64,
) ([]keyvalue.Condition, int64, error) {
	environmentID := parent.Owner.EnvironmentID
	keys := []string{taskjournal.TaskStorageKey(parent.ID), taskjournal.BlueprintParentClaimKey(parent.ID),
		blueprints.EnvironmentBlueprintRootKey(environmentID, parent.ID),
		blueprints.EnvironmentBlueprintOwnedIdentitiesKey(environmentID, parent.ID)}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, 0, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
		return nil, 0, errs.New(errs.KindInternal, "Blueprint DNS authority read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	conditions := []keyvalue.Condition{{Key: taskjournal.BlueprintParentAbortKey(parent.ID)}}
	for index, value := range read.Values {
		if value == nil || value.Key != keys[index] {
			return nil, 0, errs.New(errs.KindStateConflict, "Blueprint DNS authority is unavailable")
		}
		conditions = append(conditions, keyvalue.Condition{Key: value.Key, ModRevision: value.ModRevision})
	}
	storedParent, parentErr := DecodeTaskRecord(read.Values[0].Value)
	claim, claimErr := idempotency.DecodeTaskReference(read.Values[1].Value)
	owned, ownedErr := environmentprojection.DecodeEnvironmentOwnedIdentities(read.Values[3].Value)
	if parentErr != nil || claimErr != nil || ownedErr != nil || claim != parent.ID ||
		validateBlueprintParentClaimTask(storedParent) != nil || storedParent.Status != taskjournal.TaskStatusRunning ||
		storedParent.Owner != parent.Owner || storedParent.PlanID != parent.PlanID || storedParent.PlanHash != parent.PlanHash ||
		storedParent.RenderGeneration != parent.RenderGeneration || owned.EnvironmentID != environmentID ||
		owned.RevisionID != parent.ID || owned.RenderGeneration != uint64(parent.RenderGeneration) {
		return nil, 0, errs.New(errs.KindStateConflict, "Blueprint DNS parent authority changed")
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
	return conditions, read.Values[2].ModRevision, nil
}
