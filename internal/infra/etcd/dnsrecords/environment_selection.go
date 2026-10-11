package dnsrecords

import (
	"context"
	"slices"
	"sort"

	model "github.com/AlanD20/groundplane/internal/common/dnsrecords"
	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentqueries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// EnvironmentSelection pins desired names and selected runtime to one MVCC
// view. Preparing it performs no DNS update and proves no answer is serving.
// Its conditions must accompany the native CoreDNS Task publication.
type EnvironmentSelection struct {
	bindings          []model.EnvironmentBinding
	conditions        []keyvalue.Condition
	readRevision      int64
	desiredRevisionID string
}

func (selection EnvironmentSelection) Bindings() []model.EnvironmentBinding {
	return slices.Clone(selection.bindings)
}

func (selection EnvironmentSelection) Conditions() []keyvalue.Condition {
	return slices.Clone(selection.conditions)
}

func (selection EnvironmentSelection) ReadRevision() int64 { return selection.readRevision }

func (selection EnvironmentSelection) DesiredRevisionID() string { return selection.desiredRevisionID }

// SelectEnvironmentRecords resolves names within the owning Environment only.
// Project/Tenant DNS namespaces never change the target search scope. Missing
// and not-yet-deployed Services keep an intention without an active answer.
func (reader *Reader) SelectEnvironmentRecords(
	ctx context.Context,
	environmentID string,
	records []model.EnvironmentRecord,
	revision int64,
) (EnvironmentSelection, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return EnvironmentSelection{}, err
	}
	if ids.Validate(ids.KindEnvironment, environmentID) != nil || revision < 0 ||
		len(records) > model.MaxEnvironmentRecords {
		return EnvironmentSelection{}, errs.New(errs.KindValidationFailed, "Environment DNS selection is invalid")
	}
	selected := slices.Clone(records)
	sort.Slice(selected, func(left, right int) bool { return selected[left].Hostname() < selected[right].Hostname() })
	for index, record := range selected {
		if err := record.Validate(); err != nil {
			return EnvironmentSelection{}, err
		}
		if record.EnvironmentID != environmentID ||
			index > 0 && selected[index-1].Hostname() == record.Hostname() {
			return EnvironmentSelection{}, errs.New(
				errs.KindValidationFailed,
				"DNS records must have one Environment owner and unique hostnames",
			)
		}
	}
	view, err := reader.environmentDNSView(ctx, environmentID, revision)
	if err != nil {
		return EnvironmentSelection{}, err
	}
	return reader.selectEnvironmentRecordsInView(ctx, selected, view)
}

func (reader *Reader) selectEnvironmentRecordsInView(
	ctx context.Context, selected []model.EnvironmentRecord, view environmentDNSView,
) (EnvironmentSelection, error) {
	selection := EnvironmentSelection{
		readRevision: view.revision, conditions: view.conditions,
		desiredRevisionID: view.identities.Record.RevisionID,
		bindings:          make([]model.EnvironmentBinding, 0, len(selected)),
	}
	for _, record := range selected {
		binding, conditions, err := reader.selectEnvironmentRecord(ctx, record, view)
		if err != nil {
			return EnvironmentSelection{}, err
		}
		selection.bindings = append(selection.bindings, binding)
		selection.conditions = append(selection.conditions, conditions...)
	}
	conditions, err := uniqueConditions(selection.conditions)
	if err != nil {
		return EnvironmentSelection{}, err
	}
	selection.conditions = conditions
	return selection, nil
}

type environmentDNSView struct {
	projection keyvalue.Versioned[environmentprojection.EnvironmentComposeProjection]
	identities keyvalue.Versioned[environmentprojection.EnvironmentOwnedIdentities]
	conditions []keyvalue.Condition
	revision   int64
}

func (reader *Reader) environmentDNSView(
	ctx context.Context,
	environmentID string,
	revision int64,
) (environmentDNSView, error) {
	read, err := reader.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{
			hierarchy.EnvironmentKey(environmentID),
			blueprints.EnvironmentBlueprintHeadKey(environmentID),
		}, Revision: revision,
	})
	if err != nil {
		return environmentDNSView{}, err
	}
	if read == nil || len(read.Values) != 2 || read.ReadRevision <= 0 || revision > 0 && read.ReadRevision != revision {
		return environmentDNSView{}, errs.New(errs.KindInternal, "Environment DNS owner read is incomplete")
	}
	if read.Values[0] == nil {
		return environmentDNSView{}, errs.New(errs.KindEnvironmentNotFound, "Environment was not found")
	}
	owner, err := hierarchy.DecodeEnvironment(read.Values[0].Value)
	if err != nil || owner.ID != environmentID {
		return environmentDNSView{}, errs.New(errs.KindInternal, "Environment DNS owner is corrupt")
	}
	revision = read.ReadRevision
	projectRead, err := reader.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{hierarchy.ProjectKey(owner.ProjectID)}, Revision: revision,
	})
	if err != nil {
		return environmentDNSView{}, err
	}
	if projectRead == nil || len(projectRead.Values) != 1 || projectRead.Values[0] == nil {
		return environmentDNSView{}, errs.New(errs.KindInternal, "Environment DNS Project is missing")
	}
	project, err := hierarchy.DecodeProject(projectRead.Values[0].Value)
	if err != nil || project.ID != owner.ProjectID {
		return environmentDNSView{}, errs.New(errs.KindInternal, "Environment DNS Project is corrupt")
	}
	if project.Kind != hierarchy.ProjectKindTenant {
		return environmentDNSView{}, errs.New(
			errs.KindValidationFailed,
			"tenantless Backing Environments cannot own scoped DNS records; use platform DNS settings",
		)
	}
	fence, err := environmentfence.LoadOrdinary(ctx, reader.store, environmentID, revision)
	if err != nil {
		return environmentDNSView{}, err
	}
	identities, _, err := blueprints.ReadCurrentOwnedIdentities(ctx, reader.store, environmentID, revision)
	if err != nil {
		return environmentDNSView{}, err
	}
	projection, projectionCondition, err := reader.environmentDNSRuntimeProjection(ctx, identities, revision)
	if err != nil {
		return environmentDNSView{}, err
	}
	view := environmentDNSView{
		projection: projection,
		identities: identities,
		conditions: fence.TransactionConditions(),
		revision:   revision,
	}
	head := keyvalue.Condition{Key: blueprints.EnvironmentBlueprintHeadKey(environmentID)}
	if read.Values[1] != nil {
		head.ModRevision = read.Values[1].ModRevision
	}
	view.conditions = append(view.conditions, head)
	if projectionCondition.Key != "" {
		view.conditions = append(view.conditions, projectionCondition)
	}
	return view, nil
}

func (reader *Reader) selectEnvironmentRecord(
	ctx context.Context,
	record model.EnvironmentRecord,
	view environmentDNSView,
) (model.EnvironmentBinding, []keyvalue.Condition, error) {
	binding := model.EnvironmentBinding{Record: record}
	if record.Address != "" {
		binding.Address = record.Address
		return binding, nil, binding.Validate()
	}
	zoneOwned := false
	for _, identity := range view.identities.Record.Networks {
		if identity.ID == record.ZoneID {
			zoneOwned = true
			break
		}
	}
	if !zoneOwned {
		return model.EnvironmentBinding{}, nil, errs.New(
			errs.KindValidationFailed,
			"DNS target Zone must belong to the owning Environment",
		)
	}
	var serviceID string
	for _, identity := range view.identities.Record.Services {
		if identity.Name == record.ServiceName {
			serviceID = identity.ID
			break
		}
	}
	if serviceID == "" {
		return binding, nil, binding.Validate()
	}
	withdrawing, removalConditions, err := reader.environmentServiceWithdrawing(ctx, record, serviceID, view.revision)
	if err != nil {
		return model.EnvironmentBinding{}, nil, err
	}
	if withdrawing || view.projection.Record.RevisionID == "" {
		return binding, removalConditions, binding.Validate()
	}
	if view.projection.Record.RevisionID != view.identities.Record.RevisionID ||
		view.projection.Record.RenderGeneration != view.identities.Record.RenderGeneration {
		return model.EnvironmentBinding{}, nil, errs.New(
			errs.KindStateConflict,
			"Environment DNS runtime projection differs from its declared identities",
		)
	}
	var selectedZone *environmentprojection.EnvironmentZoneProjection
	for index := range view.projection.Record.DesiredZones {
		candidate := &view.projection.Record.DesiredZones[index]
		if candidate.Desired.ID == record.ZoneID {
			selectedZone = candidate
			break
		}
	}
	if selectedZone == nil || selectedZone.EnvironmentID != record.EnvironmentID {
		return model.EnvironmentBinding{}, nil, errs.New(
			errs.KindValidationFailed,
			"DNS target Zone must belong to the owning Environment",
		)
	}
	zone, err := zones.NewRecord(record.EnvironmentID, selectedZone.Desired)
	if err != nil {
		return model.EnvironmentBinding{}, nil, err
	}
	ordinary := environmentqueries.OrdinaryServices(view.projection.Record)
	var selectedService *services.EnvironmentServiceProjection
	for index := range ordinary {
		candidate := &ordinary[index]
		if candidate.Desired.ID == serviceID && candidate.Desired.Name == record.ServiceName {
			selectedService = candidate
			break
		}
	}
	if selectedService == nil {
		return binding, removalConditions, binding.Validate()
	}
	service, err := services.ReadJoined(ctx, reader.store, services.DesiredSelection{
		Services: ordinary, Revision: view.projection.Revision, ReadRevision: view.revision,
	}, selectedService.Desired.ID, blueprints.EnvironmentBlueprintHeadKey(record.EnvironmentID))
	if err != nil {
		return model.EnvironmentBinding{}, nil, err
	}
	conditions := append(
		removalConditions,
		services.ServiceDesiredCondition(service),
		services.ServiceRuntimeCondition(service),
	)
	// A same-named declaration is not a replacement endpoint. Wait until its
	// selected Zone, TCP proxy and lifecycle can supply acknowledged runtime.
	if !ordinaryRunning(service.Record) || !slices.Contains(service.Record.Desired.Zones, zone.Desired.Name) ||
		domain.RequireTCPProxyPort(service.Record.Desired.Expose) != nil {
		return binding, conditions, binding.Validate()
	}
	proxy, err := reader.selectProxy(ctx, service, keyvalue.Versioned[zones.Record]{
		Record: zone, Revision: view.projection.Revision, ReadRevision: view.revision,
	}, view.revision)
	if err != nil {
		return model.EnvironmentBinding{}, nil, err
	}
	conditions = append(conditions, proxy.conditions...)
	if !proxy.waiting() {
		binding.ServiceID, binding.Address = service.Record.Desired.ID, proxy.address
		binding.RuntimeRevision = proxy.runtimeRevision
	}
	return binding, conditions, binding.Validate()
}
