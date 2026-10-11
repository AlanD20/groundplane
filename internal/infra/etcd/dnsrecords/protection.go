package dnsrecords

import (
	"context"
	"fmt"
	model "github.com/AlanD20/groundplane/internal/common/dnsrecords"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentqueries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/platformcomponents"
	"github.com/AlanD20/groundplane/pkg/errs"
	"slices"
)

// Last-applied records remain protected while replacement is pending or failed.
type referenceSnapshot struct {
	records    []core.DNSRecord
	hosts      []platformcomponents.PlatformDNSHost
	bindings   []model.EnvironmentBinding
	updating   bool
	conditions []keyvalue.Condition
}

func (reader *Reader) references(ctx context.Context, revision int64) (referenceSnapshot, error) {
	kindKey := platformcomponents.PlatformComponentKindKey(core.ComponentKindCoreDNS)
	index, err := reader.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{kindKey}, Revision: revision})
	if err != nil {
		return referenceSnapshot{}, err
	}
	if index == nil || len(index.Values) != 1 {
		return referenceSnapshot{}, errs.New(errs.KindInternal, "DNS reference index read is incomplete")
	}
	conditions := []keyvalue.Condition{{Key: kindKey}}
	if index.Values[0] == nil {
		return referenceSnapshot{conditions: conditions}, nil
	}
	conditions[0].ModRevision = index.Values[0].ModRevision
	id := string(index.Values[0].Value)
	keys := []string{
		components.RecordKey(id),
		platformcomponents.ComponentObservationKey(id),
		platformcomponents.ActiveTaskKey(id),
	}
	read, err := reader.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: index.ReadRevision})
	if err != nil {
		return referenceSnapshot{}, err
	}
	if read == nil || len(read.Values) != len(keys) || read.Values[0] == nil {
		return referenceSnapshot{}, errs.New(errs.KindInternal, "DNS reference authority is missing")
	}
	component, err := components.DecodeRecord(read.Values[0].Value)
	if err != nil {
		return referenceSnapshot{}, err
	}
	if component.Desired.Config.CoreDNS == nil {
		return referenceSnapshot{}, errs.New(errs.KindInternal, "DNS config authority is missing")
	}
	records := slices.Clone(component.Desired.Config.CoreDNS.Records)
	for i, key := range keys {
		condition := keyvalue.Condition{Key: key}
		if read.Values[i] != nil {
			condition.ModRevision = read.Values[i].ModRevision
		}
		conditions = append(conditions, condition)
	}
	snapshot := referenceSnapshot{records: records, conditions: conditions, updating: read.Values[2] != nil}
	if read.Values[1] != nil {
		observation, err := platformcomponents.DecodeComponentObservation(read.Values[1].Value)
		if err != nil {
			return referenceSnapshot{}, err
		}
		if observation.Enabled {
			snapshot.records = append(snapshot.records, observation.DNSRecords...)
			snapshot.hosts = platformcomponents.CloneDNSHosts(observation.ResolvedDNSHosts)
			for _, capture := range observation.EnvironmentDNS {
				snapshot.bindings = append(snapshot.bindings, capture.Binding)
			}
		}
	}
	return snapshot, nil
}

func (reader *Reader) RequireDNSUnreferenced(
	ctx context.Context,
	serviceID, zoneID string,
	revision int64,
) ([]keyvalue.Condition, error) {
	snapshot, err := reader.references(ctx, revision)
	if err != nil {
		return nil, err
	}
	for _, record := range snapshot.records {
		if serviceID != "" && record.ServiceID == serviceID || zoneID != "" && record.ZoneID == zoneID {
			return nil, referenced(record.Hostname)
		}
	}
	for _, binding := range snapshot.bindings {
		if binding.Address != "" && (serviceID != "" && binding.ServiceID == serviceID ||
			zoneID != "" && binding.Record.ZoneID == zoneID) {
			return nil, referenced(binding.Record.Hostname())
		}
	}
	if snapshot.updating {
		return nil, errs.New(
			errs.KindResourceInUse,
			"CoreDNS is updating; wait for its Task before removing the DNS target",
		)
	}
	return snapshot.conditions, nil
}

// PrepareServiceWithdrawal permits Environment-owned names to become waiting
// after the native removal intent is published. Platform names still require an
// explicit successful DNS edit before removal can be accepted.
func (reader *Reader) PrepareServiceWithdrawal(ctx context.Context, serviceID string,
	revision int64,
) ([]keyvalue.Condition, error) {
	snapshot, err := reader.references(ctx, revision)
	if err != nil {
		return nil, err
	}
	for _, record := range snapshot.records {
		if record.ServiceID == serviceID {
			return nil, referenced(record.Hostname)
		}
	}
	return snapshot.conditions, nil
}

// AppliedEnvironmentBindings is the last acknowledged answer selection. It is
// used to order withdrawal before resource removal, never to invent new answers.
func (reader *Reader) AppliedEnvironmentBindings(ctx context.Context, environmentID string,
	revision int64,
) ([]model.EnvironmentBinding, error) {
	snapshot, err := reader.references(ctx, revision)
	if err != nil {
		return nil, err
	}
	var bindings []model.EnvironmentBinding
	for _, binding := range snapshot.bindings {
		if binding.Record.EnvironmentID == environmentID {
			bindings = append(bindings, binding)
		}
	}
	return bindings, nil
}

func (reader *Reader) ProtectDNSProjection(
	ctx context.Context,
	projection environmentprojection.EnvironmentComposeProjection,
	revision int64,
) ([]keyvalue.Condition, error) {
	hostnames := make([]string, 0, len(projection.DesiredRoutes))
	for _, route := range projection.DesiredRoutes {
		if route.Desired.Host != "" {
			hostnames = append(hostnames, route.Desired.Host)
		}
	}
	nameConditions, err := reader.RequireHostnamesUnclaimed(ctx, hostnames, revision)
	if err != nil {
		return nil, err
	}
	previous, found, err := blueprints.ReadCurrentProjection(ctx, reader.store, projection.EnvironmentID, revision)
	if err != nil {
		return nil, err
	}
	// Non-network edits cannot invalidate a DNS target. The caller already
	// fences the Environment head used for this comparison.
	if found && sameDNSTargets(previous.Record, projection) {
		return nameConditions, nil
	}
	snapshot, err := reader.references(ctx, revision)
	if err != nil {
		return nil, err
	}
	for _, record := range snapshot.records {
		if record.ServiceID == "" {
			continue
		}
		previous, err := environmentqueries.FindServiceAtRevision(ctx, reader.store, record.ServiceID, revision)
		if err != nil {
			return nil, err
		}
		if previous.Record.EnvironmentID != projection.EnvironmentID {
			continue
		}
		var service *core.Service
		for i := range projection.DesiredServices {
			if projection.DesiredServices[i].Desired.ID == record.ServiceID {
				service = &projection.DesiredServices[i].Desired
				break
			}
		}
		if service == nil || service.Adapter != "" {
			return nil, referenced(record.Hostname)
		}
		if err := domain.RequireTCPProxyPort(service.Expose); err != nil {
			return nil, referenced(record.Hostname)
		}
		zoneFound := false
		for _, zone := range projection.DesiredZones {
			if zone.Desired.ID == record.ZoneID && slices.Contains(service.Zones, zone.Desired.Name) {
				zoneFound = true
				break
			}
		}
		if !zoneFound {
			return nil, referenced(record.Hostname)
		}
	}
	return uniqueConditions(append(snapshot.conditions, nameConditions...))
}

func (reader *Reader) ProtectDNSHierarchy(
	ctx context.Context,
	kind, id string,
	revision int64,
) ([]keyvalue.Condition, error) {
	snapshot, err := reader.references(ctx, revision)
	if err != nil {
		return nil, err
	}
	for _, record := range snapshot.records {
		if record.ServiceID == "" {
			continue
		}
		service, err := environmentqueries.FindServiceAtRevision(ctx, reader.store, record.ServiceID, revision)
		if err != nil {
			return nil, err
		}
		if kind == "environment" && service.Record.EnvironmentID == id {
			return nil, referenced(record.Hostname)
		}
		if kind != "tenant" && kind != "project" && kind != "backing" {
			continue
		}
		read, err := reader.store.GetMany(
			ctx,
			keyvalue.GetManyRequest{
				Keys:     []string{hierarchy.EnvironmentKey(service.Record.EnvironmentID)},
				Revision: revision,
			},
		)
		if err != nil {
			return nil, err
		}
		if read == nil || len(read.Values) != 1 || read.Values[0] == nil {
			return nil, errs.New(errs.KindInternal, "DNS target Environment is missing")
		}
		environment, err := hierarchy.DecodeEnvironment(read.Values[0].Value)
		if err != nil {
			return nil, err
		}
		if (kind == "project" || kind == "backing") && environment.ProjectID == id {
			return nil, referenced(record.Hostname)
		}
		if kind == "tenant" {
			read, err := reader.store.GetMany(
				ctx,
				keyvalue.GetManyRequest{
					Keys:     []string{hierarchy.ProjectKey(environment.ProjectID)},
					Revision: revision,
				},
			)
			if err != nil {
				return nil, err
			}
			if read == nil || len(read.Values) != 1 || read.Values[0] == nil {
				return nil, errs.New(errs.KindInternal, "DNS target Project is missing")
			}
			project, err := hierarchy.DecodeProject(read.Values[0].Value)
			if err != nil {
				return nil, err
			}
			if project.TenantID == id {
				return nil, referenced(record.Hostname)
			}
		}
	}
	return snapshot.conditions, nil
}

func referenced(hostname string) error {
	return errs.New(
		errs.KindResourceInUse,
		fmt.Sprintf(
			"DNS record %s targets this resource; remove the record in CoreDNS and wait for its Task to complete first",
			hostname,
		),
	)
}
