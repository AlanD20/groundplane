package dnsrecords

import (
	"context"
	"fmt"
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
func (reader *Reader) references(ctx context.Context, revision int64) ([]core.DNSRecord, []keyvalue.Condition, error) {
	kindKey := platformcomponents.PlatformComponentKindKey(core.ComponentKindCoreDNS)
	index, err := reader.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{kindKey}, Revision: revision})
	if err != nil {
		return nil, nil, err
	}
	if index == nil || len(index.Values) != 1 {
		return nil, nil, errs.New(errs.KindInternal, "DNS reference index read is incomplete")
	}
	conditions := []keyvalue.Condition{{Key: kindKey}}
	if index.Values[0] == nil {
		return nil, conditions, nil
	}
	conditions[0].ModRevision = index.Values[0].ModRevision
	id := string(index.Values[0].Value)
	keys := []string{components.RecordKey(id), platformcomponents.ComponentObservationKey(id)}
	read, err := reader.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: index.ReadRevision})
	if err != nil {
		return nil, nil, err
	}
	if read == nil || len(read.Values) != 2 || read.Values[0] == nil {
		return nil, nil, errs.New(errs.KindInternal, "DNS reference authority is missing")
	}
	component, err := components.DecodeRecord(read.Values[0].Value)
	if err != nil {
		return nil, nil, err
	}
	if component.Desired.Config.CoreDNS == nil {
		return nil, nil, errs.New(errs.KindInternal, "DNS config authority is missing")
	}
	records := slices.Clone(component.Desired.Config.CoreDNS.Records)
	for i, key := range keys {
		condition := keyvalue.Condition{Key: key}
		if read.Values[i] != nil {
			condition.ModRevision = read.Values[i].ModRevision
		}
		conditions = append(conditions, condition)
	}
	if read.Values[1] != nil {
		observation, err := platformcomponents.DecodeComponentObservation(read.Values[1].Value)
		if err != nil {
			return nil, nil, err
		}
		if observation.Enabled {
			records = append(records, observation.DNSRecords...)
		}
	}
	return records, conditions, nil
}

func (reader *Reader) RequireDNSUnreferenced(
	ctx context.Context,
	serviceID, zoneID string,
	revision int64,
) ([]keyvalue.Condition, error) {
	records, conditions, err := reader.references(ctx, revision)
	if err != nil {
		return nil, err
	}
	for _, record := range records {
		if serviceID != "" && record.ServiceID == serviceID || zoneID != "" && record.ZoneID == zoneID {
			return nil, referenced(record.Hostname)
		}
	}
	return conditions, nil
}

func (reader *Reader) ProtectDNSProjection(
	ctx context.Context,
	projection environmentprojection.EnvironmentComposeProjection,
	revision int64,
) ([]keyvalue.Condition, error) {
	previous, found, err := blueprints.ReadCurrentProjection(ctx, reader.store, projection.EnvironmentID, revision)
	if err != nil {
		return nil, err
	}
	// Non-network edits cannot invalidate a DNS target. The caller already
	// fences the Environment head used for this comparison.
	if found && sameDNSTargets(previous.Record, projection) {
		return nil, nil
	}
	records, conditions, err := reader.references(ctx, revision)
	if err != nil {
		return nil, err
	}
	for _, record := range records {
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
		if _, err := domain.ProxyPorts(service.Expose); err != nil {
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
	return conditions, nil
}

func (reader *Reader) ProtectDNSHierarchy(
	ctx context.Context,
	kind, id string,
	revision int64,
) ([]keyvalue.Condition, error) {
	records, conditions, err := reader.references(ctx, revision)
	if err != nil {
		return nil, err
	}
	for _, record := range records {
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
	return conditions, nil
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
