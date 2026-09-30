// Package dnsrecords resolves and protects operator DNS references from durable
// desired and acknowledged runtime authority, never container discovery.
package dnsrecords

import (
	"context"
	model "github.com/AlanD20/groundplane/internal/common/dnsrecords"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentqueries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/platformcomponents"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
	"slices"
	"sort"
)

type Store interface {
	Get(context.Context, string) (*keyvalue.GetResult, error)
	GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error)
	Range(context.Context, keyvalue.RangeRequest) (*keyvalue.RangeResult, error)
}

type Reader struct{ store Store }

func NewReader(store Store) *Reader { return &Reader{store: store} }

func (reader *Reader) ResolveDNSRecords(
	ctx context.Context,
	records []core.DNSRecord,
	revision int64,
) ([]platformcomponents.PlatformDNSHost, []keyvalue.Condition, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return nil, nil, err
	}
	if err := model.ValidateDNSRecords(records); err != nil {
		return nil, nil, err
	}
	hosts := make([]platformcomponents.PlatformDNSHost, 0, len(records))
	var conditions []keyvalue.Condition
	for _, record := range records {
		address := record.Address
		if record.ServiceID != "" {
			service, err := environmentqueries.FindServiceAtRevision(ctx, reader.store, record.ServiceID, revision)
			if err != nil {
				return nil, nil, err
			}
			if revision == 0 {
				revision = service.ReadRevision
			}
			zone, err := environmentqueries.FindZoneAtRevision(ctx, reader.store, record.ZoneID, revision)
			if err != nil {
				return nil, nil, err
			}
			if service.Record.EnvironmentID != zone.Record.EnvironmentID ||
				!slices.Contains(service.Record.Desired.Zones, zone.Record.Desired.Name) ||
				service.Record.Desired.Adapter != "" {
				return nil, nil, errs.New(
					errs.KindValidationFailed,
					"DNS Service target must belong to its selected Zone",
				)
			}
			if _, err := domain.ProxyPorts(service.Record.Desired.Expose); err != nil {
				return nil, nil, err
			}
			fence, err := environmentfence.LoadOrdinary(ctx, reader.store, service.Record.EnvironmentID, revision)
			if err != nil {
				return nil, nil, err
			}
			keys := []string{
				networkreservations.ComponentAddressRegistryKey(record.ZoneID),
				serviceruntimerecord.Key(record.ServiceID),
				deletions.TombstoneKey("service", record.ServiceID),
				deletions.TombstoneKey("zone", record.ZoneID),
			}
			read, err := reader.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
			if err != nil {
				return nil, nil, err
			}
			if read == nil || len(read.Values) != len(keys) {
				return nil, nil, errs.New(errs.KindInternal, "DNS target authority read is incomplete")
			}
			if read.Values[2] != nil || read.Values[3] != nil {
				return nil, nil, errs.New(errs.KindResourceInUse, "DNS target is being removed")
			}
			if read.Values[0] == nil || read.Values[1] == nil {
				return nil, nil, needsDeploy()
			}
			registry, err := recordcodec.Decode[networkreservations.ComponentAddressRegistry](
				read.Values[0].Value,
				"component_address_registry",
			)
			if err != nil || networkreservations.ValidateComponentAddressRegistry(zone.Record, registry) != nil {
				return nil, nil, networkreservations.CorruptComponentAddressRegistry()
			}
			address = registry.ServiceReservations[record.ServiceID]
			runtime, err := releases.DecodeReleaseRecord[serviceruntimerecord.Record](
				read.Values[1].Value,
				"service-acknowledged-runtime",
			)
			if err != nil {
				return nil, nil, err
			}
			if serviceruntimerecord.Validate(runtime) != nil {
				return nil, nil, errs.New(errs.KindInternal, "DNS target runtime authority is corrupt")
			}
			if !appliedProxyAddress(runtime, record.ServiceID, record.ZoneID, address) {
				return nil, nil, needsDeploy()
			}
			conditions = append(conditions, services.ServiceDesiredCondition(service))
			conditions = append(conditions, fence.TransactionConditions()...)
			for i, key := range keys {
				condition := keyvalue.Condition{Key: key}
				if read.Values[i] != nil {
					condition.ModRevision = read.Values[i].ModRevision
				}
				conditions = append(conditions, condition)
			}
		}
		hosts = append(
			hosts,
			platformcomponents.PlatformDNSHost{Address: address, Hostnames: []string{record.Hostname}},
		)
	}
	conditions, err := uniqueConditions(conditions)
	return hosts, conditions, err
}

func needsDeploy() error {
	return errs.New(
		errs.KindStateConflict,
		"Deploy this TCP Service before selecting it for DNS; its stable proxy address has not been applied",
	)
}

func appliedProxyAddress(runtime serviceruntimerecord.Record, serviceID, zoneID, address string) bool {
	if address == "" || runtime.Runtime.ServiceID != serviceID {
		return false
	}
	artifact := &agentpb.ComposeArtifact{}
	if proto.Unmarshal(runtime.Runtime.CurrentArtifact, artifact) != nil {
		return false
	}
	zone := ""
	for _, network := range artifact.Networks {
		if network.NetworkId == zoneID {
			zone = network.ComposeName
			break
		}
	}
	if zone == "" {
		return false
	}
	var document struct {
		Services map[string]struct {
			Networks map[string]struct {
				IPv4 string `yaml:"ipv4_address"`
			} `yaml:"networks"`
		} `yaml:"services"`
	}
	if yaml.Unmarshal(artifact.CanonicalYaml, &document) != nil {
		return false
	}
	for _, service := range artifact.Services {
		if service.ServiceId == serviceID &&
			service.Role == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY &&
			document.Services[service.ComposeName].Networks[zone].IPv4 == address {
			return true
		}
	}
	return false
}

func uniqueConditions(conditions []keyvalue.Condition) ([]keyvalue.Condition, error) {
	byKey := map[string]keyvalue.Condition{}
	for _, condition := range conditions {
		if previous, found := byKey[condition.Key]; found && previous != condition {
			return nil, errs.New(errs.KindStateConflict, "DNS reference changed while being captured")
		}
		byKey[condition.Key] = condition
	}
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]keyvalue.Condition, len(keys))
	for i, key := range keys {
		result[i] = byKey[key]
	}
	return result, nil
}
