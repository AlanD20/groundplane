// Package dnsrecords resolves and protects operator DNS references from durable
// desired and acknowledged runtime authority, never container discovery.
package dnsrecords

import (
	"context"
	model "github.com/AlanD20/groundplane/internal/common/dnsrecords"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentqueries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/platformcomponents"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
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
	names := make([]string, len(records))
	for index, record := range records {
		names[index] = record.Hostname
	}
	conditions, err := reader.RequireHostnamesUnclaimed(ctx, names, revision)
	if err != nil {
		return nil, nil, err
	}
	hosts := make([]platformcomponents.PlatformDNSHost, 0, len(records))
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
			fence, err := environmentfence.LoadOrdinary(ctx, reader.store, service.Record.EnvironmentID, revision)
			if err != nil {
				return nil, nil, err
			}
			selection, err := reader.selectProxy(ctx, service, zone, revision)
			if err != nil {
				return nil, nil, err
			}
			if selection.waiting() {
				return nil, nil, needsDeploy()
			}
			address = selection.address
			conditions = append(conditions, selection.conditions...)
			conditions = append(conditions, fence.TransactionConditions()...)
		}
		hosts = append(
			hosts,
			platformcomponents.PlatformDNSHost{Address: address, Hostnames: []string{record.Hostname}},
		)
	}
	conditions, err = uniqueConditions(conditions)
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
