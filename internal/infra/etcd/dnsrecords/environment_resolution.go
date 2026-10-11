package dnsrecords

import (
	"context"
	"net/netip"
	"sort"
	"strings"

	model "github.com/AlanD20/groundplane/internal/common/dnsrecords"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentqueries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/platformcomponents"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ResolveEnvironmentDNS captures every Environment's shared-resolver input at
// one MVCC revision. The global name fence closes newly published-head phantoms;
// individual selection and claim comparisons fence runtime and ownership changes.
func (reader *Reader) ResolveEnvironmentDNS(
	ctx context.Context, revision int64,
) (platformcomponents.EnvironmentDNSResolution, []keyvalue.Condition, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return platformcomponents.EnvironmentDNSResolution{}, nil, err
	}
	if revision <= 0 {
		return platformcomponents.EnvironmentDNSResolution{}, nil,
			errs.New(errs.KindValidationFailed, "Environment DNS requires a captured read revision")
	}
	generation, err := reader.routeNameCondition(ctx, revision)
	if err != nil {
		return platformcomponents.EnvironmentDNSResolution{}, nil, err
	}
	conditions := []keyvalue.Condition{generation}
	resolution := platformcomponents.EnvironmentDNSResolution{}
	start := ""
	for {
		if err := ctx.Err(); err != nil {
			return platformcomponents.EnvironmentDNSResolution{}, nil, err
		}
		page, err := reader.store.Range(ctx, keyvalue.RangeRequest{
			Prefix:         environmentqueries.EnvironmentDesiredHeadScanPrefix,
			StartExclusive: start, Limit: keyvalue.MaximumPageLimit, Revision: revision,
		})
		if err != nil {
			return platformcomponents.EnvironmentDNSResolution{}, nil, err
		}
		if page == nil || page.ReadRevision != revision || page.More && len(page.Values) == 0 {
			return platformcomponents.EnvironmentDNSResolution{}, nil,
				errs.New(errs.KindInternal, "Environment DNS head scan is incomplete")
		}
		for _, value := range page.Values {
			start = value.Key
			if !strings.HasSuffix(value.Key, "/current") {
				continue
			}
			environmentID := strings.TrimSuffix(strings.TrimPrefix(value.Key,
				environmentqueries.EnvironmentDesiredHeadScanPrefix), "/current")
			if ids.Validate(ids.KindEnvironment, environmentID) != nil {
				return platformcomponents.EnvironmentDNSResolution{}, nil,
					errs.New(errs.KindInternal, "Environment DNS head owner is invalid")
			}
			desired, found, err := blueprints.ReadCurrentDesiredInput(ctx, reader.store, environmentID, revision)
			if err != nil {
				return platformcomponents.EnvironmentDNSResolution{}, nil, err
			}
			if !found {
				return platformcomponents.EnvironmentDNSResolution{}, nil,
					errs.New(errs.KindInternal, "Environment DNS scanned head has no authored input")
			}
			if len(desired.Record.Input.DNS) == 0 {
				continue
			}
			selection, err := reader.SelectAuthoredEnvironmentRecords(ctx, environmentID, revision)
			if err != nil {
				return platformcomponents.EnvironmentDNSResolution{}, nil, err
			}
			bindings := selection.Bindings()
			claims, err := reader.requireEnvironmentBindingClaims(ctx, bindings, revision)
			if err != nil {
				return platformcomponents.EnvironmentDNSResolution{}, nil, err
			}
			conditions = append(conditions, selection.Conditions()...)
			conditions = append(conditions, claims...)
			for _, binding := range bindings {
				resolution.Captures = append(resolution.Captures, platformcomponents.EnvironmentDNSCapture{
					DesiredRevisionID: selection.DesiredRevisionID(), Binding: binding,
				})
			}
		}
		if !page.More {
			break
		}
	}
	sort.Slice(resolution.Captures, func(left, right int) bool {
		return resolution.Captures[left].Binding.Record.Hostname() < resolution.Captures[right].Binding.Record.Hostname()
	})
	byAddress := make(map[netip.Addr][]string)
	for _, capture := range resolution.Captures {
		if capture.Binding.Address != "" {
			address, err := netip.ParseAddr(capture.Binding.Address)
			if err != nil {
				return platformcomponents.EnvironmentDNSResolution{}, nil,
					errs.New(errs.KindInternal, "Environment DNS captured address is invalid")
			}
			byAddress[address] = append(byAddress[address], capture.Binding.Record.Hostname())
		}
	}
	addresses := make([]netip.Addr, 0, len(byAddress))
	for address := range byAddress {
		addresses = append(addresses, address)
	}
	sort.Slice(addresses, func(left, right int) bool {
		return addresses[left].Compare(addresses[right]) < 0
	})
	for _, address := range addresses {
		resolution.Hosts = append(resolution.Hosts, platformcomponents.PlatformDNSHost{
			Address: address.String(), Hostnames: byAddress[address],
		})
	}
	if err := platformcomponents.ValidateEnvironmentDNS(resolution.Captures, resolution.Hosts); err != nil {
		return platformcomponents.EnvironmentDNSResolution{}, nil, err
	}
	conditions, err = uniqueConditions(conditions)
	if err != nil {
		return platformcomponents.EnvironmentDNSResolution{}, nil, err
	}
	return resolution, conditions, nil
}

func (reader *Reader) requireEnvironmentBindingClaims(
	ctx context.Context, bindings []model.EnvironmentBinding, revision int64,
) ([]keyvalue.Condition, error) {
	if len(bindings) == 0 {
		return nil, nil
	}
	keys := make([]string, len(bindings))
	for index, binding := range bindings {
		keys[index] = HostnameClaimKey(binding.Record.Hostname())
	}
	read, err := reader.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, err
	}
	if read == nil || len(read.Values) != len(keys) || read.ReadRevision != revision {
		return nil, errs.New(errs.KindInternal, "Environment DNS claim selection is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	conditions := make([]keyvalue.Condition, len(keys))
	for index, value := range read.Values {
		if value == nil {
			return nil, errs.New(errs.KindStateConflict, "Environment DNS record has no published hostname ownership")
		}
		claim, err := DecodeHostnameClaim(value.Value)
		if err != nil {
			return nil, err
		}
		record := bindings[index].Record
		if claim != (HostnameClaim{
			RecordID: record.ID,
			Hostname: record.Hostname(), EnvironmentID: record.EnvironmentID, Label: record.Label,
		}) {
			return nil, hostnameOwned(claim)
		}
		conditions[index] = keyvalue.Condition{Key: keys[index], ModRevision: value.ModRevision}
	}
	return conditions, nil
}
