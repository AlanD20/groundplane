package dnsresolver

import (
	"context"
	componentdns "github.com/AlanD20/groundplane-component-sdk/dnsresolver"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/platformcomponents"
	"net/netip"
)

type DNSRecordsResolver interface {
	ResolveDNSRecords(
		context.Context,
		[]core.DNSRecord,
		int64,
	) ([]platformcomponents.PlatformDNSHost, []keyvalue.Condition, error)
}

func mergeResolverHosts(
	projection componentdns.HostResolutionProjection,
	records []platformcomponents.PlatformDNSHost,
) (componentdns.HostResolutionProjection, []platformcomponents.PlatformDNSHost, error) {
	byName := map[string]netip.Addr{}
	byAddress := map[netip.Addr][]string{}
	for _, host := range projection.Hosts {
		for _, name := range host.Hostnames {
			byName[name] = host.Address
			byAddress[host.Address] = append(byAddress[host.Address], name)
		}
	}
	for _, record := range records {
		address, err := netip.ParseAddr(record.Address)
		if err != nil {
			return componentdns.HostResolutionProjection{}, nil, invalid("DNS record address is invalid")
		}
		for _, name := range record.Hostnames {
			if previous, found := byName[name]; found {
				if previous != address {
					return componentdns.HostResolutionProjection{}, nil, invalid(
						"DNS hostname conflicts with a Route or another record",
					)
				}
				continue
			}
			byName[name] = address
			byAddress[address] = append(byAddress[address], name)
		}
	}
	hosts := make([]componentdns.Host, 0, len(byAddress))
	for address, names := range byAddress {
		hosts = append(hosts, componentdns.Host{Address: address, Hostnames: names})
	}
	next, err := componentdns.NewHostResolutionProjection(projection.InputRevision, projection.InputSHA256, hosts)
	if err != nil {
		return componentdns.HostResolutionProjection{}, nil, err
	}
	durable := make([]platformcomponents.PlatformDNSHost, len(next.Hosts))
	for i, host := range next.Hosts {
		durable[i] = platformcomponents.PlatformDNSHost{
			Address:   host.Address.String(),
			Hostnames: append([]string(nil), host.Hostnames...),
		}
	}
	return next, durable, nil
}

func dnsRecordsIntent(records []core.DNSRecord) requestidempotency.Value {
	values := make([]requestidempotency.Value, len(records))
	for i, record := range records {
		values[i] = requestidempotency.Object(
			requestidempotency.Field{Name: "hostname", Value: requestidempotency.String(record.Hostname)},
			requestidempotency.Field{Name: "address", Value: requestidempotency.String(record.Address)},
			requestidempotency.Field{Name: "service_id", Value: requestidempotency.String(record.ServiceID)},
			requestidempotency.Field{Name: "zone_id", Value: requestidempotency.String(record.ZoneID)},
		)
	}
	return requestidempotency.List(values...)
}
