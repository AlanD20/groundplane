package dnsresolver

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/netip"
	"sort"
	"strings"

	componentdns "github.com/AlanD20/groundplane-component-sdk/dnsresolver"
	"github.com/AlanD20/groundplane/internal/common/ids"
)

type RouteHostState string

const (
	RouteHostPending  RouteHostState = "pending"
	RouteHostObserved RouteHostState = "observed"
	RouteHostFailed   RouteHostState = "failed"
	RouteHostUnserved RouteHostState = "unserved"
	RouteHostDisabled RouteHostState = "disabled"
)

type RouteHostInput struct {
	EnvironmentID     string
	DesiredRevisionID string
	AppliedRevision   int64
	RouteID           string
	ServiceID         string
	Host              string
	Address           netip.Addr
	State             RouteHostState
}

type RouteHost struct {
	EnvironmentID     string `json:"environment_id"`
	DesiredRevisionID string `json:"desired_revision_id"`
	AppliedRevision   int64  `json:"applied_revision"`
	RouteID           string `json:"route_id"`
	ServiceID         string `json:"service_id"`
	Hostname          string `json:"hostname"`
	IPv4              string `json:"ipv4"`
}

type HostProjection struct {
	Resolver componentdns.HostResolutionProjection
	Routes   []RouteHost
}

func BuildRouteHostProjection(inputRevision int64, input []RouteHostInput) (HostProjection, error) {
	routes := make([]RouteHost, 0, len(input))
	seenRoutes := make(map[string]struct{}, len(input))
	addressByHost := make(map[string]netip.Addr, len(input))
	for _, source := range input {
		if !validRouteHostState(source.State) || ids.Validate(ids.KindEnvironment, source.EnvironmentID) != nil ||
			ids.Validate(
				ids.KindTask,
				source.DesiredRevisionID,
			) != nil || ids.Validate(ids.KindRoute, source.RouteID) != nil ||
			ids.Validate(ids.KindService, source.ServiceID) != nil || !validRouteHostName(source.Host) {
			return HostProjection{}, fmt.Errorf("dns resolver: Route host source is invalid")
		}
		routeKey := source.EnvironmentID + "\x00" + source.RouteID
		if _, duplicate := seenRoutes[routeKey]; duplicate {
			return HostProjection{}, fmt.Errorf("dns resolver: Route host identity is duplicated")
		}
		seenRoutes[routeKey] = struct{}{}
		if source.State != RouteHostObserved {
			continue
		}
		if source.AppliedRevision <= 0 || !validRouteHostAddress(source.Address) {
			return HostProjection{}, fmt.Errorf("dns resolver: observed Route host evidence is invalid")
		}
		if address, exists := addressByHost[source.Host]; exists && address != source.Address {
			return HostProjection{}, fmt.Errorf("dns resolver: hostname maps to more than one address")
		}
		addressByHost[source.Host] = source.Address
		routes = append(routes, RouteHost{
			EnvironmentID: source.EnvironmentID, DesiredRevisionID: source.DesiredRevisionID,
			AppliedRevision: source.AppliedRevision, RouteID: source.RouteID, ServiceID: source.ServiceID,
			Hostname: source.Host, IPv4: source.Address.String(),
		})
	}
	sort.Slice(routes, func(left, right int) bool { return routeHostKey(routes[left]) < routeHostKey(routes[right]) })
	encoded, err := json.Marshal(routes)
	if err != nil {
		return HostProjection{}, fmt.Errorf("dns resolver: encode host projection: %w", err)
	}
	digest := sha256.Sum256(encoded)
	byAddress := make(map[netip.Addr][]string)
	for _, route := range routes {
		address := netip.MustParseAddr(route.IPv4)
		byAddress[address] = append(byAddress[address], route.Hostname)
	}
	hosts := make([]componentdns.Host, 0, len(byAddress))
	for address, names := range byAddress {
		hosts = append(hosts, componentdns.Host{Address: address, Hostnames: names})
	}
	resolver, err := componentdns.NewHostResolutionProjection(inputRevision, digest, hosts)
	if err != nil {
		return HostProjection{}, err
	}
	return HostProjection{Resolver: resolver, Routes: routes}, nil
}

func routeHostKey(route RouteHost) string {
	return strings.Join([]string{
		route.EnvironmentID, route.Hostname, route.RouteID, route.ServiceID, route.DesiredRevisionID,
		fmt.Sprintf("%020d", route.AppliedRevision), route.IPv4,
	}, "\x00")
}

func validRouteHostState(state RouteHostState) bool {
	switch state {
	case RouteHostPending, RouteHostObserved, RouteHostFailed, RouteHostUnserved, RouteHostDisabled:
		return true
	default:
		return false
	}
}

func validRouteHostAddress(address netip.Addr) bool {
	return address.IsValid() && address.Is4() && !address.Is4In6() && !address.IsUnspecified() && !address.IsMulticast()
}

func validRouteHostName(value string) bool {
	if value == "" || len(value) > 253 || value[len(value)-1] == '.' {
		return false
	}
	for _, label := range strings.Split(value, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, character := range label {
			if !(character == '-' || character >= 'a' && character <= 'z' || character >= '0' && character <= '9') {
				return false
			}
		}
	}
	return true
}
