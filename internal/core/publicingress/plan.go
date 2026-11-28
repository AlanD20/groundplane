// Package publicingress plans the approved platform HTTP ingress listeners and
// their acknowledged workload targets.
package publicingress

import (
	"encoding/hex"
	"net/netip"
	"slices"
	"sort"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/ingressexecution"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type EndpointKind string

const (
	// EndpointServiceProxy goes directly to a stable Service proxy. The
	// Controller selects this only if the Route has no intervening policy.
	EndpointServiceProxy EndpointKind = "service_proxy"
	// EndpointPublicRouter preserves router policy through a listener
	// independently confirmed to serve only approved public routing.
	EndpointPublicRouter EndpointKind = "public_router"

	HTTPPort              = uint16(80)
	HTTPSPort             = uint16(443)
	maxAddresses          = 16
	maxRoutes             = 512
	MaxConfigurationBytes = 256 << 10
)

// Endpoint is resolved from acknowledged serving state at the captured
// revision. It must not be populated from operator-supplied proxy addresses.
type Endpoint struct {
	Kind                                                 EndpointKind
	HostID, EnvironmentID, ServiceID, ReleaseID, RouteID string
	Address                                              netip.AddrPort
	Relay                                                netip.AddrPort
	RelayProof                                           string
	AcknowledgedRevision                                 int64
}

type Route struct {
	Source        core.Route
	EnvironmentID string
	Endpoint      Endpoint
}

type InternalRoute struct {
	Source        core.Route
	EnvironmentID string
}

// Input is the complete authorized host publication snapshot. The caller
// selects the current host, reserves the listener addresses, and resolves
// every Route and healthy endpoint from one consistent revision.
type Input struct {
	HostID         string
	Revision       int64
	Addresses      []netip.Addr
	Routes         []Route
	InternalRoutes []InternalRoute
}

// Plan describes the exact host bindings and immutable Caddy configuration.
// The host adapter publishes only the two listed TCP bindings per address.
type Plan struct {
	HostID        string
	Revision      int64
	Listeners     []netip.AddrPort
	Routes        []Route
	Configuration []byte
}

// Build rejects stale-shape and ambiguous publications before any host effect.
func Build(input Input) (Plan, error) {
	if ids.Validate(ids.KindHost, input.HostID) != nil || input.Revision <= 0 {
		return Plan{}, errs.New(errs.KindValidationFailed, "public ingress requires a host and captured revision")
	}
	listeners, err := listenersFor(input.Addresses)
	if err != nil {
		return Plan{}, err
	}
	routes, err := routesFor(input.Routes, input.Revision)
	if err != nil {
		return Plan{}, err
	}
	if err := checkInternalOverlaps(routes, input.InternalRoutes); err != nil {
		return Plan{}, err
	}
	config, err := render(routes)
	if err != nil {
		return Plan{}, errs.Wrap(errs.KindInternal, err)
	}
	if len(config) > MaxConfigurationBytes {
		return Plan{}, errs.New(errs.KindValidationFailed, "public ingress configuration size limit exceeded")
	}
	return Plan{
		HostID:        input.HostID,
		Revision:      input.Revision,
		Listeners:     listeners,
		Routes:        routes,
		Configuration: config,
	}, nil
}

func listenersFor(addresses []netip.Addr) ([]netip.AddrPort, error) {
	if len(addresses) == 0 || len(addresses) > maxAddresses {
		return nil, errs.New(errs.KindValidationFailed, "public ingress requires explicit host addresses")
	}
	ordered := slices.Clone(addresses)
	slices.SortFunc(ordered, netip.Addr.Compare)
	result := make([]netip.AddrPort, 0, len(ordered)*2)
	for i, address := range ordered {
		if !bindableAddress(address) || i > 0 && address == ordered[i-1] {
			return nil, errs.New(errs.KindValidationFailed, "public ingress has an invalid or repeated host address")
		}
		result = append(result, netip.AddrPortFrom(address, HTTPPort), netip.AddrPortFrom(address, HTTPSPort))
	}
	return result, nil
}

func bindableAddress(address netip.Addr) bool {
	return address.IsGlobalUnicast() && !address.Is4In6() && address.Zone() == ""
}

func routesFor(input []Route, revision int64) ([]Route, error) {
	if len(input) > maxRoutes {
		return nil, errs.New(errs.KindValidationFailed, "public ingress route limit exceeded")
	}
	owner := make(map[string]string, len(input))
	seenRoutes := make(map[string]bool, len(input))
	seenMatches := make(map[string]bool, len(input))
	result := slices.Clone(input)
	for _, route := range result {
		if err := validateRoute(route, revision); err != nil {
			return nil, err
		}
		source := route.Source
		if previous := owner[source.Host]; previous != "" && previous != route.EnvironmentID {
			return nil, errs.New(errs.KindStateConflict, "public ingress hostname belongs to more than one Environment")
		}
		key := source.Host + "\x00" + strings.ToLower(matchPath(source.Path))
		if seenRoutes[source.ID] || seenMatches[key] {
			return nil, errs.New(errs.KindStateConflict, "public ingress repeats a Route or hostname and path")
		}
		owner[source.Host] = route.EnvironmentID
		seenRoutes[source.ID] = true
		seenMatches[key] = true
	}
	sort.Slice(result, func(i, j int) bool {
		left, right := result[i].Source, result[j].Source
		if left.Host != right.Host {
			return left.Host < right.Host
		}
		leftPath, rightPath := matchPath(left.Path), matchPath(right.Path)
		leftPrefix, leftWildcard := strings.CutSuffix(leftPath, "*")
		rightPrefix, rightWildcard := strings.CutSuffix(rightPath, "*")
		if len(leftPrefix) != len(rightPrefix) {
			return len(leftPrefix) > len(rightPrefix)
		}
		if leftWildcard != rightWildcard {
			return !leftWildcard
		}
		return left.ID < right.ID
	})
	return result, nil
}

// Named virtual hosts take precedence over the hostless internal fallback.
// Overlapping named routes cannot share the edge with an internal origin
// route, even when a public catch-all would otherwise match it.
func checkInternalOverlaps(routes []Route, internalRoutes []InternalRoute) error {
	for _, internal := range internalRoutes {
		if internal.Source.Exposure != "internal" || internal.Source.Validate() != nil ||
			ids.Validate(ids.KindEnvironment, internal.EnvironmentID) != nil {
			return errs.New(errs.KindValidationFailed, "public ingress has invalid internal Route evidence")
		}
		if internal.Source.Host == "" {
			continue
		}
		for _, route := range routes {
			if route.EnvironmentID != internal.EnvironmentID || route.Source.Host != internal.Source.Host {
				continue
			}
			if !safeMatchPath(internal.Source.Path) || overlaps(internal.Source.Path, route.Source.Path) {
				return errs.New(errs.KindStateConflict, "public ingress overlaps an internal Route")
			}
		}
	}
	return nil
}

func overlaps(left, right string) bool {
	leftPath, leftWildcard := strings.CutSuffix(strings.ToLower(matchPath(left)), "*")
	rightPath, rightWildcard := strings.CutSuffix(strings.ToLower(matchPath(right)), "*")
	return leftPath == rightPath ||
		leftWildcard && strings.HasPrefix(rightPath, leftPath) ||
		rightWildcard && strings.HasPrefix(leftPath, rightPath)
}

func validateRoute(route Route, revision int64) error {
	source := route.Source
	if source.Exposure != "public" || source.Validate() != nil ||
		ids.Validate(ids.KindRoute, source.ID) != nil ||
		ids.Validate(ids.KindEnvironment, route.EnvironmentID) != nil ||
		ids.Validate(ids.KindService, source.TargetServiceID) != nil ||
		!strings.Contains(source.Host, ".") || !safeMatchPath(source.Path) {
		return errs.New(errs.KindValidationFailed, "public ingress requires an unambiguous public Route")
	}
	endpoint := route.Endpoint
	if endpoint.RouteID != source.ID || endpoint.EnvironmentID != route.EnvironmentID ||
		endpoint.ServiceID != source.TargetServiceID ||
		ids.Validate(ids.KindHost, endpoint.HostID) != nil ||
		ids.Validate(ids.KindDeployment, endpoint.ReleaseID) != nil ||
		endpoint.AcknowledgedRevision <= 0 || endpoint.AcknowledgedRevision > revision ||
		!bindableAddress(endpoint.Address.Addr()) || !endpoint.Address.Addr().IsPrivate() {
		return errs.New(
			errs.KindStateConflict,
			"public ingress requires an acknowledged private endpoint for the exact Route",
		)
	}
	if endpoint.Relay.IsValid() {
		if !endpoint.Relay.Addr().Is4() || !endpoint.Relay.Addr().IsPrivate() ||
			endpoint.Relay.Port() != ingressexecution.RelayPort ||
			!validRelayProof(endpoint.RelayProof) {
			return errs.New(errs.KindStateConflict, "public ingress has invalid private relay authority")
		}
	} else if endpoint.RelayProof != "" {
		return errs.New(errs.KindStateConflict, "public ingress relay proof lacks an approved listener")
	}
	switch endpoint.Kind {
	case EndpointServiceProxy:
		if source.TargetPort == endpoint.Address.Port() {
			return nil
		}
	case EndpointPublicRouter:
		if endpoint.Address.Port() == HTTPPort {
			return nil
		}
	}
	return errs.New(errs.KindStateConflict, "public ingress endpoint does not match an approved proxy listener")
}

func validRelayProof(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil && strings.ToLower(value) == value
}

func matchPath(path string) string {
	if path == "/" {
		return "/*"
	}
	return path
}

// The edge and origin must agree about the selected path. Keep percent-encoded
// matchers and path traversal aliases out of the direct publication contract.
func safeMatchPath(path string) bool {
	if strings.Contains(path, "%") || strings.Contains(path, "//") {
		return false
	}
	for segment := range strings.SplitSeq(strings.TrimSuffix(path, "*"), "/") {
		if segment == "." || segment == ".." {
			return false
		}
	}
	return true
}
