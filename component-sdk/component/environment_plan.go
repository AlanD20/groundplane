package component

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"sort"
	"strconv"
	"strings"
)

type HTTPRouteExposure string

const (
	HTTPRouteExposurePublic   HTTPRouteExposure = "public"
	HTTPRouteExposureInternal HTTPRouteExposure = "internal"
)

type HTTPRoute struct {
	ID                 string
	Host               string
	Path               string
	BackendServiceID   string
	BackendServiceName string
	TargetPort         uint16
	Exposure           HTTPRouteExposure
}

// HTTPRouterOrigin is the exact managed Service endpoint exposed by one HTTP
// router provider to granted edge transports.
type HTTPRouterOrigin struct {
	ServiceName string
	URL         string
}

type HTTPRouterInput struct {
	ComponentID        string
	Enabled            bool
	GeneratedServiceID string
	ZoneID             string
	ZoneName           string
	PinnedIPv4         string
	Origin             HTTPRouterOrigin
	Routes             []HTTPRoute
}

func ValidateHTTPRouterInput(input HTTPRouterInput) error {
	if input.ComponentID == "" {
		return fmt.Errorf("component: HTTP router Component id is required")
	}
	if !input.Enabled {
		if input.GeneratedServiceID != "" || input.ZoneID != "" || input.ZoneName != "" ||
			input.PinnedIPv4 != "" || input.Origin != (HTTPRouterOrigin{}) || len(input.Routes) != 0 {
			return fmt.Errorf("component: disabled HTTP router contains runtime input")
		}
		return nil
	}
	address, err := netip.ParseAddr(input.PinnedIPv4)
	if input.GeneratedServiceID == "" || input.ZoneID == "" || !safeName(input.ZoneName) || err != nil ||
		!address.Is4() || address.Is4In6() || address.IsUnspecified() || address.IsMulticast() {
		return fmt.Errorf("component: enabled HTTP router identity is invalid")
	}
	if err := ValidateHTTPRouterOrigin(input.Origin); err != nil {
		return err
	}
	seenMatches := make(map[string]struct{}, len(input.Routes))
	hostExposure := make(map[string]HTTPRouteExposure, len(input.Routes))
	for _, route := range input.Routes {
		if err := validateHTTPRoute(route); err != nil {
			return err
		}
		match := route.Host + "\x00" + route.Path
		if _, duplicate := seenMatches[match]; duplicate {
			return fmt.Errorf("component: HTTP router repeats a host and path")
		}
		seenMatches[match] = struct{}{}
		if exposure, found := hostExposure[route.Host]; found && exposure != route.Exposure {
			return fmt.Errorf("component: one HTTP host cannot mix exposure")
		}
		hostExposure[route.Host] = route.Exposure
	}
	return nil
}

// ValidateHTTPRouterOrigin accepts only an explicit canonical plain-HTTP
// endpoint whose host is the managed Service name and whose port is present.
func ValidateHTTPRouterOrigin(origin HTTPRouterOrigin) error {
	parsed, err := url.Parse(origin.URL)
	if err != nil || !safeName(origin.ServiceName) || parsed.Scheme != "http" || parsed.User != nil ||
		parsed.Opaque != "" || parsed.Path != "" || parsed.RawPath != "" || parsed.RawQuery != "" ||
		parsed.Fragment != "" || parsed.ForceQuery || parsed.Hostname() != origin.ServiceName {
		return fmt.Errorf("component: HTTP router origin is invalid")
	}
	port, err := strconv.ParseUint(parsed.Port(), 10, 16)
	if err != nil || port == 0 {
		return fmt.Errorf("component: HTTP router origin is invalid")
	}
	canonical := (&url.URL{
		Scheme: "http",
		Host:   net.JoinHostPort(origin.ServiceName, strconv.FormatUint(port, 10)),
	}).String()
	if origin.URL != canonical {
		return fmt.Errorf("component: HTTP router origin is not canonical")
	}
	return nil
}

func CloneHTTPRouterInput(input HTTPRouterInput) HTTPRouterInput {
	input.Routes = append([]HTTPRoute(nil), input.Routes...)
	return input
}

type ManagedMount struct {
	Source   string
	Target   string
	ReadOnly bool
}

type ManagedNetworkAttachment struct {
	Name       string
	Aliases    []string
	StaticIPv4 string
}

type ManagedDependency struct {
	ServiceName string
	Condition   string
}

type ManagedSecretEnvironment struct {
	Name     string
	SecretID string
}

type ManagedService struct {
	ID                string
	Name              string
	Image             string
	NetworkMode       ManagedNetworkMode
	Command           []string
	Networks          []ManagedNetworkAttachment
	Expose            []string
	Restart           string
	Replicas          uint64
	Mounts            []ManagedMount
	Dependencies      []ManagedDependency
	SecretEnvironment []ManagedSecretEnvironment
}

type ManagedNetworkMode string

const (
	ManagedNetworkModeZones ManagedNetworkMode = "zones"
	ManagedNetworkModeHost  ManagedNetworkMode = "host"
)

func (mode ManagedNetworkMode) Valid() bool {
	return mode == ManagedNetworkModeZones || mode == ManagedNetworkModeHost
}

type ManagedFile struct {
	Path    string
	Content []byte
}

type EnvironmentPlan struct {
	Services []ManagedService
	Files    []ManagedFile
}

// DigestEnvironmentPlan returns a stable digest of every normalized plan
// field. It includes service behavior and file metadata/content so replay
// cannot silently accept a plan whose managed file bytes happen to match.
func DigestEnvironmentPlan(plan EnvironmentPlan) [sha256.Size]byte {
	normalized := CloneEnvironmentPlan(plan)
	sort.SliceStable(normalized.Services, func(left, right int) bool {
		if normalized.Services[left].ID != normalized.Services[right].ID {
			return normalized.Services[left].ID < normalized.Services[right].ID
		}
		return normalized.Services[left].Name < normalized.Services[right].Name
	})
	sort.SliceStable(normalized.Files, func(left, right int) bool {
		return normalized.Files[left].Path < normalized.Files[right].Path
	})
	encoded := make([]byte, 0, 1024)
	encoded = appendPlanString(encoded, "environment-plan-v1")
	encoded = appendPlanCount(encoded, len(normalized.Services))
	for _, service := range normalized.Services {
		encoded = appendPlanString(encoded, service.ID)
		encoded = appendPlanString(encoded, service.Name)
		encoded = appendPlanString(encoded, service.Image)
		encoded = appendPlanString(encoded, string(service.NetworkMode))
		encoded = appendPlanStrings(encoded, service.Command)
		encoded = appendPlanCount(encoded, len(service.Networks))
		for _, network := range service.Networks {
			encoded = appendPlanString(encoded, network.Name)
			encoded = appendPlanStrings(encoded, network.Aliases)
			encoded = appendPlanString(encoded, network.StaticIPv4)
		}
		encoded = appendPlanStrings(encoded, service.Expose)
		encoded = appendPlanString(encoded, service.Restart)
		encoded = appendPlanUint64(encoded, service.Replicas)
		encoded = appendPlanCount(encoded, len(service.Mounts))
		for _, mount := range service.Mounts {
			encoded = appendPlanString(encoded, mount.Source)
			encoded = appendPlanString(encoded, mount.Target)
			encoded = appendPlanBool(encoded, mount.ReadOnly)
		}
		encoded = appendPlanCount(encoded, len(service.Dependencies))
		for _, dependency := range service.Dependencies {
			encoded = appendPlanString(encoded, dependency.ServiceName)
			encoded = appendPlanString(encoded, dependency.Condition)
		}
		encoded = appendPlanCount(encoded, len(service.SecretEnvironment))
		for _, secret := range service.SecretEnvironment {
			encoded = appendPlanString(encoded, secret.Name)
			encoded = appendPlanString(encoded, secret.SecretID)
		}
	}
	encoded = appendPlanCount(encoded, len(normalized.Files))
	for _, file := range normalized.Files {
		encoded = appendPlanString(encoded, file.Path)
		encoded = appendPlanBytes(encoded, file.Content)
	}
	return sha256.Sum256(encoded)
}

func appendPlanString(target []byte, value string) []byte {
	target = appendPlanCount(target, len(value))
	return append(target, value...)
}

func appendPlanBytes(target, value []byte) []byte {
	target = appendPlanCount(target, len(value))
	return append(target, value...)
}

func appendPlanStrings(target []byte, values []string) []byte {
	target = appendPlanCount(target, len(values))
	for _, value := range values {
		target = appendPlanString(target, value)
	}
	return target
}

func appendPlanCount(target []byte, value int) []byte {
	return appendPlanUint64(target, uint64(value))
}

func appendPlanUint64(target []byte, value uint64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	return append(target, encoded[:binary.PutUvarint(encoded[:], value)]...)
}

func appendPlanBool(target []byte, value bool) []byte {
	if value {
		return append(target, 1)
	}
	return append(target, 0)
}

func CloneEnvironmentPlan(plan EnvironmentPlan) EnvironmentPlan {
	cloned := EnvironmentPlan{
		Services: make([]ManagedService, len(plan.Services)),
		Files:    make([]ManagedFile, len(plan.Files)),
	}
	for index, service := range plan.Services {
		service.Command = append([]string(nil), service.Command...)
		service.Expose = append([]string(nil), service.Expose...)
		service.Mounts = append([]ManagedMount(nil), service.Mounts...)
		service.Dependencies = append([]ManagedDependency(nil), service.Dependencies...)
		service.SecretEnvironment = append([]ManagedSecretEnvironment(nil), service.SecretEnvironment...)
		service.Networks = append([]ManagedNetworkAttachment(nil), service.Networks...)
		for networkIndex := range service.Networks {
			service.Networks[networkIndex].Aliases = append(
				[]string(nil),
				service.Networks[networkIndex].Aliases...,
			)
		}
		cloned.Services[index] = service
	}
	for index, file := range plan.Files {
		cloned.Files[index] = ManagedFile{Path: file.Path, Content: append([]byte(nil), file.Content...)}
	}
	return cloned
}

func SortedHTTPRoutes(routes []HTTPRoute) []HTTPRoute {
	result := append([]HTTPRoute(nil), routes...)
	sort.Slice(result, func(left, right int) bool {
		if result[left].Host != result[right].Host {
			return result[left].Host < result[right].Host
		}
		if len(result[left].Path) != len(result[right].Path) {
			return len(result[left].Path) > len(result[right].Path)
		}
		return result[left].ID < result[right].ID
	})
	return result
}

func validateHTTPRoute(route HTTPRoute) error {
	if route.ID == "" || route.BackendServiceID == "" || !safeName(route.BackendServiceName) ||
		route.TargetPort == 0 || !strings.HasPrefix(route.Path, "/") ||
		strings.ContainsAny(route.Host, "\x00\r\n\t {}") || strings.ContainsAny(route.Path, "\x00\r\n{}") {
		return fmt.Errorf("component: HTTP route is invalid")
	}
	if route.Exposure != HTTPRouteExposurePublic && route.Exposure != HTTPRouteExposureInternal {
		return fmt.Errorf("component: HTTP route exposure is invalid")
	}
	return nil
}

func safeName(value string) bool {
	if value == "" {
		return false
	}
	for index := range len(value) {
		character := value[index]
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' || character == '_' || character == '-' || character == '.' {
			continue
		}
		return false
	}
	return true
}
