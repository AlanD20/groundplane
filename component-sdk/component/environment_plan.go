package component

import (
	"fmt"
	"net/netip"
	"sort"
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

type HTTPRouterInput struct {
	ComponentID       string
	Enabled           bool
	GeneratedServiceID string
	ZoneID            string
	ZoneName          string
	PinnedIPv4        string
	Routes            []HTTPRoute
}

func ValidateHTTPRouterInput(input HTTPRouterInput) error {
	if input.ComponentID == "" {
		return fmt.Errorf("component: HTTP router Component id is required")
	}
	if !input.Enabled {
		if input.GeneratedServiceID != "" || input.ZoneID != "" || input.ZoneName != "" ||
			input.PinnedIPv4 != "" || len(input.Routes) != 0 {
			return fmt.Errorf("component: disabled HTTP router contains runtime input")
		}
		return nil
	}
	address, err := netip.ParseAddr(input.PinnedIPv4)
	if input.GeneratedServiceID == "" || input.ZoneID == "" || !safeName(input.ZoneName) || err != nil ||
		!address.Is4() || address.Is4In6() || address.IsUnspecified() || address.IsMulticast() {
		return fmt.Errorf("component: enabled HTTP router identity is invalid")
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
