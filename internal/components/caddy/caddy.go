// Package caddy is the "caddy" component — the entry router. See
// mvp.md, "Router (ingress components)": one host-reachable service on a
// statically pinned IPv4, the documented exception to "no host port
// publishing".
package caddy

import (
	"fmt"
	"net/netip"
	"sort"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ipam"
	"github.com/AlanD20/groundplane/internal/components"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	caddyfileName       = "components/caddy/Caddyfile"
	caddyImage          = "caddy:2.11.4-alpine"
	caddyServiceName    = "caddy"
	defaultTemplateBody = "{routes}\n"
	routesMarker        = "{routes}"
)

// Register adds this component to the registry. Called once, explicitly,
// from internal/app.NewController.
func Register() {
	implementation := &component{}
	components.Register(components.Registration{
		Kind:          core.ComponentKindIngressCaddy,
		Label:         "Caddy (entry router)",
		AllowedOwners: []core.ComponentOwner{core.ComponentOwnerEnvironment},
		ApplyStrategy: components.EnvironmentRender,
		ConfigSchema:  []string{"zone_id", "caddyfile_template"},
		Environment:   implementation,
	})
}

type component struct{}

func (a *component) Render(
	env core.Environment,
	ad core.Component,
) (map[string]components.GeneratedService, map[string][]byte, error) {
	if err := validateIdentity(env, ad); err != nil {
		return nil, nil, err
	}
	if !ad.Enabled {
		return map[string]components.GeneratedService{}, map[string][]byte{}, nil
	}
	zone, err := selectedZone(env, ad)
	if err != nil {
		return nil, nil, err
	}
	if err := validatePinnedAddress(zone, ad.PinnedIPv4); err != nil {
		return nil, nil, err
	}
	if len(ad.GeneratedServices) != 1 || ad.GeneratedServices[0] == "" {
		return nil, nil, errs.New(errs.KindValidationFailed, "caddy: one stable generated Service id is required")
	}
	routes, err := resolveRoutes(env, zone)
	if err != nil {
		return nil, nil, err
	}
	caddyfile, err := renderCaddyfile(ad.Config, routes)
	if err != nil {
		return nil, nil, err
	}
	service := core.Service{
		ID: ad.GeneratedServices[0], Name: caddyServiceName, Image: caddyImage,
		Zones: []string{zone.Name}, Aliases: map[string][]string{zone.Name: {caddyServiceName}},
		Expose: []string{"80", "443"}, Restart: "unless-stopped", Replicas: 1,
	}
	return map[string]components.GeneratedService{
		caddyServiceName: {
			Service:    service,
			StaticIPv4: map[string]string{zone.Name: ad.PinnedIPv4},
			Mounts: []components.GeneratedMount{
				{Source: caddyfileName, Target: "/etc/caddy/Caddyfile", ReadOnly: true},
				{Source: "components/caddy/data", Target: "/data"},
				{Source: "components/caddy/config", Target: "/config"},
			},
		},
	}, map[string][]byte{caddyfileName: caddyfile}, nil
}

func (a *component) Healthy(env core.Environment, ad core.Component) (bool, error) {
	if !ad.Enabled {
		return false, nil
	}
	if _, _, err := a.Render(env, ad); err != nil {
		return false, err
	}
	return ad.Healthy, nil
}

type resolvedRoute struct {
	route   core.Route
	service core.Service
}

func validateIdentity(env core.Environment, component core.Component) error {
	if env.ID == "" || component.ID == "" || component.Owner != core.ComponentOwnerEnvironment ||
		component.OwnerID != env.ID || component.Kind != core.ComponentKindIngressCaddy {
		return errs.New(errs.KindValidationFailed, "caddy: component ownership or kind is invalid")
	}
	return nil
}

func selectedZone(env core.Environment, component core.Component) (core.Zone, error) {
	for key := range component.Config {
		if key != "zone_id" && key != "caddyfile_template" {
			return core.Zone{}, errs.Newf(errs.KindValidationFailed, "caddy: unknown config field %q", key)
		}
	}
	zoneID, ok := component.Config["zone_id"].(string)
	if !ok || zoneID == "" {
		return core.Zone{}, errs.New(errs.KindValidationFailed, "caddy: config zone_id is required")
	}
	for _, zone := range env.Zones {
		if zone.ID == zoneID {
			return zone, nil
		}
	}
	return core.Zone{}, errs.New(errs.KindValidationFailed, "caddy: selected Zone is not in the Environment")
}

func validatePinnedAddress(zone core.Zone, raw string) error {
	prefix, err := ipam.ParseIPv4Prefix(zone.Subnet)
	if err != nil || prefix.String() != zone.Subnet {
		return errs.New(errs.KindValidationFailed, "caddy: selected Zone must have a canonical usable IPv4 subnet")
	}
	address, err := netip.ParseAddr(raw)
	if err != nil || ipam.ValidateUsableIPv4(prefix, address) != nil {
		return errs.New(errs.KindValidationFailed, "caddy: pinned IPv4 is not usable in the selected Zone")
	}
	return nil
}

func resolveRoutes(env core.Environment, zone core.Zone) ([]resolvedRoute, error) {
	services := make(map[string]core.Service, len(env.Services))
	for _, service := range env.Services {
		services[service.ID] = service
	}
	seen := make(map[string]struct{}, len(env.Routes))
	hostExposure := make(map[string]string, len(env.Routes))
	resolved := make([]resolvedRoute, 0, len(env.Routes))
	for _, route := range env.Routes {
		if err := route.Validate(); err != nil {
			return nil, errs.Wrap(errs.KindValidationFailed, err)
		}
		match := route.Host + "\x00" + route.Path
		if _, duplicate := seen[match]; duplicate {
			return nil, errs.New(errs.KindValidationFailed, "caddy: duplicate Route host and path")
		}
		seen[match] = struct{}{}
		if exposure, found := hostExposure[route.Host]; found && exposure != route.Exposure {
			return nil, errs.New(
				errs.KindValidationFailed,
				"caddy: one host cannot mix public and internal Route exposure",
			)
		}
		hostExposure[route.Host] = route.Exposure
		service, ok := services[route.TargetServiceID]
		if !ok || !safeServiceName(service.Name) {
			return nil, errs.New(errs.KindValidationFailed, "caddy: Route target Service is missing or invalid")
		}
		if !contains(service.Zones, zone.Name) {
			return nil, errs.Newf(
				errs.KindValidationFailed,
				"caddy: Route %s target Service does not join Zone %s",
				route.ID,
				zone.ID,
			)
		}
		if !core.ServiceExposesTCPPort(service.Expose, route.TargetPort) {
			return nil, errs.Newf(
				errs.KindValidationFailed,
				"caddy: Route %s target Service does not expose TCP port %d",
				route.ID,
				route.TargetPort,
			)
		}
		resolved = append(resolved, resolvedRoute{route: route, service: service})
	}
	sort.Slice(resolved, func(i, j int) bool {
		if resolved[i].route.Host != resolved[j].route.Host {
			return resolved[i].route.Host < resolved[j].route.Host
		}
		if len(resolved[i].route.Path) != len(resolved[j].route.Path) {
			return len(resolved[i].route.Path) > len(resolved[j].route.Path)
		}
		return resolved[i].route.ID < resolved[j].route.ID
	})
	return resolved, nil
}

func safeServiceName(name string) bool {
	if name == "" {
		return false
	}
	for index := range len(name) {
		character := name[index]
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') || character == '_' || character == '-' || character == '.' {
			continue
		}
		return false
	}
	return true
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func renderCaddyfile(config map[string]any, routes []resolvedRoute) ([]byte, error) {
	templateBody := defaultTemplateBody
	if raw, exists := config["caddyfile_template"]; exists {
		value, ok := raw.(string)
		if !ok {
			return nil, errs.New(errs.KindValidationFailed, "caddy: caddyfile_template must be a string")
		}
		if value != "" {
			templateBody = value
		}
	}
	if strings.Count(templateBody, routesMarker) != 1 || strings.Contains(templateBody, "{host}") ||
		strings.Contains(templateBody, "{slot}") || strings.IndexByte(templateBody, 0) >= 0 {
		return nil, errs.New(
			errs.KindValidationFailed,
			"caddy: template must contain exactly one {routes} marker and no legacy placeholders",
		)
	}
	rendered := strings.Replace(templateBody, routesMarker, renderRouteBlocks(routes), 1)
	if !strings.HasSuffix(rendered, "\n") {
		rendered += "\n"
	}
	return []byte(rendered), nil
}

func renderRouteBlocks(routes []resolvedRoute) string {
	if len(routes) == 0 {
		return "http:// {\n\trespond 404\n}"
	}
	var output strings.Builder
	for start := 0; start < len(routes); {
		end := start + 1
		for end < len(routes) && routes[end].route.Host == routes[start].route.Host {
			end++
		}
		host := routes[start].route.Host
		if host == "" {
			output.WriteString("http://")
		} else {
			fmt.Fprintf(&output, "http://%s, https://%s", host, host)
		}
		output.WriteString(" {\n")
		allInternal := host != ""
		for index := start; index < end; index++ {
			allInternal = allInternal && routes[index].route.Exposure == "internal"
		}
		if allInternal {
			output.WriteString("\ttls internal\n")
		}
		output.WriteString("\troute {\n")
		for index := start; index < end; index++ {
			path := routes[index].route.Path
			if path == "/" {
				path = "/*"
			}
			fmt.Fprintf(
				&output,
				"\t\thandle %s {\n\t\t\treverse_proxy %s:%d\n\t\t}\n",
				path,
				routes[index].service.Name,
				routes[index].route.TargetPort,
			)
		}
		output.WriteString("\t}\n}\n")
		if end < len(routes) {
			output.WriteByte('\n')
		}
		start = end
	}
	return strings.TrimSuffix(output.String(), "\n")
}
