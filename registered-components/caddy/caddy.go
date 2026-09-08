package caddy

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/AlanD20/groundplane-component-sdk/component"
)

const (
	caddyfileName         = "components/caddy/Caddyfile"
	caddyDataMarkerName   = "components/caddy/data/.groundplane-managed"
	caddyConfigMarkerName = "components/caddy/config/.groundplane-managed"
	ServiceName           = "caddy"
	OriginURL             = "http://caddy:80"
	defaultTemplateBody   = "{routes}\n"
	routesMarker          = "{routes}"
	maxTemplateBytes      = 32 << 10
	ActivateConfigAction  = component.ActionID("activate-config")
	CaddyfileSource       = caddyfileName
	CaddyfileContainer    = "/etc/caddy/Caddyfile"
)

var Image = component.OCIImage{
	Repository:  "docker.io/library/caddy",
	IndexDigest: "5f5c8640aae01df9654968d946d8f1a56c497f1dd5c5cda4cf95ab7c14d58648",
	Platforms: []component.OCIPlatform{
		{
			OS:           "linux",
			Architecture: "amd64",
			ChildDigest:  "98eb57d882ccd5213d1688764db10c1ca2c58a1ca3a6717a3411ad798f7a423a",
			ConfigDigest: "af555904a0961945f16bb323a501457b13a4f7e9bde969b145b97da80b38ecbe",
		},
		{
			OS:           "linux",
			Architecture: "arm64",
			Variant:      "v8",
			ChildDigest:  "1172d4213087d3fc30bafc7ff2c2896180eb0c41ff7f75f315568fb36cabdcba",
			ConfigDigest: "6b08c1b9858ca9a7d99c1da13c3695081e0e604c6cf214ca26a7ce0e2c4fd9b4",
		},
	},
}

func ValidateConfigCommand() []string {
	return []string{"caddy", "validate", "--config", CaddyfileContainer, "--adapter", "caddyfile"}
}

func ActivateConfigCommand() []string {
	return []string{"caddy", "reload", "--config", CaddyfileContainer, "--adapter", "caddyfile"}
}

type Config struct {
	CaddyfileTemplate string
}

func ConfigFields() []string {
	return []string{"zone_ids", "caddyfile_template"}
}

func Definition() (component.Definition, error) {
	routes, err := component.NewGrant(
		component.CapabilityRoutes,
		component.OperationList,
		component.OperationRead,
	)
	if err != nil {
		return component.Definition{}, err
	}
	services, err := component.NewGrant(
		component.CapabilityServices,
		component.OperationRead,
		component.OperationCreate,
	)
	if err != nil {
		return component.Definition{}, err
	}
	networks, err := component.NewGrant(component.CapabilityNetworks, component.OperationRead)
	if err != nil {
		return component.Definition{}, err
	}
	managedConfig, err := component.NewGrant(
		component.CapabilityManagedConfig,
		component.OperationConfigure,
		component.OperationActivate,
	)
	if err != nil {
		return component.Definition{}, err
	}
	hostTrust, err := component.NewGrant(component.CapabilityHostTrust, component.OperationConfigure)
	if err != nil {
		return component.Definition{}, err
	}
	activate, err := component.NewActionDefinition(
		ActivateConfigAction,
		component.CapabilityManagedConfig,
		component.OperationActivate,
	)
	if err != nil {
		return component.Definition{}, err
	}
	return component.NewDefinition(component.DefinitionInput{
		Implementation: "caddy",
		ConfigVariant:  "caddy-v1",
		Provides:       []component.Capability{component.CapabilityHTTPRouter},
		Grants:         []component.Grant{routes, services, networks, managedConfig, hostTrust},
		OwnerScopes:    []component.OwnerScope{component.OwnerScopeEnvironment},
		Actions:        []component.ActionDefinition{activate},
	})
}

func Plan(input component.HTTPRouterInput, config Config) (component.EnvironmentPlan, error) {
	input = component.CloneHTTPRouterInput(input)
	if err := component.ValidateHTTPRouterInput(input); err != nil {
		return component.EnvironmentPlan{}, err
	}
	if !input.Enabled {
		return component.EnvironmentPlan{}, nil
	}
	if input.Origin.ServiceName != ServiceName || input.Origin.URL != OriginURL {
		return component.EnvironmentPlan{}, fmt.Errorf("caddy: HTTP router origin does not match the managed Service")
	}
	caddyfile, err := renderCaddyfile(config.CaddyfileTemplate, component.SortedHTTPRoutes(input.Routes))
	if err != nil {
		return component.EnvironmentPlan{}, err
	}
	return component.CloneEnvironmentPlan(component.EnvironmentPlan{
		Services: []component.ManagedService{{
			ID: input.GeneratedServiceID, Name: ServiceName, Image: Image,
			NetworkMode: component.ManagedNetworkModeZones,
			Networks:    caddyNetworks(input.Zones),
			Expose:      []string{"80", "443"}, Restart: "unless-stopped", Replicas: 1,
			// Observe the running admin API, not a new process's version.
			// https://caddyserver.com/docs/api#get-configpath
			Healthcheck: &component.ManagedHealthcheck{
				Command:         []string{"wget", "-q", "-O", "/dev/null", "http://127.0.0.1:2019/config/"},
				IntervalSeconds: 5, TimeoutSeconds: 3, StartPeriodSeconds: 5, Retries: 3,
			},
			Mounts: []component.ManagedMount{
				{
					Source:   caddyfileName,
					Target:   "/etc/caddy/Caddyfile",
					Kind:     component.ManagedMountKindFile,
					ReadOnly: true,
				},
				{Source: "components/caddy/data", Target: "/data", Kind: component.ManagedMountKindDirectory},
				{Source: "components/caddy/config", Target: "/config", Kind: component.ManagedMountKindDirectory},
			},
		}},
		Files: []component.ManagedFile{
			{Path: caddyfileName, Content: caddyfile},
			{Path: caddyDataMarkerName, Content: []byte{}},
			{Path: caddyConfigMarkerName, Content: []byte{}},
		},
	}), nil
}

func caddyNetworks(zones []component.HTTPRouterZoneInput) []component.ManagedNetworkAttachment {
	networks := make([]component.ManagedNetworkAttachment, len(zones))
	for index, zone := range zones {
		networks[index] = component.ManagedNetworkAttachment{
			Name: zone.Name, Aliases: []string{ServiceName}, StaticIPv4: zone.StaticIPv4,
		}
	}
	return networks
}

func renderCaddyfile(templateBody string, routes []component.HTTPRoute) ([]byte, error) {
	if templateBody == "" {
		templateBody = defaultTemplateBody
	}
	if len(templateBody) > maxTemplateBytes || !utf8.ValidString(templateBody) ||
		strings.Count(templateBody, routesMarker) != 1 || strings.Contains(templateBody, "{host}") ||
		strings.Contains(templateBody, "{slot}") || strings.IndexByte(templateBody, 0) >= 0 {
		return nil, fmt.Errorf(
			"caddy: template must contain exactly one {routes} marker and no legacy placeholders",
		)
	}
	rendered := strings.Replace(templateBody, routesMarker, renderRouteBlocks(routes), 1)
	if !strings.HasSuffix(rendered, "\n") {
		rendered += "\n"
	}
	return []byte(rendered), nil
}

func renderRouteBlocks(routes []component.HTTPRoute) string {
	if len(routes) == 0 {
		return "http:// {\n\trespond 404\n}"
	}
	var output strings.Builder
	for start := 0; start < len(routes); {
		end := start + 1
		for end < len(routes) && routes[end].Host == routes[start].Host {
			end++
		}
		host := routes[start].Host
		if host == "" {
			output.WriteString("http://")
		} else {
			fmt.Fprintf(&output, "http://%s, https://%s", host, host)
		}
		output.WriteString(" {\n")
		allInternal := host != ""
		for index := start; index < end; index++ {
			allInternal = allInternal && routes[index].Exposure == component.HTTPRouteExposureInternal
		}
		if allInternal {
			output.WriteString("\ttls internal\n")
		}
		output.WriteString("\troute {\n")
		for index := start; index < end; index++ {
			path := routes[index].Path
			if path == "/" {
				path = "/*"
			}
			fmt.Fprintf(
				&output,
				"\t\thandle %s {\n\t\t\treverse_proxy %s:%d\n\t\t}\n",
				path,
				routes[index].BackendServiceName,
				routes[index].TargetPort,
			)
		}
		output.WriteString("\t\trespond 404\n")
		output.WriteString("\t}\n}\n")
		if end < len(routes) {
			output.WriteByte('\n')
		}
		start = end
	}
	return strings.TrimSuffix(output.String(), "\n")
}
