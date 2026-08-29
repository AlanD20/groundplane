package caddy

import (
	"fmt"
	"strings"

	"github.com/AlanD20/groundplane-component-sdk/component"
)

const (
	caddyfileName         = "components/caddy/Caddyfile"
	caddyDataMarkerName   = "components/caddy/data/.groundplane-managed"
	caddyConfigMarkerName = "components/caddy/config/.groundplane-managed"
	Image                 = "docker.io/library/caddy:2.11.4-alpine@sha256:5f5c8640aae01df9654968d946d8f1a56c497f1dd5c5cda4cf95ab7c14d58648"
	ServiceName           = "caddy"
	defaultTemplateBody   = "{routes}\n"
	routesMarker          = "{routes}"
	ActivateConfigAction  = component.ActionID("activate-config")
	CaddyfileSource       = caddyfileName
	CaddyfileContainer    = "/etc/caddy/Caddyfile"
)

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
	return []string{"zone_id", "caddyfile_template"}
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
	caddyfile, err := renderCaddyfile(config.CaddyfileTemplate, component.SortedHTTPRoutes(input.Routes))
	if err != nil {
		return component.EnvironmentPlan{}, err
	}
	return component.CloneEnvironmentPlan(component.EnvironmentPlan{
		Services: []component.ManagedService{{
			ID: input.GeneratedServiceID, Name: ServiceName, Image: Image,
			NetworkMode: component.ManagedNetworkModeZones,
			Networks: []component.ManagedNetworkAttachment{{
				Name: input.ZoneName, Aliases: []string{ServiceName}, StaticIPv4: input.PinnedIPv4,
			}},
			Expose: []string{"80", "443"}, Restart: "unless-stopped", Replicas: 1,
			Mounts: []component.ManagedMount{
				{Source: caddyfileName, Target: "/etc/caddy/Caddyfile", ReadOnly: true},
				{Source: "components/caddy/data", Target: "/data"},
				{Source: "components/caddy/config", Target: "/config"},
			},
		}},
		Files: []component.ManagedFile{
			{Path: caddyfileName, Content: caddyfile},
			{Path: caddyDataMarkerName, Content: []byte{}},
			{Path: caddyConfigMarkerName, Content: []byte{}},
		},
	}), nil
}

func renderCaddyfile(templateBody string, routes []component.HTTPRoute) ([]byte, error) {
	if templateBody == "" {
		templateBody = defaultTemplateBody
	}
	if strings.Count(templateBody, routesMarker) != 1 || strings.Contains(templateBody, "{host}") ||
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
