package envoy

import (
	"fmt"

	"github.com/AlanD20/groundplane-component-sdk/component"
)

const (
	ServiceName          = "envoy"
	OriginURL            = "http://envoy:80"
	ActivateConfigAction = component.ActionID("activate-config")
	ConfigurationSource  = "components/envoy/router.input"
	ConfigurationTarget  = "/etc/envoy/router.input"
	RuntimeDirectory     = "/tmp/groundplane-envoy-router"
	BootstrapTarget      = RuntimeDirectory + "/bootstrap.json"
	ClustersTarget       = RuntimeDirectory + "/clusters.json"
	RoutesTarget         = RuntimeDirectory + "/routes.json"
	ManifestTarget       = RuntimeDirectory + "/manifest.json"
)

type Config struct {
	Alias string
}

func ConfigFields() []string { return []string{"zone_ids", "alias"} }

func Definition() (component.Definition, error) {
	routes, err := component.NewGrant(component.CapabilityRoutes, component.OperationList, component.OperationRead)
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
	configuration, err := component.NewGrant(
		component.CapabilityManagedConfig,
		component.OperationConfigure,
		component.OperationActivate,
	)
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
		Implementation: "envoy", ConfigVariant: "envoy-v1",
		Provides:    []component.Capability{component.CapabilityHTTPRouter},
		Grants:      []component.Grant{routes, services, networks, configuration},
		OwnerScopes: []component.OwnerScope{component.OwnerScopeEnvironment},
		Actions:     []component.ActionDefinition{activate},
	})
}

// Plan consumes the same scoped HTTP capability snapshot as Caddy. Transport
// forwarding belongs to the Service proxy, not to these host/path Routes.
func Plan(input component.HTTPRouterInput, config Config) (component.EnvironmentPlan, error) {
	input = component.CloneHTTPRouterInput(input)
	if !component.ValidRouterAlias(config.Alias) {
		return component.EnvironmentPlan{}, fmt.Errorf("envoy: invalid router alias")
	}
	if err := component.ValidateHTTPRouterInput(input); err != nil {
		return component.EnvironmentPlan{}, err
	}
	if !input.Enabled {
		return component.EnvironmentPlan{}, nil
	}
	if input.Origin.ServiceName != ServiceName || input.Origin.URL != OriginURL {
		return component.EnvironmentPlan{}, fmt.Errorf("envoy: HTTP router origin does not match the managed Service")
	}
	content, err := renderConfiguration(component.SortedHTTPRoutes(input.Routes))
	if err != nil {
		return component.EnvironmentPlan{}, err
	}
	networks := make([]component.ManagedNetworkAttachment, len(input.Zones))
	for index, zone := range input.Zones {
		networks[index] = component.ManagedNetworkAttachment{
			Name: zone.Name, StaticIPv4: zone.StaticIPv4, Aliases: []string{ServiceName},
		}
		if index == 0 && config.Alias != "" && config.Alias != ServiceName {
			networks[index].Aliases = append(networks[index].Aliases, config.Alias)
		}
	}
	return component.CloneEnvironmentPlan(component.EnvironmentPlan{
		Services: []component.ManagedService{{
			ID: input.GeneratedServiceID, Name: ServiceName, Image: Image,
			NetworkMode: component.ManagedNetworkModeZones, Networks: networks,
			Expose: []string{"80"}, Restart: "unless-stopped", Replicas: 1,
			Command: []string{"sh", "-ec", startupCommand},
			Healthcheck: &component.ManagedHealthcheck{
				Command: []string{"env", "-i", "PATH=/usr/local/bin:/usr/bin:/bin", "timeout", "3",
					"bash", "--noprofile", "--norc", "-ec", readyCommand},
				IntervalSeconds: 5, TimeoutSeconds: 4, StartPeriodSeconds: 5, Retries: 3,
			},
			Mounts: []component.ManagedMount{{Source: "components/envoy", Target: "/etc/envoy",
				Kind: component.ManagedMountKindDirectory, ReadOnly: true}},
		}},
		Files: []component.ManagedFile{{Path: ConfigurationSource, Content: content}},
	}), nil
}

// These commands are compiled image operations, never operator-authored shell.
// The upstream entrypoint drops privileges to envoy before starting this shell.
const startupCommand = "umask 077; command -v bash timeout sed sync mv cat tr sha256sum cut mktemp rm >/dev/null; " +
	"test ! -L " + RuntimeDirectory + "; mkdir -p " + RuntimeDirectory + "; " +
	"test ! -L " + BootstrapTarget + "; test ! -L " + ClustersTarget + "; " +
	"test ! -L " + RoutesTarget + "; test ! -L " + ManifestTarget + "; " +
	"if [ ! -e " + BootstrapTarget + " ]; then sed -n '1p' " + ConfigurationTarget + " > " + BootstrapTarget + ".next; " +
	"sync; mv -f " + BootstrapTarget + ".next " + BootstrapTarget + "; sync; fi; " +
	"if [ ! -e " + ClustersTarget + " ]; then sed -n '2p' " + ConfigurationTarget + " > " + ClustersTarget + ".next; " +
	"sync; mv -f " + ClustersTarget + ".next " + ClustersTarget + "; sync; fi; " +
	"if [ ! -e " + RoutesTarget + " ]; then sed -n '3p' " + ConfigurationTarget + " > " + RoutesTarget + ".next; " +
	"sync; mv -f " + RoutesTarget + ".next " + RoutesTarget + "; sync; fi; " +
	"if [ ! -e " + ManifestTarget + " ]; then sed -n '4p' " + ConfigurationTarget + " > " + ManifestTarget + ".next; " +
	"sync; mv -f " + ManifestTarget + ".next " + ManifestTarget + "; sync; fi; " +
	"exec envoy --config-path " + BootstrapTarget + " --disable-hot-restart"

const readyCommand = "exec 3<>/dev/tcp/127.0.0.1/9901; " +
	"printf 'GET /ready HTTP/1.0\\r\\nHost: localhost\\r\\nConnection: close\\r\\n\\r\\n' >&3; " +
	"IFS= read -r status <&3; case \"$status\" in 'HTTP/1.0 200 '*|'HTTP/1.1 200 '*) exit 0;; *) exit 1;; esac"

func PreflightConfigCommand() []string {
	return []string{"sh", "-ec", validationPrefix + "sed -n '4p' > \"$gp_validation\"; " + validationSuffix}
}

func ValidateConfigCommand() []string {
	return []string{
		"sh",
		"-ec",
		validationPrefix + "sed -n '4p' " + ConfigurationTarget + " > \"$gp_validation\"; " + validationSuffix,
	}
}

// Keep configuration in data files, not argv: a legal large snapshot must not
// exceed Linux's per-argument exec limit. These files exist only in the owned
// container/helper, and the EXIT trap removes them on validation failure too.
const validationPrefix = "umask 077; gp_validation=$(mktemp /tmp/gp-envoy-validate.XXXXXX); " +
	"trap 'rm -f -- \"$gp_validation\"' EXIT; "

const validationSuffix = "test -s \"$gp_validation\"; " +
	"envoy --mode validate --disable-hot-restart --config-path \"$gp_validation\""
