// Package cloudflaretunnel is the "cloudflare-tunnel" component — the
// outbound-only edge gateway. See mvp.md, "Router (ingress components)":
// the token registers the tunnel but cannot modify DNS; the operator is
// guided through the manual DNS process.
package cloudflaretunnel

import (
	"slices"

	"github.com/AlanD20/groundplane/internal/components"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	cloudflaredImage       = "cloudflare/cloudflared:2026.7.2"
	cloudflaredServiceName = "cloudflare-tunnel"
	operatorTokenKey       = "CLOUDFLARE_TUNNEL_TOKEN"
	cloudflaredTokenKey    = "TUNNEL_TOKEN"
)

// Register adds this component to the registry. Called once, explicitly,
// from internal/app.NewController.
func Register() {
	implementation := &component{}
	components.Register(components.Registration{
		Kind:          core.ComponentKindEdgeCloudflare,
		Label:         "Cloudflare Tunnel (outbound edge)",
		AllowedOwners: []core.ComponentOwner{core.ComponentOwnerEnvironment},
		ApplyStrategy: components.EnvironmentRender,
		ConfigSchema:  []string{"token_entry_id"},
		Environment:   implementation,
	})
}

type component struct{}

func (a *component) Render(
	env core.Environment,
	component core.Component,
) (map[string]components.GeneratedService, map[string][]byte, error) {
	if err := validateIdentity(env, component); err != nil {
		return nil, nil, err
	}
	if !component.Enabled {
		return map[string]components.GeneratedService{}, map[string][]byte{}, nil
	}
	zone, err := caddyZone(env)
	if err != nil {
		return nil, nil, err
	}
	entry, err := tokenEntry(env, component)
	if err != nil {
		return nil, nil, err
	}
	if len(component.GeneratedServices) != 1 || component.GeneratedServices[0] == "" {
		return nil, nil, errs.New(
			errs.KindValidationFailed,
			"cloudflaretunnel: one stable generated Service id is required",
		)
	}
	service := core.Service{
		ID: component.GeneratedServices[0], Name: cloudflaredServiceName, Image: cloudflaredImage,
		Zones: []string{zone.Name}, Command: []string{"tunnel", "--no-autoupdate", "run"},
		Aliases: map[string][]string{zone.Name: {cloudflaredServiceName}},
		DependsOn: map[string]core.ServiceDependency{
			"caddy": {Condition: "service_started"},
		},
		Restart: "unless-stopped", Replicas: 1,
	}
	return map[string]components.GeneratedService{
		cloudflaredServiceName: {
			Service: service,
			SecretEnvironment: []components.GeneratedSecretEnvironment{{
				Name: cloudflaredTokenKey, EntryID: entry.ID,
			}},
		},
	}, map[string][]byte{}, nil
}

func (a *component) Healthy(env core.Environment, component core.Component) (bool, error) {
	if !component.Enabled {
		return false, nil
	}
	if _, _, err := a.Render(env, component); err != nil {
		return false, err
	}
	return component.Healthy, nil
}

func validateIdentity(env core.Environment, component core.Component) error {
	if env.ID == "" || component.ID == "" || component.Owner != core.ComponentOwnerEnvironment ||
		component.OwnerID != env.ID || component.Kind != core.ComponentKindEdgeCloudflare {
		return errs.New(errs.KindValidationFailed, "cloudflaretunnel: component ownership or kind is invalid")
	}
	return nil
}

func caddyZone(env core.Environment) (core.Zone, error) {
	for _, component := range env.Components {
		if component.Kind != core.ComponentKindIngressCaddy {
			continue
		}
		if !component.Enabled {
			return core.Zone{}, errs.New(errs.KindValidationFailed, "cloudflaretunnel: Caddy must be enabled")
		}
		zoneID, ok := component.Config["zone_id"].(string)
		if !ok || zoneID == "" {
			return core.Zone{}, errs.New(errs.KindValidationFailed, "cloudflaretunnel: Caddy Zone is invalid")
		}
		for _, zone := range env.Zones {
			if zone.ID == zoneID {
				return zone, nil
			}
		}
		return core.Zone{}, errs.New(errs.KindValidationFailed, "cloudflaretunnel: Caddy Zone is missing")
	}
	return core.Zone{}, errs.New(errs.KindValidationFailed, "cloudflaretunnel: enabled Caddy Component is required")
}

func tokenEntry(env core.Environment, component core.Component) (core.EnvEntry, error) {
	for key := range component.Config {
		if key != "token_entry_id" {
			return core.EnvEntry{}, errs.Newf(
				errs.KindValidationFailed,
				"cloudflaretunnel: unknown config field %q",
				key,
			)
		}
	}
	entryID, ok := component.Config["token_entry_id"].(string)
	if !ok || entryID == "" {
		return core.EnvEntry{}, errs.New(
			errs.KindValidationFailed,
			"cloudflaretunnel: config token_entry_id is required",
		)
	}
	for _, entry := range env.Entries {
		if entry.ID != entryID {
			continue
		}
		if err := entry.Validate(); err != nil || entry.Kind != core.EntryKindEnv ||
			entry.Key != operatorTokenKey || !entry.Secret ||
			!slices.Equal(entry.Exposure, []string{cloudflaredServiceName}) {
			return core.EnvEntry{}, errs.New(
				errs.KindValidationFailed,
				"cloudflaretunnel: token Entry must be the exact service-scoped CLOUDFLARE_TUNNEL_TOKEN secret",
			)
		}
		return entry, nil
	}
	return core.EnvEntry{}, errs.New(errs.KindValidationFailed, "cloudflaretunnel: token Entry is missing")
}
