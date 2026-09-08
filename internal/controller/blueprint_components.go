package controller

import (
	"reflect"
	"sort"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// BlueprintComponentCandidate is one exact active singleton and the
// Controller-projected replacement that still needs durable address
// preparation.
type BlueprintComponentCandidate struct {
	Current   core.Component
	Candidate core.Component
}

// BlueprintComponentChanges separates the effective Component graph used for
// rendering from the subset that requires an Agent-owned candidate lifecycle.
type BlueprintComponentChanges struct {
	Effective  []core.Component
	Candidates []BlueprintComponentCandidate
}

// ReconcileBlueprintComponents applies only explicitly authored singleton
// switches. Omission preserves the active Component; disabling requires an
// explicit enabled:false and never acts as an implicit host teardown.
func ReconcileBlueprintComponents(
	specs map[string]core.ComponentSpec,
	current []core.Component,
	allocate func(ids.Kind) string,
) (BlueprintComponentChanges, error) {
	if allocate == nil {
		return BlueprintComponentChanges{}, errs.New(
			errs.KindInternal,
			"Blueprint Component id allocator is required",
		)
	}
	byKind := make(map[core.ComponentKind]core.Component, len(current))
	for _, component := range current {
		if component.Owner != core.ComponentOwnerEnvironment || component.Validate() != nil ||
			(component.Kind != core.ComponentKindIngressCaddy &&
				component.Kind != core.ComponentKindEdgeCloudflare) {
			return BlueprintComponentChanges{}, errs.New(
				errs.KindInternal,
				"active Environment Component projection is invalid",
			)
		}
		if _, duplicate := byKind[component.Kind]; duplicate {
			return BlueprintComponentChanges{}, errs.New(
				errs.KindInternal,
				"active Environment Component kind is duplicated",
			)
		}
		if component.Enabled && len(component.GeneratedServices) != 1 {
			return BlueprintComponentChanges{}, errs.New(
				errs.KindInternal,
				"enabled Environment Component does not own one generated Service",
			)
		}
		if !component.Enabled && (len(component.GeneratedServices) != 0 || component.PinnedIPv4 != "") {
			return BlueprintComponentChanges{}, errs.New(
				errs.KindInternal,
				"disabled Environment Component retains active runtime identity",
			)
		}
		byKind[component.Kind] = cloneBlueprintComponent(component)
	}
	if len(byKind) != 2 {
		return BlueprintComponentChanges{}, errs.New(
			errs.KindInternal,
			"Environment must have Caddy and Cloudflare Component singletons",
		)
	}

	authored := make(map[core.ComponentKind]core.ComponentSpec, len(specs))
	for capability, spec := range specs {
		kind, err := validateBlueprintComponentSpec(core.ComponentCapability(capability), spec)
		if err != nil {
			return BlueprintComponentChanges{}, err
		}
		if _, duplicate := authored[kind]; duplicate {
			return BlueprintComponentChanges{}, errs.New(
				errs.KindValidationFailed,
				"Blueprint Component kind is duplicated",
			)
		}
		authored[kind] = spec
	}

	kinds := []core.ComponentKind{
		core.ComponentKindIngressCaddy,
		core.ComponentKindEdgeCloudflare,
	}
	result := BlueprintComponentChanges{Effective: make([]core.Component, 0, len(kinds))}
	for _, kind := range kinds {
		active := byKind[kind]
		spec, present := authored[kind]
		config := active.Config
		if present {
			config = blueprintComponentConfig(kind, spec)
		}
		if !present || (active.Enabled == spec.Enabled && reflect.DeepEqual(active.Config, config)) {
			if active.Enabled && !active.Healthy {
				candidate := cloneBlueprintComponent(active)
				candidate.PinnedIPv4 = ""
				result.Candidates = append(result.Candidates, BlueprintComponentCandidate{
					Current: active, Candidate: candidate,
				})
				result.Effective = append(result.Effective, candidate)
				continue
			}
			result.Effective = append(result.Effective, cloneBlueprintComponent(active))
			continue
		}
		candidate := cloneBlueprintComponent(active)
		candidate.Enabled = spec.Enabled
		candidate.Config = core.CloneComponentConfig(config)
		candidate.PinnedIPv4 = ""
		candidate.Healthy = false
		if !candidate.Enabled {
			candidate.GeneratedServices = nil
		} else if !active.Enabled {
			candidate.GeneratedServices = []string{allocate(ids.KindService)}
		}
		if candidate.Validate() != nil ||
			(candidate.Enabled && len(candidate.GeneratedServices) != 1) ||
			(!candidate.Enabled && len(candidate.GeneratedServices) != 0) {
			return BlueprintComponentChanges{}, errs.New(
				errs.KindValidationFailed,
				"Blueprint Component candidate is invalid",
			)
		}
		result.Candidates = append(result.Candidates, BlueprintComponentCandidate{
			Current: active, Candidate: candidate,
		})
		result.Effective = append(result.Effective, cloneBlueprintComponent(candidate))
	}
	sort.Slice(result.Candidates, func(left int, right int) bool {
		return result.Candidates[left].Current.ID < result.Candidates[right].Current.ID
	})
	return result, nil
}

func validateBlueprintComponentSpec(
	capability core.ComponentCapability,
	spec core.ComponentSpec,
) (core.ComponentKind, error) {
	switch capability {
	case core.ComponentCapabilityHTTPRouter:
		if spec.Implementation != core.ComponentKindIngressCaddy || spec.Settings.SecretID != "" ||
			(spec.Enabled && len(spec.Settings.ZoneIDs) == 0) {
			return "", errs.New(
				errs.KindValidationFailed,
				"http-router must select Caddy and provide zone_ids while enabled",
			)
		}
		return core.ComponentKindIngressCaddy, nil
	case core.ComponentCapabilityEdgeTunnel:
		if spec.Implementation != core.ComponentKindEdgeCloudflare ||
			spec.ImplementationConfig.CaddyfileTemplate != "" ||
			(spec.Enabled && (spec.Settings.SecretID == "" || len(spec.Settings.ZoneIDs) == 0)) {
			return "", errs.New(
				errs.KindValidationFailed,
				"edge-tunnel must select Cloudflare Tunnel and provide zone_ids and secret_id while enabled",
			)
		}
		return core.ComponentKindEdgeCloudflare, nil
	default:
		return "", errs.New(
			errs.KindValidationFailed,
			"Blueprint Component capability is not supported for an Environment",
		)
	}
}

func blueprintComponentConfig(kind core.ComponentKind, spec core.ComponentSpec) core.ComponentConfig {
	switch kind {
	case core.ComponentKindIngressCaddy:
		if len(spec.Settings.ZoneIDs) == 0 && spec.ImplementationConfig.CaddyfileTemplate == "" {
			return core.ComponentConfig{}
		}
		return core.ComponentConfig{Caddy: &core.CaddyComponentConfig{
			ZoneIDs:           append([]string(nil), spec.Settings.ZoneIDs...),
			CaddyfileTemplate: spec.ImplementationConfig.CaddyfileTemplate,
		}}
	case core.ComponentKindEdgeCloudflare:
		if spec.Settings.SecretID == "" && len(spec.Settings.ZoneIDs) == 0 {
			return core.ComponentConfig{}
		}
		return core.ComponentConfig{CloudflareTunnel: &core.CloudflareTunnelComponentConfig{
			ZoneIDs:  append([]string(nil), spec.Settings.ZoneIDs...),
			SecretID: spec.Settings.SecretID,
		}}
	default:
		return core.ComponentConfig{}
	}
}

func cloneBlueprintComponent(component core.Component) core.Component {
	clone := component
	clone.Config = core.CloneComponentConfig(component.Config)
	clone.GeneratedServices = append([]string(nil), component.GeneratedServices...)
	return clone
}
