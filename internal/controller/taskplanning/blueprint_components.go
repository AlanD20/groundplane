package taskplanning

import (
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

// ReconcileBlueprintComponents treats omitted optional singletons as disabled.
// The candidate lifecycle still owns teardown and dependency checks.
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
	byKind, err := blueprintComponentMap(current)
	if err != nil {
		return BlueprintComponentChanges{}, err
	}

	authored, err := blueprintAuthoredComponents(specs)
	if err != nil {
		return BlueprintComponentChanges{}, err
	}

	kinds := []core.ComponentKind{
		core.ComponentKindIngressCaddy,
		core.ComponentKindEdgeCloudflare,
	}
	result := BlueprintComponentChanges{Effective: make([]core.Component, 0, len(kinds))}
	for _, kind := range kinds {
		active := byKind[kind]
		spec, present := authored[kind]
		change, err := reconcileBlueprintComponent(active, spec, present, allocate)
		if err != nil {
			return BlueprintComponentChanges{}, err
		}
		result.Effective = append(result.Effective, change.Effective...)
		result.Candidates = append(result.Candidates, change.Candidates...)
	}
	sort.Slice(result.Candidates, func(left int, right int) bool {
		return result.Candidates[left].Current.ID < result.Candidates[right].Current.ID
	})
	return result, nil
}

// ReconcileBlueprintComponent projects exactly one authored Component
// singleton. It never changes or emits a candidate for another Environment
// Component, so a resource unit cannot acquire all-Component effect authority.
func ReconcileBlueprintComponent(
	capability core.ComponentCapability,
	spec core.ComponentSpec,
	current core.Component,
	allocate func(ids.Kind) string,
) (BlueprintComponentChanges, error) {
	if allocate == nil || current.Validate() != nil ||
		current.Owner != core.ComponentOwnerEnvironment || current.Kind != spec.Implementation {
		return BlueprintComponentChanges{}, errs.New(
			errs.KindValidationFailed,
			"Blueprint Component singleton input is invalid",
		)
	}
	kind, err := validateBlueprintComponentSpec(capability, spec)
	if err != nil {
		return BlueprintComponentChanges{}, err
	}
	if kind != current.Kind {
		return BlueprintComponentChanges{}, errs.New(
			errs.KindStateConflict,
			"Blueprint Component singleton implementation changed",
		)
	}
	return reconcileBlueprintComponent(current, spec, true, allocate)
}

func reconcileBlueprintComponent(
	active core.Component,
	spec core.ComponentSpec,
	present bool,
	allocate func(ids.Kind) string,
) (BlueprintComponentChanges, error) {
	var config core.ComponentConfig
	if present {
		config = core.ComponentConfigForSpec(spec)
	}
	if active.Enabled == spec.Enabled && core.EqualComponentConfig(active.Config, config) {
		if active.Enabled && !active.Healthy {
			candidate := cloneBlueprintComponent(active)
			candidate.PinnedIPv4 = ""
			return BlueprintComponentChanges{
				Effective:  []core.Component{candidate},
				Candidates: []BlueprintComponentCandidate{{Current: active, Candidate: candidate}},
			}, nil
		}
		return BlueprintComponentChanges{Effective: []core.Component{cloneBlueprintComponent(active)}}, nil
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
	return BlueprintComponentChanges{
		Effective:  []core.Component{cloneBlueprintComponent(candidate)},
		Candidates: []BlueprintComponentCandidate{{Current: active, Candidate: candidate}},
	}, nil
}

// SelectBlueprintComponentSingletons binds authored capabilities to the existing
// Environment identities without preparing effects or allocating new runtime.
func SelectBlueprintComponentSingletons(
	specs map[string]core.ComponentSpec, current []core.Component,
) ([]core.Component, error) {
	byKind, err := blueprintComponentMap(current)
	if err != nil {
		return nil, err
	}
	authored, err := blueprintAuthoredComponents(specs)
	if err != nil {
		return nil, err
	}
	selected := make([]core.Component, 0, len(authored))
	for kind := range authored {
		selected = append(selected, cloneBlueprintComponent(byKind[kind]))
	}
	sort.Slice(selected, func(left, right int) bool { return selected[left].Kind < selected[right].Kind })
	return selected, nil
}

func blueprintAuthoredComponents(
	specs map[string]core.ComponentSpec,
) (map[core.ComponentKind]core.ComponentSpec, error) {
	authored := make(map[core.ComponentKind]core.ComponentSpec, len(specs))
	for capability, spec := range specs {
		kind, err := validateBlueprintComponentSpec(core.ComponentCapability(capability), spec)
		if err != nil {
			return nil, err
		}
		if _, duplicate := authored[kind]; duplicate {
			return nil, errs.New(errs.KindValidationFailed, "Blueprint Component kind is duplicated")
		}
		authored[kind] = spec
	}
	return authored, nil
}

func blueprintComponentMap(current []core.Component) (map[core.ComponentKind]core.Component, error) {
	byKind := make(map[core.ComponentKind]core.Component, len(current))
	for _, component := range current {
		if component.Owner != core.ComponentOwnerEnvironment || component.Validate() != nil ||
			(component.Kind != core.ComponentKindIngressCaddy &&
				component.Kind != core.ComponentKindEdgeCloudflare) {
			return nil, errs.New(
				errs.KindInternal,
				"active Environment Component projection is invalid",
			)
		}
		if _, duplicate := byKind[component.Kind]; duplicate {
			return nil, errs.New(
				errs.KindInternal,
				"active Environment Component kind is duplicated",
			)
		}
		if component.Enabled && len(component.GeneratedServices) != 1 {
			return nil, errs.New(
				errs.KindInternal,
				"enabled Environment Component does not own one generated Service",
			)
		}
		if !component.Enabled && (len(component.GeneratedServices) != 0 || component.PinnedIPv4 != "") {
			return nil, errs.New(
				errs.KindInternal,
				"disabled Environment Component retains active runtime identity",
			)
		}
		byKind[component.Kind] = cloneBlueprintComponent(component)
	}
	if len(byKind) != 2 {
		return nil, errs.New(
			errs.KindInternal,
			"Environment must have Caddy and Cloudflare Component singletons",
		)
	}
	return byKind, nil
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
			spec.ImplementationConfig.CaddyfileTemplate != "" || spec.Settings.Alias != "" ||
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

func cloneBlueprintComponent(component core.Component) core.Component {
	clone := component
	clone.Config = core.CloneComponentConfig(component.Config)
	clone.GeneratedServices = append([]string(nil), component.GeneratedServices...)
	return clone
}
