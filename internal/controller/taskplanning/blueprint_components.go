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
	byCapability, err := blueprintComponentMap(current)
	if err != nil {
		return BlueprintComponentChanges{}, err
	}

	authored, err := blueprintAuthoredComponents(specs)
	if err != nil {
		return BlueprintComponentChanges{}, err
	}
	for capability := range authored {
		if _, found := byCapability[capability]; !found {
			return BlueprintComponentChanges{}, errs.New(
				errs.KindStateConflict,
				"Blueprint Component capability has no Environment singleton",
			)
		}
	}

	capabilities := make([]core.ComponentCapability, 0, len(byCapability))
	for capability := range byCapability {
		capabilities = append(capabilities, capability)
	}
	sort.Slice(capabilities, func(left, right int) bool { return capabilities[left] < capabilities[right] })
	result := BlueprintComponentChanges{Effective: make([]core.Component, 0, len(capabilities))}
	for _, capability := range capabilities {
		active := byCapability[capability]
		spec, present := authored[capability]
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
		current.Owner != core.ComponentOwnerEnvironment {
		return BlueprintComponentChanges{}, errs.New(
			errs.KindValidationFailed,
			"Blueprint Component singleton input is invalid",
		)
	}
	kind, err := validateBlueprintComponentSpec(capability, spec)
	if err != nil {
		return BlueprintComponentChanges{}, err
	}
	if !current.Kind.SharesEnvironmentCapability(kind) {
		return BlueprintComponentChanges{}, errs.New(
			errs.KindStateConflict,
			"Blueprint Component cannot change its Environment capability",
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
	kind := active.Kind
	if present {
		config = core.ComponentConfigForSpec(spec)
		kind = spec.Implementation
	}
	if active.Kind == kind && active.Enabled == spec.Enabled && core.EqualComponentConfig(active.Config, config) {
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
	candidate.Kind = kind
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
	byCapability, err := blueprintComponentMap(current)
	if err != nil {
		return nil, err
	}
	authored, err := blueprintAuthoredComponents(specs)
	if err != nil {
		return nil, err
	}
	selected := make([]core.Component, 0, len(authored))
	for capability := range authored {
		component, found := byCapability[capability]
		if !found {
			return nil, errs.New(
				errs.KindStateConflict,
				"Blueprint Component capability has no Environment singleton",
			)
		}
		selected = append(selected, cloneBlueprintComponent(component))
	}
	sort.Slice(selected, func(left, right int) bool { return selected[left].Kind < selected[right].Kind })
	return selected, nil
}

func blueprintAuthoredComponents(
	specs map[string]core.ComponentSpec,
) (map[core.ComponentCapability]core.ComponentSpec, error) {
	authored := make(map[core.ComponentCapability]core.ComponentSpec, len(specs))
	for capability, spec := range specs {
		_, err := validateBlueprintComponentSpec(core.ComponentCapability(capability), spec)
		if err != nil {
			return nil, err
		}
		if _, duplicate := authored[core.ComponentCapability(capability)]; duplicate {
			return nil, errs.New(errs.KindValidationFailed, "Blueprint Component capability is duplicated")
		}
		authored[core.ComponentCapability(capability)] = spec
	}
	return authored, nil
}

func blueprintComponentMap(current []core.Component) (map[core.ComponentCapability]core.Component, error) {
	byCapability := make(map[core.ComponentCapability]core.Component, len(current))
	for _, component := range current {
		if component.Owner != core.ComponentOwnerEnvironment || component.Validate() != nil ||
			(!component.Kind.IsEnvironmentProxy() &&
				component.Kind != core.ComponentKindEdgeCloudflare) {
			return nil, errs.New(
				errs.KindInternal,
				"active Environment Component projection is invalid",
			)
		}
		capability, _ := component.Kind.EnvironmentCapability()
		if _, duplicate := byCapability[capability]; duplicate {
			return nil, errs.New(
				errs.KindInternal,
				"active Environment Component capability is duplicated",
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
		byCapability[capability] = cloneBlueprintComponent(component)
	}
	_, router := byCapability[core.ComponentCapabilityHTTPRouter]
	_, tunnel := byCapability[core.ComponentCapabilityEdgeTunnel]
	if len(byCapability) != 2 || !tunnel || !router {
		return nil, errs.New(
			errs.KindInternal,
			"Environment must have one proxy singleton and one Cloudflare Tunnel singleton",
		)
	}
	return byCapability, nil
}

func validateBlueprintComponentSpec(
	capability core.ComponentCapability,
	spec core.ComponentSpec,
) (core.ComponentKind, error) {
	if err := spec.Validate(capability); err != nil {
		return "", errs.New(errs.KindValidationFailed, err.Error())
	}
	return spec.Implementation, nil
}

func cloneBlueprintComponent(component core.Component) core.Component {
	clone := component
	clone.Config = core.CloneComponentConfig(component.Config)
	clone.GeneratedServices = append([]string(nil), component.GeneratedServices...)
	return clone
}
