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
	for name, spec := range specs {
		if name != string(spec.Kind) ||
			(spec.Kind != core.ComponentKindIngressCaddy && spec.Kind != core.ComponentKindEdgeCloudflare) {
			return BlueprintComponentChanges{}, errs.New(
				errs.KindValidationFailed,
				"Blueprint Component key and kind must be a supported canonical singleton",
			)
		}
		if _, duplicate := authored[spec.Kind]; duplicate {
			return BlueprintComponentChanges{}, errs.New(
				errs.KindValidationFailed,
				"Blueprint Component kind is duplicated",
			)
		}
		authored[spec.Kind] = spec
	}

	kinds := []core.ComponentKind{
		core.ComponentKindIngressCaddy,
		core.ComponentKindEdgeCloudflare,
	}
	result := BlueprintComponentChanges{Effective: make([]core.Component, 0, len(kinds))}
	for _, kind := range kinds {
		active := byKind[kind]
		spec, present := authored[kind]
		if !present || (active.Enabled == spec.Enabled &&
			reflect.DeepEqual(normalizeBlueprintComponentConfig(active.Config), normalizeBlueprintComponentConfig(spec.Config))) {
			result.Effective = append(result.Effective, cloneBlueprintComponent(active))
			continue
		}
		candidate := cloneBlueprintComponent(active)
		candidate.Enabled = spec.Enabled
		candidate.Config = normalizeBlueprintComponentConfig(spec.Config)
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
	if componentEnabled(result.Effective, core.ComponentKindEdgeCloudflare) &&
		!componentEnabled(result.Effective, core.ComponentKindIngressCaddy) {
		return BlueprintComponentChanges{}, errs.New(
			errs.KindValidationFailed,
			"Cloudflare Tunnel requires the Caddy Component to be enabled",
		)
	}
	sort.Slice(result.Candidates, func(left int, right int) bool {
		return result.Candidates[left].Current.ID < result.Candidates[right].Current.ID
	})
	return result, nil
}

func componentEnabled(components []core.Component, kind core.ComponentKind) bool {
	for _, component := range components {
		if component.Kind == kind {
			return component.Enabled
		}
	}
	return false
}

func normalizeBlueprintComponentConfig(config map[string]any) map[string]any {
	if len(config) == 0 {
		return nil
	}
	clone := make(map[string]any, len(config))
	for key, value := range config {
		clone[key] = value
	}
	return clone
}

func cloneBlueprintComponent(component core.Component) core.Component {
	clone := component
	clone.Config = normalizeBlueprintComponentConfig(component.Config)
	clone.GeneratedServices = append([]string(nil), component.GeneratedServices...)
	return clone
}
