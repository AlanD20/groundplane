package blueprintrelease

import (
	"bytes"
	"encoding/json"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/pkg/errs"
	"sort"
)

// selectCandidates owns group exclusion, material-change detection and ordering.
// Both preflight and durable preparation select from the same authored groups;
// callers never construct a parallel set of group member identities.
func selectCandidates(
	projection projectionrecord.EnvironmentComposeProjection,
	changes []etcd.EnvironmentBlueprintServiceChange,
	groups map[string]core.ReleaseGroupSpec,
	memberships NormalizedServiceMemberships,
) ([]etcd.EnvironmentBlueprintServiceChange, error) {
	if !memberships.initialized {
		return nil, errs.New(errs.KindInternal, "Blueprint normalized Service memberships are absent")
	}
	selected := make(map[string]etcd.EnvironmentBlueprintServiceChange)
	groupMembers := releaseGroupMembers(groups, changes)
	for _, change := range changes {
		service := change.Record.Desired
		candidateMembership, candidateExists := memberships.candidate[service.Name]
		if !candidateExists ||
			(candidateMembership != blueprintServiceActive && candidateMembership != blueprintServiceProfileDisabled) {
			return nil, errs.New(errs.KindInternal, "Blueprint candidate Service is absent from its sealed projection")
		}
		previousMembership, previousExists := memberships.previous[service.Name]
		if change.Current == nil && previousExists {
			return nil, errs.New(errs.KindInternal, "Blueprint new Service exists in its predecessor projection")
		}
		if change.Current != nil && (!previousExists ||
			(previousMembership != blueprintServiceActive && previousMembership != blueprintServiceProfileDisabled)) {
			return nil, errs.New(
				errs.KindInternal,
				"Blueprint existing Service is absent from its predecessor projection",
			)
		}
		if candidateMembership == blueprintServiceProfileDisabled {
			continue
		}
		if _, grouped := groupMembers[service.ID]; grouped {
			continue
		}
		if change.Record.Runtime.RuntimeIntent != core.ServiceRuntimeIntentRunning {
			continue
		}
		material := change.Current == nil
		if change.Current != nil {
			before, beforeErr := json.Marshal(change.Current.Record.Desired)
			after, afterErr := json.Marshal(change.Record.Desired)
			if beforeErr != nil || afterErr != nil {
				return nil, errs.New(errs.KindInternal, "Blueprint Service material comparison failed")
			}
			material = previousMembership != candidateMembership || !bytes.Equal(before, after) ||
				memberships.previousNative[service.Name] != memberships.candidateNative[service.Name]
		}
		if material {
			selected[service.Name] = change
		}
	}
	ordered := make([]etcd.EnvironmentBlueprintServiceChange, 0, len(selected))
	seen := make(map[string]struct{}, len(selected))
	for _, name := range projection.DeployDependencyPlan.OrderedServices {
		if change, exists := selected[name]; exists {
			ordered = append(ordered, change)
			seen[name] = struct{}{}
		}
	}
	remaining := make([]string, 0, len(selected)-len(seen))
	for name := range selected {
		if _, exists := seen[name]; !exists {
			remaining = append(remaining, name)
		}
	}
	sort.Strings(remaining)
	for _, name := range remaining {
		ordered = append(ordered, selected[name])
	}
	return ordered, nil
}

func releaseGroupMembers(
	groups map[string]core.ReleaseGroupSpec,
	changes []etcd.EnvironmentBlueprintServiceChange,
) map[string]struct{} {
	byName := make(map[string]string, len(changes))
	for _, change := range changes {
		byName[change.Record.Desired.Name] = change.Record.Desired.ID
	}
	result := make(map[string]struct{})
	for _, group := range groups {
		for _, name := range group.Services {
			if serviceID := byName[name]; serviceID != "" {
				result[serviceID] = struct{}{}
			}
		}
	}
	return result
}
