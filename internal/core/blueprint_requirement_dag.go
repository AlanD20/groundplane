package core

import (
	"sort"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type RequirementTargetKind string

const RequirementTargetBackingAttach RequirementTargetKind = "backing-attach"

type RequirementCondition string

const (
	RequirementExists                RequirementCondition = "exists"
	RequirementReady                 RequirementCondition = "ready"
	RequirementCompletedSuccessfully RequirementCondition = "completed_successfully"
)

type RequirementPhase string

const (
	RequirementPhaseStart    RequirementPhase = "start"
	RequirementPhaseDeploy   RequirementPhase = "deploy"
	RequirementPhaseRollback RequirementPhase = "rollback"
	RequirementPhaseAlways   RequirementPhase = "always"
)

func (value Requirement) Validate() error {
	if value.Target.Kind != RequirementTargetBackingAttach || value.Target.Name == "" {
		return errs.New(errs.KindValidationFailed, "Blueprint requirement target is invalid")
	}
	if err := validateRequirementCondition(value.Condition); err != nil {
		return err
	}
	return validateRequirementPhases(value.Phases)
}

func validateRequirementCondition(value RequirementCondition) error {
	switch value {
	case RequirementExists, RequirementReady, RequirementCompletedSuccessfully:
		return nil
	default:
		return errs.New(errs.KindValidationFailed, "Blueprint requirement condition is invalid")
	}
}

func validateRequirementPhases(values []RequirementPhase) error {
	if len(values) == 0 {
		return errs.New(errs.KindValidationFailed, "Blueprint requirement phases are required")
	}
	seen := make(map[RequirementPhase]struct{}, len(values))
	for _, phase := range values {
		switch phase {
		case RequirementPhaseStart, RequirementPhaseDeploy, RequirementPhaseRollback, RequirementPhaseAlways:
		default:
			return errs.New(errs.KindValidationFailed, "Blueprint requirement phase is invalid")
		}
		if _, duplicate := seen[phase]; duplicate {
			return errs.New(errs.KindValidationFailed, "Blueprint requirement phase is duplicated")
		}
		seen[phase] = struct{}{}
	}
	return nil
}

func cloneRequirement(value Requirement) Requirement {
	value.Phases = append([]RequirementPhase(nil), value.Phases...)
	return value
}

func NormalizeRequirements(values []Requirement) ([]Requirement, error) {
	if values == nil {
		return nil, nil
	}
	result := make([]Requirement, len(values))
	seen := make(map[RequirementTarget]struct{}, len(values))
	for index, value := range values {
		if err := value.Validate(); err != nil {
			return nil, err
		}
		if _, duplicate := seen[value.Target]; duplicate {
			return nil, errs.New(errs.KindValidationFailed, "Blueprint requirement is duplicated")
		}
		seen[value.Target] = struct{}{}
		result[index] = cloneRequirement(value)
	}
	return result, nil
}

type ResolvedRequirementTarget struct {
	Kind     RequirementTargetKind `json:"kind"`
	Name     string                `json:"name"`
	ID       string                `json:"id"`
	TaskID   string                `json:"task_id"`
	Revision int64                 `json:"revision"`
}

type ResolvedRequirement struct {
	Target    ResolvedRequirementTarget `json:"target"`
	Condition RequirementCondition      `json:"condition"`
	Phases    []RequirementPhase        `json:"phases"`
}

type RequirementResolutionTarget struct {
	Kind     RequirementTargetKind
	Name     string
	ID       string
	TaskID   string
	Revision int64
}

type BlueprintRequirements struct {
	Authored           []Requirement         `json:"authored_requirements,omitempty"`
	Resolved           []ResolvedRequirement `json:"resolved_requirements,omitempty"`
	ResolutionRevision int64                 `json:"requirement_resolution_revision,omitempty"`
}

func (value BlueprintRequirements) Clone() BlueprintRequirements {
	clone := BlueprintRequirements{ResolutionRevision: value.ResolutionRevision}
	if value.Authored != nil {
		clone.Authored = make([]Requirement, len(value.Authored))
		for index := range value.Authored {
			clone.Authored[index] = cloneRequirement(value.Authored[index])
		}
	}
	if value.Resolved != nil {
		clone.Resolved = make([]ResolvedRequirement, len(value.Resolved))
		for index := range value.Resolved {
			clone.Resolved[index] = value.Resolved[index]
			clone.Resolved[index].Phases = append([]RequirementPhase(nil), value.Resolved[index].Phases...)
		}
	}
	return clone
}

func (value BlueprintRequirements) Validate() error {
	authored, err := NormalizeRequirements(value.Authored)
	if err != nil {
		return err
	}
	if len(authored) == 0 {
		if len(value.Resolved) != 0 || value.ResolutionRevision != 0 {
			return errs.New(errs.KindValidationFailed, "Blueprint requirement projection is inconsistent")
		}
		return nil
	}
	if value.ResolutionRevision <= 0 || len(value.Resolved) != len(authored) {
		return errs.New(errs.KindValidationFailed, "Blueprint requirement resolution is incomplete")
	}
	byTarget := make(map[RequirementTarget]Requirement, len(authored))
	for _, requirement := range authored {
		byTarget[requirement.Target] = requirement
	}
	previousID := ""
	for _, resolved := range value.Resolved {
		target := RequirementTarget{Kind: resolved.Target.Kind, Name: resolved.Target.Name}
		want, exists := byTarget[target]
		if !exists || resolved.Condition != want.Condition ||
			!sameRequirementPhases(resolved.Phases, want.Phases) ||
			ids.Validate(ids.KindAttach, resolved.Target.ID) != nil ||
			ids.Validate(ids.KindTask, resolved.Target.TaskID) != nil ||
			resolved.Target.Revision <= 0 ||
			previousID != "" && resolved.Target.ID <= previousID {
			return errs.New(errs.KindValidationFailed, "Blueprint resolved requirement is invalid")
		}
		previousID = resolved.Target.ID
	}
	return nil
}

func sameRequirementPhases(left, right []RequirementPhase) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func ResolveBlueprintRequirements(
	authored []Requirement,
	available []RequirementResolutionTarget,
	candidateNames []string,
	readRevision int64,
) (BlueprintRequirements, error) {
	normalized, err := NormalizeRequirements(authored)
	if err != nil || len(normalized) == 0 {
		return BlueprintRequirements{Authored: normalized}, err
	}
	if readRevision <= 0 {
		return BlueprintRequirements{}, errs.New(errs.KindInternal, "Blueprint requirement fixed revision is missing")
	}
	targets := make(map[RequirementTarget]RequirementResolutionTarget, len(available))
	for _, target := range available {
		key := RequirementTarget{Kind: target.Kind, Name: target.Name}
		if target.Kind != RequirementTargetBackingAttach || target.Name == "" ||
			ids.Validate(ids.KindAttach, target.ID) != nil ||
			ids.Validate(ids.KindTask, target.TaskID) != nil || target.Revision <= 0 {
			return BlueprintRequirements{}, errs.New(
				errs.KindInternal,
				"Blueprint requirement target projection is invalid",
			)
		}
		if _, duplicate := targets[key]; duplicate {
			return BlueprintRequirements{}, errs.New(
				errs.KindInternal,
				"Blueprint requirement target projection is duplicated",
			)
		}
		targets[key] = target
	}
	candidates := make(map[string]struct{}, len(candidateNames))
	for _, name := range candidateNames {
		candidates[name] = struct{}{}
	}
	resolved := make([]ResolvedRequirement, 0, len(normalized))
	for _, requirement := range normalized {
		target, exists := targets[requirement.Target]
		if !exists {
			if _, self := candidates[requirement.Target.Name]; self {
				return BlueprintRequirements{}, errs.New(
					errs.KindValidationFailed,
					"Blueprint requirement depends on an Attach created by the same Task",
				)
			}
			return BlueprintRequirements{}, errs.New(
				errs.KindValidationFailed,
				"Blueprint requirement target does not exist",
			)
		}
		resolved = append(resolved, ResolvedRequirement{
			Target:    ResolvedRequirementTarget(target),
			Condition: requirement.Condition,
			Phases:    append([]RequirementPhase(nil), requirement.Phases...),
		})
	}
	sort.Slice(resolved, func(left, right int) bool {
		return resolved[left].Target.ID < resolved[right].Target.ID
	})
	result := BlueprintRequirements{
		Authored: normalized, Resolved: resolved, ResolutionRevision: readRevision,
	}
	return result, result.Validate()
}

type BlueprintRequirementTaskEdge struct {
	PrerequisiteTaskID string `json:"prerequisite_task_id"`
	DependentTaskID    string `json:"dependent_task_id"`
}

func BuildBlueprintRequirementTaskGraph(
	rootTaskID string,
	requirements BlueprintRequirements,
	inherited []BlueprintRequirementTaskEdge,
) ([]BlueprintRequirementTaskEdge, []string, error) {
	if ids.Validate(ids.KindTask, rootTaskID) != nil {
		return nil, nil, errs.New(errs.KindValidationFailed, "Blueprint requirement root Task id is invalid")
	}
	if err := requirements.Validate(); err != nil {
		return nil, nil, err
	}
	edges := append([]BlueprintRequirementTaskEdge(nil), inherited...)
	for _, requirement := range requirements.Resolved {
		edges = append(edges, BlueprintRequirementTaskEdge{
			PrerequisiteTaskID: requirement.Target.TaskID,
			DependentTaskID:    rootTaskID,
		})
	}
	sort.Slice(edges, func(left, right int) bool {
		if edges[left].PrerequisiteTaskID != edges[right].PrerequisiteTaskID {
			return edges[left].PrerequisiteTaskID < edges[right].PrerequisiteTaskID
		}
		return edges[left].DependentTaskID < edges[right].DependentTaskID
	})
	compacted := edges[:0]
	for _, edge := range edges {
		if ids.Validate(ids.KindTask, edge.PrerequisiteTaskID) != nil ||
			ids.Validate(ids.KindTask, edge.DependentTaskID) != nil {
			return nil, nil, errs.New(errs.KindValidationFailed, "Blueprint requirement Task edge is invalid")
		}
		if len(compacted) != 0 && compacted[len(compacted)-1] == edge {
			continue
		}
		compacted = append(compacted, edge)
	}
	edges = compacted
	nodes := map[string]struct{}{rootTaskID: {}}
	indegree := map[string]int{rootTaskID: 0}
	adjacent := make(map[string][]string)
	for _, edge := range edges {
		nodes[edge.PrerequisiteTaskID] = struct{}{}
		nodes[edge.DependentTaskID] = struct{}{}
		if _, exists := indegree[edge.PrerequisiteTaskID]; !exists {
			indegree[edge.PrerequisiteTaskID] = 0
		}
		indegree[edge.DependentTaskID]++
		adjacent[edge.PrerequisiteTaskID] = append(adjacent[edge.PrerequisiteTaskID], edge.DependentTaskID)
	}
	ready := make([]string, 0, len(nodes))
	for node := range nodes {
		if indegree[node] == 0 {
			ready = append(ready, node)
		}
	}
	sort.Strings(ready)
	ordered := make([]string, 0, len(nodes))
	for len(ready) != 0 {
		node := ready[0]
		ready = ready[1:]
		ordered = append(ordered, node)
		for _, dependent := range adjacent[node] {
			indegree[dependent]--
			if indegree[dependent] == 0 {
				index := sort.SearchStrings(ready, dependent)
				ready = append(ready, "")
				copy(ready[index+1:], ready[index:])
				ready[index] = dependent
			}
		}
	}
	if len(ordered) != len(nodes) {
		return nil, nil, errs.New(errs.KindValidationFailed, "Blueprint requirement dependency cycle")
	}
	return append([]BlueprintRequirementTaskEdge(nil), edges...), ordered, nil
}
