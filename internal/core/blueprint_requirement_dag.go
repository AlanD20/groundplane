package core

import (
	"slices"
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
			return BlueprintRequirements{}, errs.New(errs.KindInternal, "Blueprint requirement target projection is invalid")
		}
		if _, duplicate := targets[key]; duplicate {
			return BlueprintRequirements{}, errs.New(errs.KindInternal, "Blueprint requirement target projection is duplicated")
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
			return BlueprintRequirements{}, errs.New(errs.KindValidationFailed, "Blueprint requirement target does not exist")
		}
		resolved = append(resolved, ResolvedRequirement{
			Target: ResolvedRequirementTarget{
				Kind: target.Kind, Name: target.Name, ID: target.ID, TaskID: target.TaskID,
				Revision: target.Revision,
			},
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

type BlueprintRequirementEdge struct {
	TargetID  string               `json:"target_id"`
	StepID    string               `json:"step_id"`
	Condition RequirementCondition `json:"condition"`
}

type BlueprintRequirementPhasePlan struct {
	Phase          RequirementPhase           `json:"phase"`
	OrderedNodeIDs []string                   `json:"ordered_node_ids,omitempty"`
	Edges          []BlueprintRequirementEdge `json:"edges,omitempty"`
}

func BuildBlueprintRequirementPhasePlan(
	requirements BlueprintRequirements,
	stepIDs []string,
	phase RequirementPhase,
) (BlueprintRequirementPhasePlan, error) {
	if phase != RequirementPhaseStart && phase != RequirementPhaseDeploy && phase != RequirementPhaseRollback {
		return BlueprintRequirementPhasePlan{}, errs.New(errs.KindValidationFailed, "Blueprint requirement operation phase is invalid")
	}
	if err := requirements.Validate(); err != nil {
		return BlueprintRequirementPhasePlan{}, err
	}
	steps := append([]string(nil), stepIDs...)
	sort.Strings(steps)
	for index, stepID := range steps {
		if ids.Validate(ids.KindStep, stepID) != nil || index > 0 && stepID == steps[index-1] {
			return BlueprintRequirementPhasePlan{}, errs.New(errs.KindValidationFailed, "Blueprint requirement Task step ids are invalid")
		}
	}
	selected := make([]ResolvedRequirement, 0, len(requirements.Resolved))
	for _, requirement := range requirements.Resolved {
		for _, selector := range requirement.Phases {
			if selector == RequirementPhaseAlways || selector == phase {
				selected = append(selected, requirement)
				break
			}
		}
	}
	if len(selected) != 0 && len(steps) == 0 {
		return BlueprintRequirementPhasePlan{}, errs.New(errs.KindValidationFailed, "Blueprint requirement Task has no steps")
	}
	ordered := make([]string, 0, len(selected)+len(steps))
	edges := make([]BlueprintRequirementEdge, 0, len(selected)*len(steps))
	for _, requirement := range selected {
		ordered = append(ordered, requirement.Target.ID)
		for _, stepID := range steps {
			edges = append(edges, BlueprintRequirementEdge{
				TargetID: requirement.Target.ID, StepID: stepID, Condition: requirement.Condition,
			})
		}
	}
	ordered = append(ordered, steps...)
	return BlueprintRequirementPhasePlan{Phase: phase, OrderedNodeIDs: ordered, Edges: edges}, nil
}

type BlueprintRequirementTaskEdge struct {
	PrerequisiteTaskID string `json:"prerequisite_task_id"`
	DependentTaskID    string `json:"dependent_task_id"`
}

type BlueprintRequirementGateTarget struct {
	TargetID       string               `json:"target_id"`
	TargetTaskID   string               `json:"target_task_id"`
	TargetRevision int64                `json:"target_revision"`
	Condition      RequirementCondition `json:"condition"`
	Phases         []RequirementPhase   `json:"phases"`
}

type BlueprintRequirementDAG struct {
	RootTaskID     string                           `json:"root_task_id"`
	Requirements   []BlueprintRequirementGateTarget `json:"requirements"`
	TaskEdges      []BlueprintRequirementTaskEdge   `json:"task_edges"`
	OrderedTaskIDs []string                         `json:"ordered_task_ids"`
	StepIDs        []string                         `json:"step_ids"`
	PhasePlans     []BlueprintRequirementPhasePlan  `json:"phase_plans"`
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

func BuildBlueprintRequirementDAG(
	rootTaskID string,
	requirements BlueprintRequirements,
	stepIDs []string,
	inherited []BlueprintRequirementTaskEdge,
) (BlueprintRequirementDAG, error) {
	edges, orderedTasks, err := BuildBlueprintRequirementTaskGraph(rootTaskID, requirements, inherited)
	if err != nil {
		return BlueprintRequirementDAG{}, err
	}
	targets := make([]BlueprintRequirementGateTarget, len(requirements.Resolved))
	for index, requirement := range requirements.Resolved {
		targets[index] = BlueprintRequirementGateTarget{
			TargetID: requirement.Target.ID, TargetTaskID: requirement.Target.TaskID,
			TargetRevision: requirement.Target.Revision, Condition: requirement.Condition,
			Phases: append([]RequirementPhase(nil), requirement.Phases...),
		}
	}
	plans := make([]BlueprintRequirementPhasePlan, 0, 3)
	for _, phase := range []RequirementPhase{
		RequirementPhaseStart, RequirementPhaseDeploy, RequirementPhaseRollback,
	} {
		plan, planErr := BuildBlueprintRequirementPhasePlan(requirements, stepIDs, phase)
		if planErr != nil {
			return BlueprintRequirementDAG{}, planErr
		}
		if len(plan.Edges) == 0 {
			plan.Edges = nil
		}
		plans = append(plans, plan)
	}
	steps := append([]string(nil), stepIDs...)
	sort.Strings(steps)
	return BlueprintRequirementDAG{
		RootTaskID: rootTaskID, Requirements: targets, TaskEdges: edges,
		OrderedTaskIDs: orderedTasks, StepIDs: steps, PhasePlans: plans,
	}, nil
}

func (value BlueprintRequirementDAG) Clone() BlueprintRequirementDAG {
	clone := value
	clone.Requirements = append([]BlueprintRequirementGateTarget(nil), value.Requirements...)
	for index := range clone.Requirements {
		clone.Requirements[index].Phases = append([]RequirementPhase(nil), value.Requirements[index].Phases...)
	}
	clone.TaskEdges = append([]BlueprintRequirementTaskEdge(nil), value.TaskEdges...)
	clone.OrderedTaskIDs = append([]string(nil), value.OrderedTaskIDs...)
	clone.StepIDs = append([]string(nil), value.StepIDs...)
	clone.PhasePlans = append([]BlueprintRequirementPhasePlan(nil), value.PhasePlans...)
	for index := range clone.PhasePlans {
		clone.PhasePlans[index].OrderedNodeIDs = append([]string(nil), value.PhasePlans[index].OrderedNodeIDs...)
		clone.PhasePlans[index].Edges = append([]BlueprintRequirementEdge(nil), value.PhasePlans[index].Edges...)
	}
	return clone
}

func (value BlueprintRequirementDAG) Validate() error {
	if ids.Validate(ids.KindTask, value.RootTaskID) != nil || len(value.Requirements) == 0 ||
		len(value.StepIDs) == 0 || len(value.PhasePlans) != 3 {
		return errs.New(errs.KindValidationFailed, "Blueprint requirement DAG is invalid")
	}
	previousTarget := ""
	for _, requirement := range value.Requirements {
		if ids.Validate(ids.KindAttach, requirement.TargetID) != nil ||
			ids.Validate(ids.KindTask, requirement.TargetTaskID) != nil || requirement.TargetRevision <= 0 ||
			validateRequirementCondition(requirement.Condition) != nil ||
			validateRequirementPhases(requirement.Phases) != nil ||
			previousTarget != "" && requirement.TargetID <= previousTarget {
			return errs.New(errs.KindValidationFailed, "Blueprint requirement DAG target is invalid")
		}
		previousTarget = requirement.TargetID
	}
	for index, stepID := range value.StepIDs {
		if ids.Validate(ids.KindStep, stepID) != nil || index > 0 && stepID <= value.StepIDs[index-1] {
			return errs.New(errs.KindValidationFailed, "Blueprint requirement DAG step ids are invalid")
		}
	}
	empty := BlueprintRequirements{}
	edges, order, err := BuildBlueprintRequirementTaskGraph(value.RootTaskID, empty, value.TaskEdges)
	if err != nil || !slices.Equal(edges, value.TaskEdges) || !slices.Equal(order, value.OrderedTaskIDs) {
		return errs.New(errs.KindValidationFailed, "Blueprint requirement DAG Task graph is invalid")
	}
	direct := make(map[BlueprintRequirementTaskEdge]struct{}, len(value.TaskEdges))
	for _, edge := range value.TaskEdges {
		direct[edge] = struct{}{}
	}
	for _, requirement := range value.Requirements {
		if _, exists := direct[BlueprintRequirementTaskEdge{
			PrerequisiteTaskID: requirement.TargetTaskID,
			DependentTaskID:    value.RootTaskID,
		}]; !exists {
			return errs.New(errs.KindValidationFailed, "Blueprint requirement DAG direct Task edge is missing")
		}
	}
	for index, phase := range []RequirementPhase{
		RequirementPhaseStart, RequirementPhaseDeploy, RequirementPhaseRollback,
	} {
		expected := blueprintRequirementPhasePlan(value.Requirements, value.StepIDs, phase)
		if !sameBlueprintRequirementPhasePlan(value.PhasePlans[index], expected) {
			return errs.New(errs.KindValidationFailed, "Blueprint requirement DAG phase plan is inconsistent")
		}
	}
	return nil
}

func sameBlueprintRequirementPhasePlan(left, right BlueprintRequirementPhasePlan) bool {
	return left.Phase == right.Phase &&
		slices.Equal(left.OrderedNodeIDs, right.OrderedNodeIDs) &&
		slices.Equal(left.Edges, right.Edges)
}

func blueprintRequirementPhasePlan(
	requirements []BlueprintRequirementGateTarget,
	stepIDs []string,
	phase RequirementPhase,
) BlueprintRequirementPhasePlan {
	selected := make([]BlueprintRequirementGateTarget, 0, len(requirements))
	for _, requirement := range requirements {
		for _, selector := range requirement.Phases {
			if selector == RequirementPhaseAlways || selector == phase {
				selected = append(selected, requirement)
				break
			}
		}
	}
	ordered := make([]string, 0, len(selected)+len(stepIDs))
	var edges []BlueprintRequirementEdge
	for _, requirement := range selected {
		ordered = append(ordered, requirement.TargetID)
		for _, stepID := range stepIDs {
			edges = append(edges, BlueprintRequirementEdge{
				TargetID: requirement.TargetID, StepID: stepID, Condition: requirement.Condition,
			})
		}
	}
	ordered = append(ordered, stepIDs...)
	return BlueprintRequirementPhasePlan{Phase: phase, OrderedNodeIDs: ordered, Edges: edges}
}
