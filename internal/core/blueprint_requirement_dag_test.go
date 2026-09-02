package core

import (
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

func TestNormalizeRequirementsRejectsOpenGrammar(t *testing.T) {
	// Rationale: schema 1 rejects old, unsupported, incomplete, and duplicate spellings.
	t.Parallel()
	valid := Requirement{
		Target:    RequirementTarget{Kind: RequirementTargetBackingAttach, Name: "api-db"},
		Condition: RequirementReady, Phases: []RequirementPhase{RequirementPhaseDeploy},
	}
	cases := map[string][]Requirement{
		"healthy":         {{Target: valid.Target, Condition: "healthy", Phases: valid.Phases}},
		"unknown":         {{Target: valid.Target, Condition: "unknown", Phases: valid.Phases}},
		"kind":            {{Target: RequirementTarget{Kind: "service", Name: "api"}, Condition: valid.Condition, Phases: valid.Phases}},
		"name":            {{Target: RequirementTarget{Kind: RequirementTargetBackingAttach}, Condition: valid.Condition, Phases: valid.Phases}},
		"phases":          {{Target: valid.Target, Condition: valid.Condition}},
		"duplicate phase": {{Target: valid.Target, Condition: valid.Condition, Phases: []RequirementPhase{RequirementPhaseDeploy, RequirementPhaseDeploy}}},
		"duplicate":       {valid, {Target: valid.Target, Condition: RequirementExists, Phases: []RequirementPhase{RequirementPhaseStart}}},
	}
	for name, values := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := NormalizeRequirements(values); err == nil {
				t.Fatal("NormalizeRequirements() error = nil")
			}
		})
	}
}

func TestResolveAndPlanBlueprintRequirements(t *testing.T) {
	// Rationale: labels round-trip while retry planning uses stable target and step ids only.
	t.Parallel()
	at := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	target := ids.NewAt(ids.KindAttach, at, 1)
	firstStep := ids.NewAt(ids.KindStep, at, 2)
	secondStep := ids.NewAt(ids.KindStep, at, 3)
	producerTask := ids.NewAt(ids.KindTask, at, 4)
	rootTask := ids.NewAt(ids.KindTask, at, 5)
	authored := []Requirement{{
		Target:    RequirementTarget{Kind: RequirementTargetBackingAttach, Name: "api-db"},
		Condition: RequirementCompletedSuccessfully, Phases: []RequirementPhase{RequirementPhaseDeploy},
	}}
	resolved, err := ResolveBlueprintRequirements(authored, []RequirementResolutionTarget{{
		Kind: RequirementTargetBackingAttach, Name: "api-db", ID: target,
		TaskID: producerTask, Revision: 39,
	}}, nil, 41)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := BuildBlueprintRequirementPhasePlan(resolved, []string{secondStep, firstStep}, RequirementPhaseDeploy)
	if err != nil {
		t.Fatal(err)
	}
	wantSteps := []string{firstStep, secondStep}
	sort.Strings(wantSteps)
	wantOrder := append([]string{target}, wantSteps...)
	if !reflect.DeepEqual(resolved.Authored, authored) || resolved.Resolved[0].Target.ID != target ||
		resolved.Resolved[0].Target.TaskID != producerTask || resolved.Resolved[0].Target.Revision != 39 ||
		!reflect.DeepEqual(plan.OrderedNodeIDs, wantOrder) || len(plan.Edges) != 2 {
		t.Fatalf("resolved/plan = %#v / %#v", resolved, plan)
	}
	dag, err := BuildBlueprintRequirementDAG(rootTask, resolved, []string{secondStep, firstStep}, nil)
	if err != nil || dag.Validate() != nil {
		t.Fatalf("BuildBlueprintRequirementDAG() = %#v, %v", dag, err)
	}
	tampered := dag.Clone()
	tampered.PhasePlans[1].Edges = nil
	if err := tampered.Validate(); err == nil {
		t.Fatal("tampered sealed phase plan accepted")
	}
}

func TestResolveBlueprintRequirementsRejectsMissingAndSelf(t *testing.T) {
	// Rationale: resolution fails before one Task can wait on its own candidate Attach.
	t.Parallel()
	requirement := []Requirement{{
		Target:    RequirementTarget{Kind: RequirementTargetBackingAttach, Name: "api-db"},
		Condition: RequirementReady, Phases: []RequirementPhase{RequirementPhaseDeploy},
	}}
	if _, err := ResolveBlueprintRequirements(requirement, nil, nil, 9); err == nil {
		t.Fatal("missing target accepted")
	}
	if _, err := ResolveBlueprintRequirements(requirement, nil, []string{"api-db"}, 9); err == nil {
		t.Fatal("self dependency accepted")
	}
}
