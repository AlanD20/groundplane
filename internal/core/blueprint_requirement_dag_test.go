package core

import (
	"reflect"
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
		"healthy": {{Target: valid.Target, Condition: "healthy", Phases: valid.Phases}},
		"unknown": {{Target: valid.Target, Condition: "unknown", Phases: valid.Phases}},
		"kind": {
			{Target: RequirementTarget{Kind: "service", Name: "api"}, Condition: valid.Condition, Phases: valid.Phases},
		},
		"name": {
			{
				Target:    RequirementTarget{Kind: RequirementTargetBackingAttach},
				Condition: valid.Condition,
				Phases:    valid.Phases,
			},
		},
		"phases": {{Target: valid.Target, Condition: valid.Condition}},
		"duplicate phase": {
			{
				Target:    valid.Target,
				Condition: valid.Condition,
				Phases:    []RequirementPhase{RequirementPhaseDeploy, RequirementPhaseDeploy},
			},
		},
		"duplicate": {
			valid,
			{Target: valid.Target, Condition: RequirementExists, Phases: []RequirementPhase{RequirementPhaseStart}},
		},
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

func TestResolveBlueprintRequirementsRetainsStableTarget(t *testing.T) {
	// Rationale: later work uses the captured resource and producer, not a label
	// that might now resolve to another Attach.
	t.Parallel()
	at := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	target := ids.NewAt(ids.KindAttach, at, 1)
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
	edges, order, err := BuildBlueprintRequirementTaskGraph(rootTask, resolved, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(resolved.Authored, authored) || resolved.Resolved[0].Target.ID != target ||
		resolved.Resolved[0].Target.TaskID != producerTask || resolved.Resolved[0].Target.Revision != 39 ||
		!reflect.DeepEqual(order, []string{producerTask, rootTask}) || len(edges) != 1 ||
		edges[0].PrerequisiteTaskID != producerTask || edges[0].DependentTaskID != rootTask {
		t.Fatalf("captured prerequisite identity/order changed: %#v / %#v", resolved, order)
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
