package controller

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func TestBuildPlanSealsDeterministicOwnedCopy(t *testing.T) {
	input := validPlanBuildInput()
	first, err := BuildPlan(input)
	if err != nil {
		t.Fatalf("BuildPlan() error = %v", err)
	}
	second, err := BuildPlan(input)
	if err != nil {
		t.Fatalf("BuildPlan(replay) error = %v", err)
	}
	if first.GetSchema() != 1 || len(first.GetPlanHash()) != sha256.Size ||
		!bytes.Equal(first.GetPlanHash(), second.GetPlanHash()) {
		t.Fatalf("BuildPlan() hashes = %x / %x", first.GetPlanHash(), second.GetPlanHash())
	}

	input.Artifacts[0].CanonicalYaml[0] = 'X'
	if first.GetArtifacts()[0].GetCanonicalYaml()[0] == 'X' {
		t.Fatal("BuildPlan() retained caller-owned artifact bytes")
	}
}

func TestBuildPlanRejectsIncompleteTypedInput(t *testing.T) {
	input := validPlanBuildInput()
	input.Operation = agentpb.PlanOperation_PLAN_OPERATION_UNSPECIFIED
	if _, err := BuildPlan(input); err == nil {
		t.Fatal("BuildPlan() error = nil, want invalid operation rejection")
	}
}

// Rationale: a generic reconcile builder must not gain arbitrary Script
// execution merely because its target happens to be a valid Environment.
func TestBuildPlanRejectsOrdinaryReconcileRunScript(t *testing.T) {
	input := validPlanBuildInput()
	input.Operation = agentpb.PlanOperation_PLAN_OPERATION_RECONCILE
	input.TargetID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	input.Steps[0].Payload = &agentpb.ExecutionStep_RunScript{RunScript: &agentpb.RunScript{}}

	if _, err := BuildPlan(input); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("BuildPlan(ordinary reconcile RunScript) error = %v, want validation.failed", err)
	}
}

func validPlanBuildInput() PlanBuildInput {
	at := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	yaml := []byte("name: groundplane-infra\nservices: {}\n")
	yamlHash := sha256.Sum256(yaml)
	artifactID := ids.NewAt(ids.KindConfig, at, 4)
	return PlanBuildInput{
		VolumeRoot: "/var/lib/groundplane/vol",
		PlanID:     ids.NewAt(ids.KindPlan, at, 1), RenderGeneration: 1,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_REMOVE,
		TargetID:  ids.NewAt(ids.KindComponent, at, 2),
		Artifacts: []*agentpb.ComposeArtifact{{
			ArtifactId:  artifactID,
			OwnerKind:   agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_PLATFORM,
			ProjectName: "groundplane-infra", CanonicalYaml: yaml, YamlSha256: yamlHash[:],
		}},
		Steps: []*agentpb.ExecutionStep{{
			StepId: ids.NewAt(ids.KindStep, at, 3), TimeoutSeconds: 30,
			Payload: &agentpb.ExecutionStep_ComposeRemove{ComposeRemove: &agentpb.ComposeRemove{
				ArtifactId: artifactID, WholeProject: true,
			}},
		}},
	}
}
