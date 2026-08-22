package executionplan

import (
	"crypto/sha256"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

const (
	materializationEnvironmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	materializationArtifactID    = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	materializationID            = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	materializationPlanID        = "plan_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	materializationStepID        = "step_01ARZ3NDEKTSV4RRFFQ69G5FAV"
)

func TestMaterializationPlanSealsMetadataWithoutPlaintext(t *testing.T) {
	// Rationale: ADR 0020 requires a reproducible sealed procedure while file
	// bytes remain exclusively in the separate transient channel transfer.
	plan := validMaterializationPlan()
	sealed, err := Seal(plan)
	if err != nil {
		t.Fatalf("Seal() error = %v", err)
	}
	materialization := sealed.Steps[0].GetMaterializeFile()
	if materialization == nil || materialization.MaterializationId != materializationID ||
		materialization.Destination != "blueprints/"+materializationPlanID+"/blueprint.yaml" ||
		materialization.Length != uint64(len("services: {}\n")) {
		t.Fatalf("sealed materialization = %#v", materialization)
	}
}

func TestMaterializationPlanRejectsOwnershipPolicyAndDuplicateDestinations(t *testing.T) {
	// Rationale: stable-looking metadata cannot authorize another Environment,
	// a writable plain file, or two competing writers for one destination.
	tests := []struct {
		name   string
		mutate func(*agentpb.ExecutionPlan)
	}{
		{name: "different environment", mutate: func(plan *agentpb.ExecutionPlan) {
			plan.Steps[0].GetMaterializeFile().EnvironmentId = "env_01ARZ3NDEKTSV4RRFFQ69G5FAX"
		}},
		{name: "writable plain file", mutate: func(plan *agentpb.ExecutionPlan) {
			plan.Steps[0].GetMaterializeFile().Mode = 0o600
		}},
		{name: "oversized content", mutate: func(plan *agentpb.ExecutionPlan) {
			plan.Steps[0].GetMaterializeFile().Length = 1<<20 + 1
		}},
		{name: "duplicate destination", mutate: func(plan *agentpb.ExecutionPlan) {
			duplicate := *plan.Steps[0].GetMaterializeFile()
			duplicate.MaterializationId = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAX"
			plan.Steps = append(plan.Steps, &agentpb.ExecutionStep{
				StepId: "step_01ARZ3NDEKTSV4RRFFQ69G5FAW", TimeoutSeconds: 30,
				Payload: &agentpb.ExecutionStep_MaterializeFile{MaterializeFile: &duplicate},
			})
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan := validMaterializationPlan()
			test.mutate(plan)
			if _, err := Seal(plan); err == nil {
				t.Fatal("Seal() error = nil, want materialization rejection")
			}
		})
	}
}

func validMaterializationPlan() *agentpb.ExecutionPlan {
	yaml := []byte("services: {}\n")
	yamlDigest := sha256.Sum256(yaml)
	contentDigest := sha256.Sum256([]byte("services: {}\n"))
	return &agentpb.ExecutionPlan{
		Schema: SchemaVersion, PlanId: materializationPlanID, RenderGeneration: 1,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_RECONCILE,
		TargetId:  materializationEnvironmentID,
		Artifacts: []*agentpb.ComposeArtifact{{
			ArtifactId:    materializationArtifactID,
			OwnerKind:     agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
			OwnerId:       materializationEnvironmentID,
			ProjectName:   "gp-" + strings.ToLower(materializationEnvironmentID),
			CanonicalYaml: yaml, YamlSha256: yamlDigest[:],
			AuthorizedVolumeDir: "/var/lib/groundplane/vol/" +
				"tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV/" +
				"prj_01ARZ3NDEKTSV4RRFFQ69G5FAV/" + materializationEnvironmentID,
		}},
		Steps: []*agentpb.ExecutionStep{{
			StepId: materializationStepID, TimeoutSeconds: 30,
			Payload: &agentpb.ExecutionStep_MaterializeFile{MaterializeFile: &agentpb.MaterializeFile{
				ArtifactId: materializationArtifactID, MaterializationId: materializationID,
				EnvironmentId: materializationEnvironmentID,
				Destination:   "blueprints/" + materializationPlanID + "/blueprint.yaml",
				OutputKind:    agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_PLAIN_FILE,
				Mode:          0o444, Length: uint64(len("services: {}\n")), Sha256: contentDigest[:],
			}},
		}},
	}
}
