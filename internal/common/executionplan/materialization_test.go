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
	materializationServiceID     = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
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

// Rationale: service-specific filenames use a stable service name, but every
// sealed procedure must prove that name belongs to its stable Service id in the
// exact authenticated Compose artifact.
func TestMaterializationPlanBindsServiceIDToComposeName(t *testing.T) {
	plan := validServiceMaterializationPlan()
	if _, err := Seal(plan); err != nil {
		t.Fatalf("Seal(valid service materialization) error = %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*agentpb.MaterializeFile)
	}{
		{name: "mismatched name", mutate: func(value *agentpb.MaterializeFile) { value.ServiceName = "worker" }},
		{name: "unknown id", mutate: func(value *agentpb.MaterializeFile) {
			value.ServiceId = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAW"
		}},
		{name: "missing name", mutate: func(value *agentpb.MaterializeFile) { value.ServiceName = "" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := validServiceMaterializationPlan()
			test.mutate(candidate.Steps[0].GetMaterializeFile())
			if _, err := Seal(candidate); err == nil {
				t.Fatal("Seal() accepted an inconsistent service identity")
			}
		})
	}
}

func validServiceMaterializationPlan() *agentpb.ExecutionPlan {
	plan := validMaterializationPlan()
	plan.Artifacts[0].Services = []*agentpb.ComposeService{{
		ServiceId: materializationServiceID, ComposeName: "cloudflare-tunnel", ExpectedReplicas: 1,
		ExpectedLabels: []*agentpb.LabelPair{
			{Key: labelEnvironmentID, Value: materializationEnvironmentID},
			{Key: labelKind, Value: "service"},
			{Key: labelManaged, Value: "true"},
			{Key: labelPlanID, Value: materializationPlanID},
			{Key: labelRenderGen, Value: "1"},
			{Key: labelServiceID, Value: materializationServiceID},
		},
	}}
	materialization := plan.Steps[0].GetMaterializeFile()
	materialization.ServiceId = materializationServiceID
	materialization.ServiceName = "cloudflare-tunnel"
	materialization.Destination = "secrets/.env." + materializationEnvironmentID + ".cloudflare-tunnel"
	materialization.OutputKind = agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_GENERATED_ENV
	materialization.Mode = 0o600
	return plan
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
