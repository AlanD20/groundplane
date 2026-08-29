package executionplan

import (
	"crypto/sha256"
	"errors"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

const (
	testManagedEnvironmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testManagedVolumeID      = "vol_01ARZ3NDEKTSV4RRFFQ69G5FAV"
)

// Rationale: only an authenticated Environment artifact with at least one
// uniquely named managed volume may authorize host-directory creation.
func TestManagedVolumeDirectoriesEnsureHasClosedArtifactShape(t *testing.T) {
	if _, err := Seal(validManagedVolumeDirectoryPlan()); err != nil {
		t.Fatalf("Seal(valid) error = %v", err)
	}
	tests := map[string]func(*agentpb.ExecutionPlan){
		"unknown artifact": func(plan *agentpb.ExecutionPlan) {
			plan.Steps[0].GetManagedVolumeDirectoriesEnsure().ArtifactId =
				"cfg_01ARZ3NDEKTSV4RRFFQ69G5FAW"
		},
		"empty volume table": func(plan *agentpb.ExecutionPlan) {
			plan.Artifacts[0].Volumes = nil
		},
		"non-mutating operation": func(plan *agentpb.ExecutionPlan) {
			plan.Operation = agentpb.PlanOperation_PLAN_OPERATION_STOP
		},
		"duplicate Compose leaf": func(plan *agentpb.ExecutionPlan) {
			duplicate := proto.Clone(plan.Artifacts[0].Volumes[0]).(*agentpb.ComposeVolume)
			duplicate.VolumeId = "vol_01ARZ3NDEKTSV4RRFFQ69G5FAW"
			duplicate.DockerName = "gp_vol_" + duplicate.VolumeId
			plan.Artifacts[0].Volumes = append(plan.Artifacts[0].Volumes, duplicate)
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			plan := validManagedVolumeDirectoryPlan()
			mutate(plan)
			if _, err := Seal(plan); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
				t.Fatalf("Seal() error = %v, want validation.failed", err)
			}
		})
	}
}

func validManagedVolumeDirectoryPlan() *agentpb.ExecutionPlan {
	yaml := []byte("volumes:\n  app-data:\n    driver: local\n")
	digest := sha256.Sum256(yaml)
	return &agentpb.ExecutionPlan{
		Schema: SchemaVersion, PlanId: testPlanID, RenderGeneration: 7,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_DEPLOY, TargetId: testManagedEnvironmentID,
		Artifacts: []*agentpb.ComposeArtifact{{
			ArtifactId:    testArtifact,
			OwnerKind:     agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
			OwnerId:       testManagedEnvironmentID,
			ProjectName:   "gp-" + strings.ToLower(testManagedEnvironmentID),
			CanonicalYaml: yaml, YamlSha256: digest[:],
			AuthorizedVolumeDir: "/srv/groundplane/vol/tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV/" +
				"prj_01ARZ3NDEKTSV4RRFFQ69G5FAV/" + testManagedEnvironmentID,
			Volumes: []*agentpb.ComposeVolume{{
				VolumeId: testManagedVolumeID, ComposeName: "app-data",
				DockerName: "gp_vol_" + testManagedVolumeID,
				ExpectedLabels: []*agentpb.LabelPair{
					{Key: labelEnvironmentID, Value: testManagedEnvironmentID},
					{Key: labelKind, Value: "volume"},
					{Key: labelManaged, Value: "true"},
				},
			}},
		}},
		Steps: []*agentpb.ExecutionStep{{
			StepId: testStepID, TimeoutSeconds: 30,
			Payload: &agentpb.ExecutionStep_ManagedVolumeDirectoriesEnsure{
				ManagedVolumeDirectoriesEnsure: &agentpb.ManagedVolumeDirectoriesEnsure{
					ArtifactId: testArtifact, VolumeIds: []string{testManagedVolumeID}, IntentSha256: make([]byte, 32),
				},
			},
		}},
	}
}
