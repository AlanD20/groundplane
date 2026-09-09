package executionplan

import (
	"bytes"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: Volume identity Tasks may carry untouched historical Services only
// when every executable step is confined to the single selected Volume.
func TestVolumeResourceOnlyPlanRetainsOwnershipWithoutServiceExecution(t *testing.T) {
	for _, verify := range []bool{false, true} {
		plan := volumeResourceOnlyPlan(verify)
		if _, err := Seal(plan); err != nil {
			t.Fatalf("resource-only verify=%t: %v", verify, err)
		}
		for _, change := range []string{"apply", "extra-step", "wrong-volume", "wrong-artifact", "missing-directory", "operation", "future-label"} {
			t.Run(change, func(t *testing.T) {
				changed := proto.CloneOf(plan)
				last := changed.Steps[len(changed.Steps)-1]
				switch change {
				case "apply":
					last.Payload = validPlan().Steps[0].Payload
				case "extra-step":
					changed.Steps = append(changed.Steps, validPlan().Steps[0])
				case "wrong-volume":
					last.GetManagedVolumeEnsure().VolumeId = "vol_01ARZ3NDEKTSV4RRFFQ69G5FAW"
				case "wrong-artifact":
					last.GetManagedVolumeEnsure().ArtifactId = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAW"
				case "missing-directory":
					changed.Steps = []*agentpb.ExecutionStep{last}
					last.GetManagedVolumeEnsure().RequireExisting = false
				case "operation":
					changed.Operation = agentpb.PlanOperation_PLAN_OPERATION_START
				case "future-label":
					for _, label := range changed.Artifacts[0].Services[0].ExpectedLabels {
						if label.Key == labelRenderGen {
							label.Value = "9"
						}
					}
				}
				if _, err := Seal(changed); err == nil {
					t.Fatal("resource-only ownership authorized wider execution")
				}
			})
		}
	}
}

func volumeResourceOnlyPlan(verify bool) *agentpb.ExecutionPlan {
	plan := validPlan()
	const environmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	const volumeID = "vol_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	plan.Operation, plan.TargetId, plan.RenderGeneration = agentpb.PlanOperation_PLAN_OPERATION_RECONCILE, volumeID, 8
	artifact := plan.Artifacts[0]
	artifact.OwnerKind, artifact.OwnerId = agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT, environmentID
	artifact.ProjectName, artifact.AuthorizedVolumeDir = "gp-"+strings.ToLower(
		environmentID,
	), "/var/lib/groundplane/vol/"+environmentID
	artifact.Services[0].ExpectedLabels = append(
		[]*agentpb.LabelPair{{Key: labelEnvironmentID, Value: environmentID}},
		artifact.Services[0].ExpectedLabels...)
	artifact.Volumes = []*agentpb.ComposeVolume{
		{VolumeId: volumeID, ComposeName: "data", DockerName: "gp_vol_" + strings.ToLower(volumeID),
			ExpectedLabels: []*agentpb.LabelPair{
				{Key: labelEnvironmentID, Value: environmentID},
				{Key: labelKind, Value: "volume"},
				{Key: labelManaged, Value: "true"},
			}},
	}
	plan.Steps = nil
	if !verify {
		plan.Steps = append(
			plan.Steps,
			&agentpb.ExecutionStep{StepId: "step_01ARZ3NDEKTSV4RRFFQ69G5FAW", TimeoutSeconds: 30,
				Payload: &agentpb.ExecutionStep_ManagedVolumeDirectoriesEnsure{
					ManagedVolumeDirectoriesEnsure: &agentpb.ManagedVolumeDirectoriesEnsure{
						ArtifactId: artifact.ArtifactId, VolumeIds: []string{volumeID}, IntentSha256: bytes.Repeat([]byte{1}, 32),
					},
				}},
		)
	}
	plan.Steps = append(plan.Steps, &agentpb.ExecutionStep{StepId: testStepID, TimeoutSeconds: 30,
		Payload: &agentpb.ExecutionStep_ManagedVolumeEnsure{ManagedVolumeEnsure: &agentpb.ManagedVolumeEnsure{
			ArtifactId: artifact.ArtifactId, VolumeId: volumeID, RequireExisting: verify,
		}}})
	return plan
}
