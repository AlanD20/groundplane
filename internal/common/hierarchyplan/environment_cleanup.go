package hierarchyplan

import (
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func EnvironmentCleanup(
	planID string,
	renderGeneration uint64,
	environmentID string,
	composeStepID string,
	directoryStepID string,
	timeoutSeconds uint32,
	volumeDirectory string,
	composeArtifact []byte,
) (*agentpb.ExecutionPlan, error) {
	steps := []*agentpb.ExecutionStep{{
		StepId: directoryStepID, TimeoutSeconds: timeoutSeconds,
		Payload: &agentpb.ExecutionStep_EnvironmentDirectoryRemove{
			EnvironmentDirectoryRemove: &agentpb.EnvironmentDirectoryRemove{
				EnvironmentId: environmentID, ExpectedVolumeDir: volumeDirectory,
			},
		},
	}}
	var artifacts []*agentpb.ComposeArtifact
	if len(composeArtifact) != 0 {
		artifact := &agentpb.ComposeArtifact{}
		if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(composeArtifact, artifact); err != nil {
			return nil, errs.New(errs.KindInternal, "Environment cleanup Compose artifact is corrupt")
		}
		artifacts = []*agentpb.ComposeArtifact{artifact}
		steps = append([]*agentpb.ExecutionStep{{
			StepId: composeStepID, TimeoutSeconds: timeoutSeconds,
			Payload: &agentpb.ExecutionStep_ComposeRemove{
				ComposeRemove: &agentpb.ComposeRemove{ArtifactId: artifact.GetArtifactId(), WholeProject: true},
			},
		}}, steps...)
	} else if composeStepID != "" {
		return nil, errs.New(errs.KindInternal, "artifact-free Environment cleanup has a Compose step")
	}
	return executionplan.Seal(&agentpb.ExecutionPlan{
		Schema: 1, PlanId: planID, RenderGeneration: renderGeneration,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_REMOVE, TargetId: environmentID,
		Artifacts: artifacts, Steps: steps,
	})
}
