package environmentchanges

import (
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type ServiceRemovalRecovery struct {
	CandidateArtifactID     string
	CandidateReleaseID      string
	ComposeProjectName      string
	PriorArtifactID         string
	PriorReleaseID          string
	PriorTarget             string
	RetainedPriorArtifactID string
	PriorArtifacts          []*agentpb.ComposeArtifact
}

// ResolveAuthoredServiceRemovalRecovery opens only the captured acknowledged
// runtime and derives the removal candidate from its real serving workload.
func ResolveAuthoredServiceRemovalRecovery(intent ServiceRemovalIntent) (ServiceRemovalRecovery, error) {
	if intent.Authored == nil || intent.AcknowledgedRuntime == nil {
		return ServiceRemovalRecovery{}, errs.New(
			errs.KindValidationFailed, "authored Service removal predecessor is invalid",
		)
	}
	serving, err := executionplan.OpenNativeServingRuntime(
		intent.EnvironmentID, intent.ServiceID,
		intent.AcknowledgedRuntime.Runtime.CurrentArtifact,
		intent.AcknowledgedRuntime.Runtime.RetainedPriorArtifact,
	)
	if err != nil {
		return ServiceRemovalRecovery{}, err
	}
	current := serving.Artifact
	result := ServiceRemovalRecovery{
		ComposeProjectName: current.GetProjectName(), PriorArtifactID: current.GetArtifactId(),
		PriorArtifacts: []*agentpb.ComposeArtifact{current},
		PriorReleaseID: serving.ReleaseID, PriorTarget: serving.Target,
		CandidateArtifactID: ids.DeriveAt(
			ids.KindConfig, intent.CreatedAt, intent.TaskID, serving.Workloads[0].GetComposeName(),
		),
	}
	result.CandidateReleaseID = result.PriorReleaseID
	if ids.Validate(ids.KindConfig, result.CandidateArtifactID) != nil ||
		ids.Validate(ids.KindDeployment, result.CandidateReleaseID) != nil {
		return ServiceRemovalRecovery{}, errs.New(
			errs.KindValidationFailed,
			"authored Service removal Release identity is invalid",
		)
	}
	if encoded := intent.AcknowledgedRuntime.Runtime.RetainedPriorArtifact; len(encoded) != 0 {
		retained := new(agentpb.ComposeArtifact)
		if (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(encoded, retained) != nil {
			return ServiceRemovalRecovery{}, errs.New(
				errs.KindValidationFailed,
				"authored Service retained runtime is corrupt",
			)
		}
		result.RetainedPriorArtifactID = retained.GetArtifactId()
		result.PriorArtifacts = append(result.PriorArtifacts, retained)
	}
	return result, nil
}
