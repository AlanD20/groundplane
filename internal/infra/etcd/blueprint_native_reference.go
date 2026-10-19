package etcd

import (
	"slices"

	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	releases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	domain "github.com/AlanD20/groundplane/internal/core/release"
)

func blueprintNativePredecessorReferences(
	captures []BlueprintNativePredecessorCapture,
) ([]releases.BlueprintNativePredecessorReference, error) {
	result := make([]releases.BlueprintNativePredecessorReference, len(captures))
	for index, captured := range captures {
		runtime := captured.Runtime()
		reference := releases.BlueprintNativePredecessorReference{
			ServiceID: runtime.ServiceID, FixedReadRevision: runtime.FixedReadRevision,
			ProjectionRevision: runtime.ProjectionRevision, RuntimeRevision: runtime.RuntimeRevision,
			Serving: runtime.Serving,
		}
		if runtime.Serving != nil {
			digest, err := domain.Digest(releaserender.ServingRuntimeCapture{ServiceID: runtime.ServiceID,
				CurrentArtifact: runtime.CurrentArtifact, RetainedPriorArtifact: runtime.RetainedPriorArtifact})
			if err != nil {
				return nil, err
			}
			reference.PriorRuntimeSHA256 = digest
		}
		result[index] = reference
	}
	return result, nil
}

func resolveBlueprintNativePredecessors(
	references []releases.BlueprintNativePredecessorReference,
	members []releaserender.ReleaseTaskRenderMember,
) ([]BlueprintNativePredecessor, error) {
	result := make([]BlueprintNativePredecessor, len(references))
	matched := make(map[string]bool, len(references))
	for index, reference := range references {
		if err := reference.Validate(); err != nil {
			return nil, err
		}
		memberIndex := slices.IndexFunc(members, func(member releaserender.ReleaseTaskRenderMember) bool {
			return member.Intent.ServiceID == reference.ServiceID
		})
		if memberIndex < 0 {
			return nil, releases.CorruptReleaseRecord()
		}
		member := members[memberIndex]
		witness := member.Render.PriorRuntime
		if (reference.Serving == nil) != (witness == nil) || matched[reference.ServiceID] {
			return nil, releases.CorruptReleaseRecord()
		}
		matched[reference.ServiceID] = true
		runtime := BlueprintNativePredecessor{ServiceID: reference.ServiceID,
			FixedReadRevision: reference.FixedReadRevision, ProjectionRevision: reference.ProjectionRevision,
			RuntimeRevision: reference.RuntimeRevision, Serving: reference.Serving}
		if witness != nil {
			digest, err := domain.Digest(witness)
			if err != nil || digest != reference.PriorRuntimeSHA256 || witness.ServiceID != reference.ServiceID ||
				member.Intent.PriorServingReleaseID != reference.Serving.ServingReleaseID ||
				member.Render.PriorTarget != reference.Serving.Target || member.Render.PriorWorkload == nil {
				return nil, releases.CorruptReleaseRecord()
			}
			serving, err := executionplan.OpenNativeServingRuntime(member.Render.EnvironmentID,
				reference.ServiceID, witness.CurrentArtifact, witness.RetainedPriorArtifact)
			if err != nil || serving.Artifact.GetArtifactId() != member.Render.PriorArtifactID ||
				serving.ReleaseID != reference.Serving.ServingReleaseID || serving.Target != string(reference.Serving.Target) {
				return nil, releases.CorruptReleaseRecord()
			}
			var replicas uint32
			for _, workload := range serving.Workloads {
				if workload.GetImageReference() != member.Render.PriorWorkload.LocalImageID {
					return nil, releases.CorruptReleaseRecord()
				}
				replicas += workload.GetExpectedReplicas()
			}
			if replicas != member.Render.PriorWorkload.ReplicaCount {
				return nil, releases.CorruptReleaseRecord()
			}
			runtime.CurrentArtifact = witness.CurrentArtifact
			runtime.RetainedPriorArtifact = witness.RetainedPriorArtifact
		} else if reference.PriorRuntimeSHA256 != "" || member.Intent.PriorServingReleaseID != "" {
			return nil, releases.CorruptReleaseRecord()
		}
		result[index] = runtime
	}
	for _, member := range members {
		if !matched[member.Intent.ServiceID] &&
			(member.Render.PriorRuntime != nil || member.Intent.PriorServingReleaseID != "") {
			return nil, releases.CorruptReleaseRecord()
		}
	}
	return result, nil
}
