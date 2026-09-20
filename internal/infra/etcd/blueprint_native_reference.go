package etcd

import (
	"context"
	"encoding/json"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// BlueprintNativePredecessorReference binds the compact marker to the exact
// prior-runtime bytes in its candidate's immutable, manifest-owned render input.
// No reference points at a separately prunable predecessor Release input.
type BlueprintNativePredecessorReference struct {
	ServiceID          string                             `json:"service_id"`
	FixedReadRevision  int64                              `json:"fixed_read_revision"`
	ProjectionRevision int64                              `json:"projection_revision"`
	RuntimeRevision    int64                              `json:"runtime_revision,omitempty"`
	Serving            *BlueprintNativeServingPredecessor `json:"serving,omitempty"`
	PriorRuntimeSHA256 string                             `json:"prior_runtime_sha256,omitempty"`
}

func blueprintNativePredecessorReferences(
	captures []BlueprintNativePredecessorCapture,
) ([]BlueprintNativePredecessorReference, error) {
	result := make([]BlueprintNativePredecessorReference, len(captures))
	for index, captured := range captures {
		runtime := captured.Runtime()
		reference := BlueprintNativePredecessorReference{
			ServiceID: runtime.ServiceID, FixedReadRevision: runtime.FixedReadRevision,
			ProjectionRevision: runtime.ProjectionRevision, RuntimeRevision: runtime.RuntimeRevision,
			Serving: runtime.Serving,
		}
		if runtime.Serving != nil {
			digest, err := domain.Digest(ReleaseNativePredecessorAuthority{ServiceID: runtime.ServiceID,
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

func validateBlueprintNativePredecessorReferences(
	references []BlueprintNativePredecessorReference,
	procedure *agentpb.CandidateReleaseProcedure,
) error {
	if len(references) > maximumReleasePublicationMembers {
		return corruptReleaseRecord()
	}
	byService := make(map[string]BlueprintNativePredecessorReference, len(references))
	for index, reference := range references {
		if ids.Validate(ids.KindService, reference.ServiceID) != nil || reference.FixedReadRevision <= 0 ||
			reference.ProjectionRevision < 0 || reference.ProjectionRevision > reference.FixedReadRevision ||
			index > 0 && (references[index-1].ServiceID >= reference.ServiceID ||
				references[index-1].FixedReadRevision != reference.FixedReadRevision) {
			return corruptReleaseRecord()
		}
		if reference.Serving == nil {
			if reference.RuntimeRevision != 0 || reference.PriorRuntimeSHA256 != "" {
				return corruptReleaseRecord()
			}
		} else if !validLowerSHA256(reference.PriorRuntimeSHA256) || reference.ProjectionRevision <= 0 ||
			reference.RuntimeRevision <= 0 || reference.RuntimeRevision > reference.FixedReadRevision ||
			ids.Validate(ids.KindDeployment, reference.Serving.ServingReleaseID) != nil ||
			reference.Serving.Target.Validate() != nil || reference.Serving.RetainedPriorReleaseID != "" &&
			ids.Validate(ids.KindDeployment, reference.Serving.RetainedPriorReleaseID) != nil {
			return corruptReleaseRecord()
		}
		byService[reference.ServiceID] = reference
	}
	for _, member := range procedure.GetMembers() {
		reference := byService[member.GetServiceId()]
		delete(byService, member.GetServiceId())
		prior := member.GetServingPredecessor()
		if reference.Serving == nil {
			if prior.GetPriorArtifactId() != "" || prior.GetPriorReleaseId() != "" || prior.GetPriorTarget() != "" ||
				prior.GetRetainedPriorArtifactId() != "" {
				return corruptReleaseRecord()
			}
		} else if prior.GetPriorReleaseId() != reference.Serving.ServingReleaseID ||
			prior.GetPriorTarget() != string(reference.Serving.Target) || prior.GetPriorArtifactId() == "" ||
			(reference.Serving.RetainedPriorReleaseID != "") != (prior.GetRetainedPriorArtifactId() != "") {
			return corruptReleaseRecord()
		}
	}
	if len(byService) != 0 {
		return corruptReleaseRecord()
	}
	return nil
}

func resolveBlueprintNativePredecessors(
	references []BlueprintNativePredecessorReference,
	members []ReleaseTaskRenderMember,
) ([]BlueprintNativePredecessor, error) {
	result := make([]BlueprintNativePredecessor, len(references))
	matched := make(map[string]bool, len(references))
	for index, reference := range references {
		memberIndex := slices.IndexFunc(members, func(member ReleaseTaskRenderMember) bool {
			return member.Intent.ServiceID == reference.ServiceID
		})
		if memberIndex < 0 {
			return nil, corruptReleaseRecord()
		}
		member := members[memberIndex]
		witness := member.Render.PriorRuntime
		if (reference.Serving == nil) != (witness == nil) || matched[reference.ServiceID] {
			return nil, corruptReleaseRecord()
		}
		matched[reference.ServiceID] = true
		runtime := BlueprintNativePredecessor{ServiceID: reference.ServiceID,
			FixedReadRevision: reference.FixedReadRevision, ProjectionRevision: reference.ProjectionRevision,
			RuntimeRevision: reference.RuntimeRevision, Serving: reference.Serving}
		if witness != nil {
			digest, err := domain.Digest(witness)
			if err != nil || digest != reference.PriorRuntimeSHA256 || witness.ServiceID != reference.ServiceID ||
				member.Intent.PriorServingReleaseID != reference.Serving.ServingReleaseID ||
				member.Render.PriorTarget != reference.Serving.Target ||
				!recoveryRenderMatchesPredecessor(member.Render, member.Intent, &ReleaseRestorationAuthority{
					NativePredecessors: []ReleaseNativePredecessorAuthority{*witness},
				}) {
				return nil, corruptReleaseRecord()
			}
			runtime.CurrentArtifact = witness.CurrentArtifact
			runtime.RetainedPriorArtifact = witness.RetainedPriorArtifact
		} else if reference.PriorRuntimeSHA256 != "" || member.Intent.PriorServingReleaseID != "" {
			return nil, corruptReleaseRecord()
		}
		result[index] = runtime
	}
	for _, member := range members {
		if !matched[member.Intent.ServiceID] &&
			(member.Render.PriorRuntime != nil || member.Intent.PriorServingReleaseID != "") {
			return nil, corruptReleaseRecord()
		}
	}
	return result, nil
}

func (repository *TaskRepository) blueprintNativePredecessorsAtRevision(
	ctx context.Context,
	task TaskRecord,
	marker ReleasePublicationMarker,
	manifest ReleaseStagedManifest,
	revision int64,
) ([]BlueprintNativePredecessor, []etcdstore.Condition, error) {
	if len(marker.NativePredecessors) == 0 {
		return nil, nil, nil
	}
	keys := make([]string, 0, len(manifest.Members)*2)
	for _, member := range manifest.Members {
		keys = append(keys, releaseIntentStagingKey(manifest.PublicationID, member.ReleaseID),
			releaseRenderInputStagingKey(manifest.PublicationID, member.ReleaseID))
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
		return nil, nil, corruptReleaseRecord()
	}
	defer clearKeyValues(read.Values)
	members := make([]ReleaseTaskRenderMember, len(manifest.Members))
	conditions := make([]etcdstore.Condition, len(keys))
	for index, reference := range manifest.Members {
		intentValue, renderValue := read.Values[index*2], read.Values[index*2+1]
		if intentValue == nil || renderValue == nil {
			return nil, nil, corruptReleaseRecord()
		}
		intent, intentErr := decodeReleaseRecord[domain.Intent](intentValue.Value, "release-intent")
		raw, rawErr := decodeReleaseRecord[json.RawMessage](renderValue.Value, "release-render-input")
		render, renderErr := decodeReleaseRenderInput(raw)
		intentDigest, digestErr := domain.Digest(intent)
		renderDigest, renderDigestErr := domain.Digest(raw)
		if intentErr != nil || rawErr != nil || renderErr != nil || digestErr != nil || renderDigestErr != nil ||
			domain.ValidateIntent(intent) != nil ||
			intentDigest != reference.IntentDigest || renderDigest != reference.RenderDigest ||
			renderDigest != intent.RenderInputDigest || intent.ID != reference.ReleaseID ||
			intent.ServiceID != reference.ServiceID || intent.OperationID != task.OperationID ||
			intent.EnvironmentID != task.Owner.EnvironmentID || intent.OperationKind != domain.OperationBlueprintApply ||
			render.ReleaseID != intent.ID || render.ServiceID != intent.ServiceID || render.PlanID != task.PlanID ||
			render.EnvironmentID != task.Owner.EnvironmentID || render.ArtifactID != task.Params[TaskComposeArtifactParam] {
			return nil, nil, corruptReleaseRecord()
		}
		members[index] = ReleaseTaskRenderMember{Intent: intent, Render: render}
		conditions[index*2] = etcdstore.Condition{Key: keys[index*2], ModRevision: intentValue.ModRevision}
		conditions[index*2+1] = etcdstore.Condition{Key: keys[index*2+1], ModRevision: renderValue.ModRevision}
	}
	native, err := resolveBlueprintNativePredecessors(marker.NativePredecessors, members)
	return native, conditions, err
}
