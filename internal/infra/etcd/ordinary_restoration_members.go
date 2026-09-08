package etcd

import (
	"cmp"
	"context"
	"encoding/json"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func validateOrdinaryPriorRuntime(render ReleaseRenderInput) error {
	witness := render.PriorRuntime
	if witness == nil {
		return nil
	}
	if witness.ServiceID != render.ServiceID || render.PriorWorkload == nil ||
		executionplan.ValidateNativePredecessorWitness(render.EnvironmentID, render.ServiceID,
			witness.CurrentArtifact, witness.RetainedPriorArtifact) != nil {
		return corruptReleaseRecord()
	}
	artifact, err := openRestorationWitness(render.EnvironmentID, witness.CurrentArtifact)
	if err != nil || artifact.ArtifactId != render.PriorArtifactID || artifact.ArtifactId == render.ArtifactID {
		return corruptReleaseRecord()
	}
	return nil
}

func (repository *TaskRepository) ordinaryRestorationMembersAtRevision(
	ctx context.Context,
	task TaskRecord,
	manifest ReleaseStagedManifest,
	procedure *agentpb.CandidateReleaseProcedure,
	revision int64,
) ([]ReleaseNativePredecessorAuthority, []Condition, error) {
	keys := make([]string, 0, len(manifest.Members)*2)
	selected := make(map[string]bool, len(procedure.GetMembers()))
	for _, member := range procedure.GetMembers() {
		selected[member.GetServiceId()] = member.GetServingPredecessor() != nil
	}
	for _, member := range manifest.Members {
		if !selected[member.ServiceID] {
			continue
		}
		keys = append(keys, releaseIntentStagingKey(manifest.PublicationID, member.ReleaseID),
			releaseRenderInputStagingKey(manifest.PublicationID, member.ReleaseID))
	}
	read := &GetManyResult{ReadRevision: revision}
	var err error
	if len(keys) != 0 {
		read, err = repository.store.GetMany(ctx, GetManyRequest{Keys: keys, Revision: revision})
	}
	if err != nil {
		return nil, nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
		return nil, nil, corruptReleaseRecord()
	}
	defer clearKeyValues(read.Values)
	witnesses := make([]ReleaseNativePredecessorAuthority, 0, len(manifest.Members))
	conditions := make([]Condition, 0, len(keys))
	index := 0
	for _, member := range manifest.Members {
		if !selected[member.ServiceID] {
			witnesses = append(witnesses, ReleaseNativePredecessorAuthority{ServiceID: member.ServiceID})
			continue
		}
		intentValue, renderValue := read.Values[2*index], read.Values[2*index+1]
		if intentValue == nil || renderValue == nil {
			return nil, nil, corruptReleaseRecord()
		}
		intent, intentErr := decodeReleaseRecord[domain.Intent](intentValue.Value, "release-intent")
		raw, rawErr := decodeReleaseRecord[json.RawMessage](renderValue.Value, "release-render-input")
		render, renderErr := decodeReleaseRenderInput(raw)
		intentDigest, intentDigestErr := domain.Digest(intent)
		renderDigest, renderDigestErr := domain.Digest(raw)
		if intentErr != nil || rawErr != nil || renderErr != nil || intentDigestErr != nil || renderDigestErr != nil ||
			intentDigest != member.IntentDigest || renderDigest != member.RenderDigest || renderDigest != intent.RenderInputDigest ||
			intent.ID != member.ReleaseID || intent.ServiceID != member.ServiceID || intent.OperationID != task.OperationID ||
			intent.EnvironmentID != task.Owner.EnvironmentID || render.ReleaseID != member.ReleaseID ||
			render.ServiceID != member.ServiceID || render.EnvironmentID != task.Owner.EnvironmentID ||
			render.PlanID != task.PlanID || render.ArtifactID != task.Params[TaskComposeArtifactParam] ||
			(intent.PriorServingReleaseID == "") != (render.PriorRuntime == nil) {
			return nil, nil, corruptReleaseRecord()
		}
		witness := ReleaseNativePredecessorAuthority{ServiceID: member.ServiceID}
		if render.PriorRuntime != nil {
			witness = *render.PriorRuntime
			if witness.ServiceID != member.ServiceID {
				return nil, nil, corruptReleaseRecord()
			}
			witness.CurrentArtifact = slices.Clone(witness.CurrentArtifact)
			witness.RetainedPriorArtifact = slices.Clone(witness.RetainedPriorArtifact)
			if !recoveryRenderMatchesPredecessor(render, intent, &ReleaseRestorationAuthority{
				NativePredecessors: []ReleaseNativePredecessorAuthority{witness},
			}) {
				return nil, nil, corruptReleaseRecord()
			}
		}
		witnesses = append(witnesses, witness)
		conditions = append(conditions, Condition{Key: keys[2*index], ModRevision: intentValue.ModRevision},
			Condition{Key: keys[2*index+1], ModRevision: renderValue.ModRevision})
		index++
	}
	slices.SortFunc(witnesses, func(a, b ReleaseNativePredecessorAuthority) int {
		return cmp.Compare(a.ServiceID, b.ServiceID)
	})
	return witnesses, conditions, nil
}
