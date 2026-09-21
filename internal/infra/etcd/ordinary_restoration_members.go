package etcd

import (
	"cmp"
	"context"
	"encoding/json"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	releases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"slices"

	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (repository *TaskRepository) ordinaryRestorationMembersAtRevision(
	ctx context.Context,
	task TaskRecord,
	manifest releases.ReleaseStagedManifest,
	procedure *agentpb.CandidateReleaseProcedure,
	revision int64,
) ([]taskassignments.ReleaseNativePredecessorAuthority, []etcdstore.Condition, error) {
	keys := make([]string, 0, len(manifest.Members)*2)
	selected := make(map[string]bool, len(procedure.GetMembers()))
	for _, member := range procedure.GetMembers() {
		selected[member.GetServiceId()] = member.GetServingPredecessor() != nil
	}
	for _, member := range manifest.Members {
		if !selected[member.ServiceID] {
			continue
		}
		keys = append(keys, releases.ReleaseIntentStagingKey(manifest.PublicationID, member.ReleaseID),
			releases.ReleaseRenderInputStagingKey(manifest.PublicationID, member.ReleaseID))
	}
	read := &etcdstore.GetManyResult{ReadRevision: revision}
	var err error
	if len(keys) != 0 {
		read, err = repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	}
	if err != nil {
		return nil, nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
		return nil, nil, releases.CorruptReleaseRecord()
	}
	defer etcdstore.ClearValues(read.Values)
	witnesses := make([]taskassignments.ReleaseNativePredecessorAuthority, 0, len(manifest.Members))
	conditions := make([]etcdstore.Condition, 0, len(keys))
	index := 0
	for _, member := range manifest.Members {
		if !selected[member.ServiceID] {
			witnesses = append(
				witnesses,
				taskassignments.ReleaseNativePredecessorAuthority{ServiceID: member.ServiceID},
			)
			continue
		}
		intentValue, renderValue := read.Values[2*index], read.Values[2*index+1]
		if intentValue == nil || renderValue == nil {
			return nil, nil, releases.CorruptReleaseRecord()
		}
		intent, intentErr := releases.DecodeReleaseRecord[domain.Intent](intentValue.Value, "release-intent")
		raw, rawErr := releases.DecodeReleaseRecord[json.RawMessage](renderValue.Value, "release-render-input")
		render, renderErr := releaserender.DecodeReleaseRenderInput(raw)
		intentDigest, intentDigestErr := domain.Digest(intent)
		renderDigest, renderDigestErr := domain.Digest(raw)
		if intentErr != nil || rawErr != nil || renderErr != nil || intentDigestErr != nil || renderDigestErr != nil ||
			intentDigest != member.IntentDigest || renderDigest != member.RenderDigest || renderDigest != intent.RenderInputDigest ||
			intent.ID != member.ReleaseID || intent.ServiceID != member.ServiceID || intent.OperationID != task.OperationID ||
			intent.EnvironmentID != task.Owner.EnvironmentID || render.ReleaseID != member.ReleaseID ||
			render.ServiceID != member.ServiceID || render.EnvironmentID != task.Owner.EnvironmentID ||
			render.PlanID != task.PlanID || render.ArtifactID != task.Params[taskjournal.TaskComposeArtifactParam] ||
			(intent.PriorServingReleaseID == "") != (render.PriorRuntime == nil) {
			return nil, nil, releases.CorruptReleaseRecord()
		}
		witness := taskassignments.ReleaseNativePredecessorAuthority{ServiceID: member.ServiceID}
		if render.PriorRuntime != nil {
			witness = *render.PriorRuntime
			if witness.ServiceID != member.ServiceID {
				return nil, nil, releases.CorruptReleaseRecord()
			}
			witness.CurrentArtifact = slices.Clone(witness.CurrentArtifact)
			witness.RetainedPriorArtifact = slices.Clone(witness.RetainedPriorArtifact)
			if !recoveryRenderMatchesPredecessor(render, intent, &taskassignments.ReleaseRestorationAuthority{
				NativePredecessors: []taskassignments.ReleaseNativePredecessorAuthority{witness},
			}) {
				return nil, nil, releases.CorruptReleaseRecord()
			}
		}
		witnesses = append(witnesses, witness)
		conditions = append(conditions, etcdstore.Condition{Key: keys[2*index], ModRevision: intentValue.ModRevision},
			etcdstore.Condition{Key: keys[2*index+1], ModRevision: renderValue.ModRevision})
		index++
	}
	slices.SortFunc(witnesses, func(a, b taskassignments.ReleaseNativePredecessorAuthority) int {
		return cmp.Compare(a.ServiceID, b.ServiceID)
	})
	return witnesses, conditions, nil
}
