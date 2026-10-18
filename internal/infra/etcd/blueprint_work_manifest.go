package etcd

import (
	"bytes"
	"context"
	"encoding/json"

	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
)

// blueprintWorkManifestMember binds the original staged intent and render to the
// native rollout. Hook snapshots are sealed after manifest staging; they belong
// to the rollout/input digest, not a rewritten manifest.
func (ledger *ReleaseLedger) blueprintWorkManifestMember(ctx context.Context,
	evidence BlueprintReleasePublicationEvidence,
) (releaserender.ReleaseTaskRenderMember, []keyvalue.Condition, error) {
	var empty releaserender.ReleaseTaskRenderMember
	if evidence.ServiceRollout == nil || len(evidence.Manifest.Record.Members) != 1 ||
		evidence.Manifest.Revision <= 0 {
		return empty, nil, releases.CorruptReleaseRecord()
	}
	capture := evidence.ServiceRollout.Record
	if err := validateBlueprintServiceRolloutWork(capture, evidence.Work.Task.Task, evidence.Work.Item.Item); err != nil {
		return empty, nil, err
	}
	manifest := evidence.Manifest.Record
	if manifest.PublicationID != capture.PublicationID {
		return empty, nil, releases.CorruptReleaseRecord()
	}
	reference := manifest.Members[0]
	keys := []string{releases.ReleaseIntentStagingKey(manifest.PublicationID, reference.ReleaseID),
		releases.ReleaseRenderInputStagingKey(manifest.PublicationID, reference.ReleaseID)}
	read, err := ledger.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: evidence.Manifest.Revision})
	if err != nil {
		return empty, nil, err
	}
	if read == nil || read.ReadRevision != evidence.Manifest.Revision || len(read.Values) != 2 {
		return empty, nil, releases.CorruptReleaseRecord()
	}
	defer keyvalue.ClearValues(read.Values)
	for index, value := range read.Values {
		if value == nil || value.Key != keys[index] || value.ModRevision <= 0 || value.ModRevision > read.ReadRevision {
			return empty, nil, releases.CorruptReleaseRecord()
		}
	}
	intent, intentErr := releases.DecodeReleaseRecord[domain.Intent](read.Values[0].Value, "release-intent")
	raw, rawErr := releases.DecodeReleaseRecord[json.RawMessage](read.Values[1].Value, "release-render-input")
	render, renderErr := releaserender.DecodeReleaseRenderInput(raw)
	intentDigest, digestErr := domain.Digest(intent)
	renderDigest, renderDigestErr := domain.Digest(raw)
	if intentErr != nil || rawErr != nil || renderErr != nil || digestErr != nil || renderDigestErr != nil ||
		domain.ValidateIntent(intent) != nil || intentDigest != reference.IntentDigest ||
		renderDigest != reference.RenderDigest || renderDigest != intent.RenderInputDigest ||
		intent.ID != reference.ReleaseID || intent.ServiceID != reference.ServiceID ||
		intent.OperationID != capture.OperationID || intent.OperationID != manifest.OperationID ||
		intent.OriginatingTaskID != capture.TaskID || intent.EnvironmentID != capture.Scope.EnvironmentID ||
		intent.OperationKind != domain.OperationBlueprintApply || render.ReleaseID != intent.ID ||
		render.ServiceID != intent.ServiceID || render.CandidateWorkload != intent.CandidateWorkload {
		return empty, nil, releases.CorruptReleaseRecord()
	}
	staged := releaserender.CloneReleaseRenderInput(render)
	captured := releaserender.CloneReleaseRenderInput(capture.Render)
	staged.Hooks, captured.Hooks = nil, nil
	stagedBytes, err := releaserender.EncodeReleaseRenderInput(staged)
	if err != nil {
		return empty, nil, err
	}
	defer clear(stagedBytes)
	capturedBytes, err := releaserender.EncodeReleaseRenderInput(captured)
	if err != nil {
		return empty, nil, err
	}
	defer clear(capturedBytes)
	if !bytes.Equal(stagedBytes, capturedBytes) {
		return empty, nil, releases.CorruptReleaseRecord()
	}
	return releaserender.ReleaseTaskRenderMember{Intent: intent, Render: capture.Render}, []keyvalue.Condition{
		{Key: keys[0], ModRevision: read.Values[0].ModRevision},
		{Key: keys[1], ModRevision: read.Values[1].ModRevision},
	}, nil
}
