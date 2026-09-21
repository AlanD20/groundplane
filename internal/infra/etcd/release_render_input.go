package etcd

import (
	"context"
	"encoding/json"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	releases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type ReleaseTaskRenderInput struct {
	NativePredecessors []BlueprintNativePredecessor
	PublicationID      string
	Operation          releases.ReleaseOperationHead
	Members            []releaserender.ReleaseTaskRenderMember
}

func (ledger *ReleaseLedger) GetTaskRenderInput(
	ctx context.Context,
	task TaskRecord,
) (ReleaseTaskRenderInput, error) {
	publicationID := task.Params[releaserender.TaskReleasePublicationParam]
	if ctx == nil || ledger == nil || releases.ValidatePublicationID(publicationID) != nil ||
		(task.Type != taskjournal.TaskDeploy && task.Type != taskjournal.TaskRollback) || ids.Validate(ids.KindPlan, task.PlanID) != nil {
		return ReleaseTaskRenderInput{}, errs.New(
			errs.KindValidationFailed,
			"release Task render input request is invalid",
		)
	}
	keys := []string{
		releases.ReleaseManifestStagingKey(
			publicationID,
		), releases.ReleasePublicationKey(publicationID), releases.ReleaseOperationKey(task.OperationID),
	}
	loaded, err := ledger.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys})
	if err != nil {
		return ReleaseTaskRenderInput{}, err
	}
	if loaded == nil || len(loaded.Values) != len(keys) || loaded.Values[0] == nil || loaded.Values[1] == nil ||
		loaded.Values[2] == nil {
		return ReleaseTaskRenderInput{}, releases.CorruptReleaseRecord()
	}
	manifest, err := releases.DecodeReleaseRecord[releases.ReleaseStagedManifest](
		loaded.Values[0].Value,
		"release-staged-manifest",
	)
	if err != nil || manifest.PublicationID != publicationID || manifest.OperationID != task.OperationID {
		return ReleaseTaskRenderInput{}, releases.CorruptReleaseRecord()
	}
	marker, err := releases.DecodeReleaseRecord[releases.ReleasePublicationMarker](
		loaded.Values[1].Value,
		"release-publication",
	)
	if err != nil || marker.PublicationID != publicationID || marker.OperationID != task.OperationID ||
		marker.ManifestDigest != manifest.Digest {
		return ReleaseTaskRenderInput{}, releases.CorruptReleaseRecord()
	}
	head, err := releases.DecodeReleaseRecord[releases.ReleaseOperationHead](
		loaded.Values[2].Value,
		"release-operation",
	)
	if err != nil || head.OperationID != task.OperationID || head.PublicationID != publicationID ||
		head.LatestTaskID != task.ID {
		return ReleaseTaskRenderInput{}, releases.CorruptReleaseRecord()
	}
	memberKeys := make([]string, 0, len(manifest.Members)*2)
	for _, member := range manifest.Members {
		memberKeys = append(memberKeys, releases.ReleaseIntentStagingKey(publicationID, member.ReleaseID),
			releases.ReleaseRenderInputStagingKey(publicationID, member.ReleaseID))
	}
	members, err := ledger.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: memberKeys, Revision: loaded.ReadRevision})
	if err != nil {
		return ReleaseTaskRenderInput{}, err
	}
	if members == nil || members.ReadRevision != loaded.ReadRevision || len(members.Values) != len(memberKeys) {
		return ReleaseTaskRenderInput{}, releases.CorruptReleaseRecord()
	}
	result := ReleaseTaskRenderInput{
		PublicationID: publicationID, Operation: releases.CloneReleaseOperationHead(head),
		Members: make([]releaserender.ReleaseTaskRenderMember, len(manifest.Members)),
	}
	for index, reference := range manifest.Members {
		intentValue := members.Values[index*2]
		renderValue := members.Values[index*2+1]
		if intentValue == nil || renderValue == nil {
			return ReleaseTaskRenderInput{}, releases.CorruptReleaseRecord()
		}
		intent, err := releases.DecodeReleaseRecord[domain.Intent](intentValue.Value, "release-intent")
		if err != nil || domain.ValidateIntent(intent) != nil || intent.ID != reference.ReleaseID ||
			intent.ServiceID != reference.ServiceID || intent.OperationID != task.OperationID ||
			(len(head.Attempts) == 0 || intent.OriginatingTaskID != head.Attempts[0].TaskID) || intent.RenderInputID == "" {
			return ReleaseTaskRenderInput{}, releases.CorruptReleaseRecord()
		}
		raw, err := releases.DecodeReleaseRecord[json.RawMessage](renderValue.Value, "release-render-input")
		if err != nil {
			return ReleaseTaskRenderInput{}, err
		}
		render, err := releaserender.DecodeReleaseRenderInput(raw)
		if err != nil || render.ReleaseID != intent.ID || render.PlanID != task.PlanID ||
			render.ArtifactID != intent.RenderInputID || render.ServiceID != intent.ServiceID ||
			render.CandidateWorkload != intent.CandidateWorkload || render.Strategy != intent.Strategy || render.Slot != intent.Slot {
			return ReleaseTaskRenderInput{}, releases.CorruptReleaseRecord()
		}
		digest, _ := domain.Digest(raw)
		if digest != intent.RenderInputDigest || digest != reference.RenderDigest {
			return ReleaseTaskRenderInput{}, releases.CorruptReleaseRecord()
		}
		result.Members[index] = releaserender.ReleaseTaskRenderMember{Intent: intent, Render: render}
	}
	return result, nil
}
