package imagedelivery

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (service *Service) retainCapturedProxyImages(ctx context.Context, retention *imageRetention,
	preflight componentplanning.ProxyServicePreflight, parentID string, revision int64,
) error {
	for _, source := range preflight.Services {
		if image := source.Replacement; image != nil {
			reason := "Pending proxy replacement in Apply " + parentID
			retention.retain(image.Workload.LocalImageID, reason)
			retention.retain(image.Proxy.Reference(), reason)
		}
		if source.Rollout != nil {
			if err := service.retainProxyRolloutImages(ctx, retention, preflight.EnvironmentID, parentID, source, revision); err != nil {
				return err
			}
		}
	}
	return nil
}

// Service work exists before its executable input is published. The captured
// rollout protects its candidate, predecessor and hooks throughout that wait.
func (service *Service) retainProxyRolloutImages(ctx context.Context, retention *imageRetention,
	environmentID, parentID string, source componentplanning.ProxyServiceSource, revision int64,
) error {
	reference := source.Rollout
	if reference.Validate() != nil || reference.ServiceID != source.ServiceID || !source.ReplacementRequired {
		return errs.New(errs.KindStateConflict, "pending proxy rollout image authority is invalid")
	}
	key := releaserender.ServiceRolloutKey(environmentID, parentID, reference.WorkItemID)
	read, err := service.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return errs.New(errs.KindInternal, "pending proxy rollout image read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	if value == nil || value.Key != key || value.ModRevision != reference.NativeRevision {
		return errs.New(errs.KindStateConflict, "pending proxy rollout image capture is missing or changed")
	}
	capture, err := releaserender.DecodeServiceRolloutCapture(value.Value)
	if err != nil {
		return err
	}
	defer clear(capture.CandidateArtifact)
	render, err := releaserender.OpenServiceRolloutCapture(capture)
	if err != nil {
		return err
	}
	if capture.Action != workexecution.ActionApply || capture.TaskID != parentID ||
		capture.WorkItemID != reference.WorkItemID || capture.Scope.EnvironmentID != environmentID ||
		render.ServiceID != source.ServiceID {
		return errs.New(errs.KindStateConflict, "pending proxy rollout image capture belongs to another Service")
	}
	reason := "Pending proxy replacement in Apply " + parentID
	return retainServiceRolloutImages(retention, capture, reason)
}

func retainServiceRolloutImages(
	retention *imageRetention,
	capture releaserender.ServiceRolloutCapture,
	reason string,
) error {
	render, err := releaserender.OpenServiceRolloutCapture(capture)
	if err != nil {
		return err
	}
	artifact, err := releaserender.OpenReleaseCandidateArtifact(capture)
	if err != nil {
		return err
	}
	retention.composeArtifact(artifact, reason)
	if prior := render.PriorRuntime; prior != nil {
		if err := retention.artifact(prior.CurrentArtifact, reason); err != nil {
			return err
		}
		if err := retention.artifact(prior.RetainedPriorArtifact, reason); err != nil {
			return err
		}
	}
	for _, hook := range render.Hooks {
		snapshot := &agentpb.ResolvedRunnerSnapshot{}
		if err := proto.Unmarshal(hook.RunnerSnapshot, snapshot); err != nil ||
			executionplan.RejectUnknown(snapshot) != nil {
			return errs.New(errs.KindStateConflict, "pending proxy rollout hook image capture is invalid")
		}
		retention.retain(snapshot.GetLocalImageId(), reason)
		if explicit := snapshot.GetExplicitExecution(); explicit != nil {
			retention.retain(explicit.GetReleaseLocalImageId(), reason)
			retention.retain(explicit.GetContext().GetImageReference(), reason)
		}
	}
	return nil
}
