package imagedelivery

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
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

// The future Service Task does not exist yet. Its parent's recipe must retain
// every native candidate, predecessor and hook image until normal publication
// gives those inputs their own Task/Release owner.
func (service *Service) retainProxyRolloutImages(ctx context.Context, retention *imageRetention,
	environmentID, parentID string, source componentplanning.ProxyServiceSource, revision int64,
) error {
	reference := source.Rollout
	if reference.Validate() != nil || reference.ServiceID != source.ServiceID || !source.ReplacementRequired {
		return errs.New(errs.KindStateConflict, "pending proxy rollout image authority is invalid")
	}
	key := componentplanning.ProxyServiceNativeRolloutKey(environmentID, parentID, reference.TaskID)
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
	capture, err := releaserender.DecodeBlueprintServiceRolloutCapture(value.Value)
	if err != nil {
		return err
	}
	defer clear(capture.NativePlan)
	plan, err := releaserender.OpenBlueprintServiceRolloutCapture(capture)
	if err != nil {
		return err
	}
	if capture.TaskID != reference.TaskID || capture.Owner.EnvironmentID != environmentID ||
		capture.Params[taskjournal.TaskBlueprintParentParam] != parentID ||
		plan.GetCandidateReleaseProcedure().GetMembers()[0].GetServiceId() != source.ServiceID {
		return errs.New(errs.KindStateConflict, "pending proxy rollout image capture belongs to another Service")
	}
	reason := "Pending proxy replacement in Apply " + parentID
	for _, artifact := range plan.GetArtifacts() {
		retention.composeArtifact(artifact, reason)
	}
	for _, snapshot := range plan.GetScriptRunnerSnapshots() {
		retention.retain(snapshot.GetLocalImageId(), reason)
		if explicit := snapshot.GetExplicitExecution(); explicit != nil {
			retention.retain(explicit.GetReleaseLocalImageId(), reason)
			retention.retain(explicit.GetContext().GetImageReference(), reason)
		}
	}
	return nil
}
