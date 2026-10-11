package imagedelivery

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// A completed router child can still own candidate images for Service children
// not yet published. Its visible parent's terminal state, not router completion,
// releases that pre-publication hold. Published releases retain their own images.
func (service *Service) retainProxyReplacementImages(ctx context.Context,
	retention *imageRetention, value keyvalue.KeyValue, revision int64) error {
	execution, err := componentplanning.DecodeChildExecution(value.Value)
	if err != nil {
		return err
	}
	if value.Key != componentplanning.ChildExecutionKey(execution.TaskID) {
		return errs.New(errs.KindStateConflict, "proxy replacement image capture has the wrong storage identity")
	}
	if execution.ProxyPreflight == nil {
		return nil
	}
	read, err := service.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{taskjournal.TaskStorageKey(execution.ParentTaskID)}, Revision: revision})
	if err != nil {
		return err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return errs.New(errs.KindInternal, "proxy replacement image owner read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return nil // A pruned parent cannot prepare new children.
	}
	parent, err := etcd.DecodeTaskRecord(read.Values[0].Value)
	if err != nil || parent.ID != execution.ParentTaskID || parent.Owner.EnvironmentID != execution.EnvironmentID ||
		parent.Executor != taskjournal.TaskExecutorBlueprint {
		return errs.New(errs.KindStateConflict, "proxy replacement image capture differs from its parent")
	}
	if taskjournal.IsTerminalTaskStatus(parent.Status) {
		return nil
	}
	return service.retainCapturedProxyImages(ctx, retention, *execution.ProxyPreflight, parent.ID, revision)
}

func (service *Service) retainControllerProxyReplacementImages(ctx context.Context,
	retention *imageRetention, parent etcd.TaskRecord, revision int64,
) error {
	owned, found, err := blueprints.ReadOwnedIdentitiesRevision(
		ctx,
		service.store,
		parent.Owner.EnvironmentID,
		parent.ID,
		revision,
	)
	if err != nil {
		return err
	}
	if !found {
		return errs.New(errs.KindStateConflict, "active Apply has no immutable proxy membership")
	}
	for _, identity := range owned.Record.Components {
		selection, found, err := componentplanning.ReadControllerProxySelection(ctx, service.store,
			parent.Owner.EnvironmentID, parent.ID, identity.ID, revision)
		if err != nil {
			return err
		}
		if found {
			if err := service.retainCapturedProxyImages(ctx, retention, selection.Record.Preflight, parent.ID, revision); err != nil {
				return err
			}
		}
	}
	return nil
}

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
