package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type BlueprintReleaseWorkSources struct {
	Input        releaserender.BlueprintReleaseWorkInput
	Rollout      keyvalue.Versioned[releaserender.BlueprintServiceRolloutCapture]
	Conditions   []keyvalue.Condition
	Predecessors []BlueprintNativePredecessor
}

// LoadBlueprintReleaseWorkInput resolves only the input selected by native work.
// It does not consult desired heads, Task Params or generic execution plans.
// Admission separately joins file-head and operation-specific effect checks;
// recovery can still load its captured input after partially completed effects.
func (ledger *ReleaseLedger) LoadBlueprintReleaseWorkInput(ctx context.Context,
	work workqueue.View,
) (BlueprintReleaseWorkSources, error) {
	sources, err := ledger.loadCapturedBlueprintReleaseInput(ctx, work)
	if err != nil {
		return BlueprintReleaseWorkSources{}, err
	}
	if sources.Input.Secrets == nil {
		return sources, nil
	}
	pins, err := tasksecretpins.NewEtcdRepository(ledger.store, work.Task.Task.Scope.ProjectID)
	if err != nil {
		clear(sources.Rollout.Record.CandidateArtifact)
		return BlueprintReleaseWorkSources{}, err
	}
	root, found, err := pins.LoadActive(ctx, sources.Input.OperationID, work.ReadRevision)
	if err != nil || !found {
		clear(sources.Rollout.Record.CandidateArtifact)
		if err != nil {
			return BlueprintReleaseWorkSources{}, err
		}
		return BlueprintReleaseWorkSources{}, errs.New(errs.KindStateConflict, "Blueprint Release no longer retains its recovery Secret values")
	}
	condition, err := root.AdmissionCondition(*sources.Input.Secrets)
	if err != nil {
		clear(sources.Rollout.Record.CandidateArtifact)
		return BlueprintReleaseWorkSources{}, err
	}
	sources.Conditions = append(sources.Conditions, keyvalue.Condition{Key: condition.Key, ModRevision: condition.ModRevision})
	return sources, nil
}

// LoadRetiringBlueprintReleaseInput reads captured metadata only after the
// collection fence. It cannot authorize assignment, execution or recovery.
func (ledger *ReleaseLedger) LoadRetiringBlueprintReleaseInput(ctx context.Context, work workqueue.View) (BlueprintReleaseWorkSources, error) {
	if work.Task.Task.CollectionStartedAt == nil || work.Task.Task.FinishedAt == nil ||
		work.Task.Task.Progress.Unsettled() != 0 || work.Item.Item.Phase != workexecution.Settled || work.Item.Item.ActiveAttemptID != "" {
		return BlueprintReleaseWorkSources{}, errs.New(errs.KindStateConflict, "Release metadata retirement requires settled collecting work")
	}
	return ledger.loadCapturedBlueprintReleaseInput(ctx, work)
}

func (ledger *ReleaseLedger) loadCapturedBlueprintReleaseInput(ctx context.Context, work workqueue.View) (BlueprintReleaseWorkSources, error) {
	var empty BlueprintReleaseWorkSources
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return empty, err
	}
	task, item := work.Task.Task, work.Item.Item
	if ledger == nil || ledger.store == nil || work.ReadRevision <= 0 || work.Task.Revision <= 0 || work.Item.Revision <= 0 ||
		task.Validate() != nil || item.Validate() != nil || item.TaskID != task.ID ||
		task.Action != workexecution.ActionApply || item.Operation != workexecution.Release || item.Input == nil {
		return empty, errs.New(errs.KindValidationFailed, "Blueprint Release input requires captured native work")
	}
	key := BlueprintReleaseWorkInputKey(item.ID)
	read, err := ledger.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: work.ReadRevision})
	if err != nil {
		return empty, err
	}
	if read == nil || read.ReadRevision != work.ReadRevision || len(read.Values) != 1 {
		return empty, errs.New(errs.KindStateConflict, "Blueprint Release input read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	if value == nil || value.Key != key || value.ModRevision <= 0 || value.ModRevision > work.ReadRevision {
		return empty, errs.New(errs.KindStateConflict, "Blueprint Release input is unavailable")
	}
	input, err := releaserender.DecodeBlueprintReleaseWorkInput(value.Value, *item.Input)
	if err != nil {
		return empty, err
	}
	if err := input.Configuration.Matches(task, item); err != nil {
		return empty, err
	}
	rollout, err := ledger.GetBlueprintServiceRollout(ctx, work)
	if err != nil {
		return empty, err
	}
	if err := input.ValidateRollout(rollout.Record); err != nil {
		return empty, err
	}
	publicationKey := releases.ReleasePublicationKey(rollout.Record.PublicationID)
	manifestKey := releases.ReleaseManifestStagingKey(rollout.Record.PublicationID)
	published, err := ledger.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{publicationKey, manifestKey}, Revision: work.ReadRevision,
	})
	if err != nil {
		return empty, err
	}
	if published == nil || published.ReadRevision != work.ReadRevision || len(published.Values) != 2 {
		return empty, releases.CorruptReleaseRecord()
	}
	defer keyvalue.ClearValues(published.Values)
	for index, key := range []string{publicationKey, manifestKey} {
		if published.Values[index] == nil || published.Values[index].Key != key ||
			published.Values[index].ModRevision <= 0 || published.Values[index].ModRevision > work.ReadRevision {
			return empty, releases.CorruptReleaseRecord()
		}
	}
	marker, err := releases.DecodeWorkPublication(published.Values[0].Value)
	if err != nil {
		return empty, err
	}
	manifest, err := releases.DecodeReleaseRecord[releases.ReleaseStagedManifest](published.Values[1].Value, "release-staged-manifest")
	if err != nil || marker.PublicationID != rollout.Record.PublicationID || marker.OperationID != input.OperationID ||
		marker.TaskID != task.ID || marker.WorkItemID != item.ID || marker.Input != *item.Input ||
		marker.EnvironmentID != task.Scope.EnvironmentID || marker.ServiceID != item.TargetID ||
		manifest.PublicationID != marker.PublicationID || manifest.OperationID != marker.OperationID ||
		manifest.Digest != marker.ManifestDigest || len(manifest.Members) != 1 ||
		manifest.Members[0].ServiceID != item.TargetID || manifest.Members[0].ReleaseID != rollout.Record.Render.ReleaseID {
		return empty, releases.CorruptReleaseRecord()
	}
	conditions := []keyvalue.Condition{
		{Key: workqueue.TaskKey(task.ID), ModRevision: work.Task.Revision},
		{Key: workqueue.ItemKey(item.ID), ModRevision: work.Item.Revision},
		{Key: key, ModRevision: value.ModRevision},
		{Key: BlueprintServiceRolloutKey(task.Scope.EnvironmentID, task.ID, item.ID), ModRevision: rollout.Revision},
		{Key: publicationKey, ModRevision: published.Values[0].ModRevision},
		{Key: manifestKey, ModRevision: published.Values[1].ModRevision},
	}
	member, memberConditions, err := ledger.blueprintWorkManifestMember(ctx, BlueprintReleasePublicationEvidence{
		Work: work, ServiceRollout: &rollout,
		Manifest: releases.VersionedReleaseManifest{Record: manifest, Revision: published.Values[1].ModRevision, ReadRevision: work.ReadRevision},
	})
	if err != nil {
		return empty, err
	}
	predecessors, err := resolveBlueprintNativePredecessors(marker.NativePredecessors, []releaserender.ReleaseTaskRenderMember{member})
	if err != nil {
		return empty, err
	}
	for _, predecessor := range predecessors {
		if predecessor.FixedReadRevision > rollout.Revision {
			return empty, releases.CorruptReleaseRecord()
		}
	}
	conditions = append(conditions, memberConditions...)
	return BlueprintReleaseWorkSources{Input: input, Rollout: rollout, Conditions: conditions, Predecessors: predecessors}, nil
}
