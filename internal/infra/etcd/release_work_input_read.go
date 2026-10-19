package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type ReleaseWorkSources struct {
	Intent       domain.Intent
	Input        releaserender.ReleaseWorkInput
	Rollout      keyvalue.Versioned[releaserender.ServiceRolloutCapture]
	Conditions   []keyvalue.Condition
	Predecessors []BlueprintNativePredecessor
}

// LoadReleaseWorkInput resolves only the input selected by native work.
// It does not consult desired heads, Task Params or generic execution plans.
// Admission separately joins file-head and operation-specific effect checks;
// recovery can still load its captured input after partially completed effects.
func (ledger *ReleaseLedger) LoadReleaseWorkInput(ctx context.Context,
	work workqueue.View,
) (ReleaseWorkSources, error) {
	sources, err := ledger.loadCapturedReleaseWorkInput(ctx, work)
	if err != nil {
		return ReleaseWorkSources{}, err
	}
	if sources.Input.Secrets == nil {
		return sources, nil
	}
	pins, err := tasksecretpins.NewEtcdRepository(ledger.store, work.Task.Task.Scope.ProjectID)
	if err != nil {
		clear(sources.Rollout.Record.CandidateArtifact)
		return ReleaseWorkSources{}, err
	}
	root, found, err := pins.LoadActive(ctx, sources.Input.OperationID, work.ReadRevision)
	if err != nil || !found {
		clear(sources.Rollout.Record.CandidateArtifact)
		if err != nil {
			return ReleaseWorkSources{}, err
		}
		return ReleaseWorkSources{}, errs.New(
			errs.KindStateConflict,
			"Service Release no longer retains its recovery Secret values",
		)
	}
	condition, err := root.AdmissionCondition(*sources.Input.Secrets)
	if err != nil {
		clear(sources.Rollout.Record.CandidateArtifact)
		return ReleaseWorkSources{}, err
	}
	sources.Conditions = append(
		sources.Conditions,
		keyvalue.Condition{Key: condition.Key, ModRevision: condition.ModRevision},
	)
	return sources, nil
}

// LoadRetiringReleaseWorkInput reads captured metadata only after the
// collection fence. It cannot authorize assignment, execution or recovery.
func (ledger *ReleaseLedger) LoadRetiringReleaseWorkInput(
	ctx context.Context,
	work workqueue.View,
) (ReleaseWorkSources, error) {
	if work.Task.Task.CollectionStartedAt == nil || work.Task.Task.FinishedAt == nil ||
		work.Task.Task.Progress.Unsettled() != 0 || work.Item.Item.Phase != workexecution.Settled || work.Item.Item.ActiveAttemptID != "" {
		return ReleaseWorkSources{}, errs.New(
			errs.KindStateConflict,
			"Release metadata retirement requires settled collecting work",
		)
	}
	return ledger.loadCapturedReleaseWorkInput(ctx, work)
}

func (ledger *ReleaseLedger) loadCapturedReleaseWorkInput(
	ctx context.Context,
	work workqueue.View,
) (ReleaseWorkSources, error) {
	var empty ReleaseWorkSources
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return empty, err
	}
	task, item := work.Task.Task, work.Item.Item
	if ledger == nil || ledger.store == nil || work.ReadRevision <= 0 || work.Task.Revision <= 0 ||
		work.Item.Revision <= 0 ||
		task.Validate() != nil ||
		item.Validate() != nil ||
		item.TaskID != task.ID ||
		!releaserender.IsReleaseAction(task.Action) ||
		item.Operation != workexecution.Release ||
		item.Input == nil {
		return empty, errs.New(errs.KindValidationFailed, "Service Release input requires captured native work")
	}
	key := ReleaseWorkInputKey(item.ID)
	read, err := ledger.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: work.ReadRevision})
	if err != nil {
		return empty, err
	}
	if read == nil || read.ReadRevision != work.ReadRevision || len(read.Values) != 1 {
		return empty, errs.New(errs.KindStateConflict, "Service Release input read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	if value == nil || value.Key != key || value.ModRevision <= 0 || value.ModRevision > work.ReadRevision {
		return empty, errs.New(errs.KindStateConflict, "Service Release input is unavailable")
	}
	input, err := releaserender.DecodeReleaseWorkInput(value.Value, *item.Input)
	if err != nil {
		return empty, err
	}
	if err := input.Configuration.Matches(task, item); err != nil {
		return empty, err
	}
	rollout, err := ledger.GetServiceRollout(ctx, work)
	if err != nil {
		return empty, err
	}
	if err := input.ValidateRollout(rollout.Record); err != nil {
		return empty, err
	}
	var directGuard *keyvalue.Condition
	if task.Action == workexecution.ActionDeploy || task.Action == workexecution.ActionRollback {
		request, guard, err := ledger.ReadDirectReleasePreparation(ctx, work)
		if err != nil {
			return empty, err
		}
		if err := request.Matches(task, item, rollout.Record); err != nil {
			return empty, err
		}
		if err := request.MatchesInput(input); err != nil {
			return empty, err
		}
		directGuard = &guard
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
	manifest, err := releases.DecodeReleaseRecord[releases.ReleaseStagedManifest](
		published.Values[1].Value,
		"release-staged-manifest",
	)
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
		{
			Key:         releaserender.ServiceRolloutKey(task.Scope.EnvironmentID, task.ID, item.ID),
			ModRevision: rollout.Revision,
		},
		{Key: publicationKey, ModRevision: published.Values[0].ModRevision},
		{Key: manifestKey, ModRevision: published.Values[1].ModRevision},
	}
	if directGuard != nil {
		conditions = append(conditions, *directGuard)
	}
	member, memberConditions, err := ledger.releaseWorkManifestMember(ctx, ReleaseWorkPublicationEvidence{
		Work: work, ServiceRollout: &rollout,
		Manifest: releases.VersionedReleaseManifest{
			Record:       manifest,
			Revision:     published.Values[1].ModRevision,
			ReadRevision: work.ReadRevision,
		},
	})
	if err != nil {
		return empty, err
	}
	predecessors, err := resolveBlueprintNativePredecessors(
		marker.NativePredecessors,
		[]releaserender.ReleaseTaskRenderMember{member},
	)
	if err != nil {
		return empty, err
	}
	for _, predecessor := range predecessors {
		if predecessor.FixedReadRevision > rollout.Revision {
			return empty, releases.CorruptReleaseRecord()
		}
	}
	conditions = append(conditions, memberConditions...)
	return ReleaseWorkSources{
		Intent:       member.Intent,
		Input:        input,
		Rollout:      rollout,
		Conditions:   conditions,
		Predecessors: predecessors,
	}, nil
}
