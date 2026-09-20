package blueprintrelease

import (
	"bytes"
	"context"
	"encoding/json"
	"sort"

	"github.com/AlanD20/groundplane/internal/controller/servicelifecycle"
	taskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type retainedRuntime struct {
	applied   etcd.Versioned[etcd.EnvironmentComposeProjection]
	services  map[string]etcd.Versioned[etcd.ServiceRecord]
	snapshots map[string]etcd.BlueprintRetainedRuntimeSource
	artifacts []*agentpb.ComposeArtifact
}

func (service *Service) captureRetainedRuntime(
	ctx context.Context,
	environmentID string,
	changes, candidates []etcd.EnvironmentBlueprintServiceChange,
) (*retainedRuntime, error) {
	selected := make(map[string]bool, len(candidates))
	for _, candidate := range candidates {
		selected[candidate.Record.Desired.ID] = true
	}
	var scope *etcd.ReleasePlanningScope
	var result *retainedRuntime
	for _, change := range changes {
		if change.Current == nil || selected[change.Record.Desired.ID] {
			continue
		}
		if scope == nil {
			loaded, err := service.ledger.LoadPlanningScopeAtRevision(ctx, environmentID, change.Current.ReadRevision)
			if err != nil {
				return nil, err
			}
			scope = &loaded
		}
		if scope.ReadRevision != change.Current.ReadRevision {
			return nil, errs.New(errs.KindStateConflict, "Blueprint retained Service capture revisions disagree")
		}
		planning, err := service.ledger.LoadPlanningServices(ctx, *scope, []string{change.Record.Desired.ID})
		if err != nil {
			return nil, err
		}
		applied, present, err := service.ledger.GetPlanningAppliedProjection(ctx, *scope)
		if err != nil {
			return nil, err
		}
		if result == nil {
			result = &retainedRuntime{
				applied:   applied,
				services:  make(map[string]etcd.Versioned[etcd.ServiceRecord]),
				snapshots: make(map[string]etcd.BlueprintRetainedRuntimeSource),
			}
		}
		source := etcd.BlueprintRetainedRuntimeSource{Planning: planning[0]}
		result.snapshots[change.Record.Desired.ID] = source
		if planning[0].Projection.ServingReleaseID == "" {
			continue
		}
		if !present {
			return nil, errs.New(errs.KindStateConflict, "Blueprint retained runtime has no applied witness")
		}
		release, err := servicelifecycle.CaptureRelease(
			ctx,
			service.ledger,
			applied,
			environmentID,
			change.Record.Desired.ID,
		)
		if err != nil {
			return nil, err
		}
		serving, err := service.ledger.ResolveServing(ctx, environmentID, change.Record.Desired.ID, scope.ReadRevision)
		if err != nil {
			return nil, err
		}
		source.Release, source.Intent = &release, &serving
		artifacts, err := service.plans.RenderRetainedServiceRuntime(ctx, release)
		if err != nil {
			return nil, err
		}
		result.snapshots[change.Record.Desired.ID] = source
		result.artifacts = append(result.artifacts, artifacts...)
		result.services[change.Record.Desired.ID] = *change.Current
	}
	return result, nil
}

// PrepareRuntimeArtifact binds the post-claim desired records to the preclaim
// snapshot, then seals the exact mixed artifact before desired staging.
func (service *Service) PrepareRuntimeArtifact(
	workloads WorkloadPreparation,
	artifact *agentpb.ComposeArtifact,
	changes []etcd.EnvironmentBlueprintServiceChange,
) (*agentpb.ComposeArtifact, error) {
	artifact, err := service.prepareNativeRuntimeArtifact(workloads, artifact, changes)
	if err != nil {
		return nil, err
	}
	if workloads.retained != nil {
		return taskplanning.RetainEnvironmentComponentRuntime(artifact, workloads.retained.applied.Record.ComposeArtifact)
	}
	for _, predecessor := range workloads.predecessors {
		// All captures share one planning revision; each is checked again by
		// preparePredecessor and its applied-key CAS before final publication.
		return taskplanning.RetainEnvironmentComponentRuntime(artifact, predecessor.applied.Record.ComposeArtifact)
	}
	return artifact, nil
}

func (service *Service) prepareNativeRuntimeArtifact(
	workloads WorkloadPreparation,
	artifact *agentpb.ComposeArtifact,
	changes []etcd.EnvironmentBlueprintServiceChange,
) (*agentpb.ComposeArtifact, error) {
	if workloads.retained == nil || len(workloads.retained.services) == 0 {
		return artifact, nil
	}
	captured := workloads.retained
	ids := make([]string, 0, len(captured.services))
	for _, change := range changes {
		prior, found := captured.services[change.Record.Desired.ID]
		if !found {
			continue
		}
		if change.Current == nil || change.Current.Revision != prior.Revision {
			return nil, errs.New(errs.KindStateConflict, "Blueprint retained Service changed after preflight")
		}
		before, beforeErr := json.Marshal(prior.Record)
		after, afterErr := json.Marshal(change.Current.Record)
		if beforeErr != nil || afterErr != nil || !bytes.Equal(before, after) {
			return nil, errs.New(errs.KindStateConflict, "Blueprint retained Service changed after preflight")
		}
		ids = append(ids, change.Record.Desired.ID)
	}
	if len(ids) != len(captured.services) {
		return nil, errs.New(errs.KindStateConflict, "Blueprint retained Service disappeared after preflight")
	}
	return taskplanning.RetainBlueprintNativeRuntimeSources(artifact, captured.artifacts, ids)
}

func (service *Service) prepareRetainedPublication(
	ctx context.Context,
	input PrepareInput,
	task etcd.TaskRecord,
	publication etcd.BlueprintReleasePublication,
) (etcd.BlueprintReleasePublication, error) {
	if input.Workloads.retained == nil {
		return publication, nil
	}
	captured := input.Workloads.retained
	planning := make([]etcd.BlueprintRetainedRuntimeSource, 0, len(captured.snapshots))
	for _, snapshot := range captured.snapshots {
		planning = append(planning, snapshot)
	}
	sort.Slice(planning, func(i, j int) bool {
		return planning[i].Planning.Service.Record.Desired.ID < planning[j].Planning.Service.Record.Desired.ID
	})
	return service.ledger.PrepareBlueprintRuntimeRetention(
		ctx,
		publication,
		task,
		captured.applied,
		planning,
		input.Artifact,
	)
}
