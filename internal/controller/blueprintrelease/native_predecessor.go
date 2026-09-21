package blueprintrelease

import (
	"context"
	releasequeries "github.com/AlanD20/groundplane/internal/infra/etcd/releasequeries"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"sort"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/servicelifecycle"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) captureNativePredecessor(
	ctx context.Context,
	scope releasequeries.ReleasePlanningScope,
	captured predecessorSnapshot,
) (predecessorSnapshot, error) {
	return captureNativePredecessor(ctx, service.ledger, scope, captured)
}

func captureNativePredecessor(
	ctx context.Context,
	reader servicelifecycle.AcknowledgedRuntimeReader,
	scope releasequeries.ReleasePlanningScope,
	captured predecessorSnapshot,
) (predecessorSnapshot, error) {
	captured.native = etcd.BlueprintNativePredecessorCapture{
		ServiceID:         captured.planning.Service.Record.Desired.ID,
		FixedReadRevision: scope.ReadRevision, ProjectionRevision: captured.planning.ProjectionRevision,
	}
	if captured.serving == nil {
		return captured, nil
	}
	runtime, err := servicelifecycle.CaptureAcknowledgedRuntime(
		ctx,
		reader,
		captured.applied,
		scope.Environment.Record.ID,
		captured.native.ServiceID,
		ids.New(ids.KindConfig),
	)
	if err != nil {
		return predecessorSnapshot{}, err
	}
	return bindNativePredecessor(captured, runtime)
}

func bindNativePredecessor(
	captured predecessorSnapshot,
	runtime servicelifecycle.AcknowledgedRuntimeCapture,
) (predecessorSnapshot, error) {
	if captured.serving == nil {
		runtime.Clear()
		return predecessorSnapshot{}, errs.New(
			errs.KindStateConflict,
			"Blueprint acknowledged predecessor has no serving authority",
		)
	}
	expectedTarget, targetErr := domain.TargetFor(captured.serving.Strategy, captured.serving.Slot)
	if runtime.Release.ServingReleaseID != captured.serving.ID ||
		targetErr != nil || runtime.Release.Current.CandidateTarget != expectedTarget {
		runtime.Clear()
		return predecessorSnapshot{}, errs.New(
			errs.KindStateConflict,
			"Blueprint acknowledged predecessor is not serving",
		)
	}
	if runtime.RetainedPriorReleaseID != "" &&
		runtime.RetainedPriorReleaseID != captured.serving.PriorServingReleaseID {
		runtime.Clear()
		return predecessorSnapshot{}, errs.New(
			errs.KindStateConflict,
			"Blueprint retained predecessor is not acknowledged",
		)
	}
	captured.native.RuntimeRevision = runtime.RuntimeRevision
	captured.native.Serving = &runtime.Release
	captured.native.RetainedPriorReleaseID = runtime.RetainedPriorReleaseID
	captured.native.CurrentArtifact = runtime.CurrentArtifact
	captured.native.RetainedPriorArtifact = runtime.RetainedPriorArtifact
	return captured, nil
}

func nativePredecessors(input PrepareInput, members []releaserender.ReleaseTaskRenderMember) []etcd.BlueprintNativePredecessor {
	captures := nativePredecessorCaptures(input, members)
	result := make([]etcd.BlueprintNativePredecessor, len(captures))
	for index, capture := range captures {
		result[index] = capture.Runtime()
	}
	return result
}

func nativePredecessorCaptures(
	input PrepareInput,
	members []releaserender.ReleaseTaskRenderMember,
) []etcd.BlueprintNativePredecessorCapture {
	result := make([]etcd.BlueprintNativePredecessorCapture, 0, len(members))
	for _, member := range members {
		if captured, found := input.Workloads.predecessors[member.Render.ServiceName]; found {
			result = append(result, captured.native)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ServiceID < result[j].ServiceID })
	return result
}
