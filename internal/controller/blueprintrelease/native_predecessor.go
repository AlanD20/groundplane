package blueprintrelease

import (
	"context"
	"sort"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/servicelifecycle"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"google.golang.org/protobuf/proto"
)

func (service *Service) captureNativePredecessor(
	ctx context.Context,
	scope etcd.ReleasePlanningScope,
	captured predecessorSnapshot,
) (predecessorSnapshot, error) {
	captured.native = etcd.BlueprintNativePredecessorCapture{
		ServiceID:         captured.planning.Service.Record.Desired.ID,
		FixedReadRevision: scope.ReadRevision, ProjectionRevision: captured.planning.ProjectionRevision,
	}
	if captured.serving == nil {
		return captured, nil
	}
	authority, err := servicelifecycle.CaptureRelease(
		ctx,
		service.ledger,
		captured.applied,
		scope.Environment.Record.ID,
		captured.native.ServiceID,
	)
	if err != nil {
		return predecessorSnapshot{}, err
	}
	artifacts, err := service.plans.RenderRetainedServiceRuntime(ctx, authority)
	if err != nil {
		return predecessorSnapshot{}, err
	}
	if len(artifacts) < 1 || len(artifacts) > 2 || (len(artifacts) == 2) != (authority.RetainedPrior != nil) {
		return predecessorSnapshot{}, errs.New(
			errs.KindStateConflict,
			"Blueprint native predecessor rendering is incomplete",
		)
	}
	captured.native.Serving = &authority
	for _, artifact := range artifacts {
		artifact.ArtifactId = ids.New(ids.KindConfig)
	}
	captured.native.CurrentArtifact, err = (proto.MarshalOptions{Deterministic: true}).Marshal(artifacts[0])
	if err != nil {
		return predecessorSnapshot{}, errs.Wrap(errs.KindInternal, err)
	}
	if len(artifacts) == 2 {
		captured.native.RetainedPriorArtifact, err = (proto.MarshalOptions{Deterministic: true}).Marshal(artifacts[1])
		if err != nil {
			return predecessorSnapshot{}, errs.Wrap(errs.KindInternal, err)
		}
	}
	return captured, nil
}

func nativePredecessors(input PrepareInput, members []etcd.ReleaseTaskRenderMember) []etcd.BlueprintNativePredecessor {
	captures := nativePredecessorCaptures(input, members)
	result := make([]etcd.BlueprintNativePredecessor, len(captures))
	for index, capture := range captures {
		result[index] = capture.Runtime()
	}
	return result
}

func nativePredecessorCaptures(
	input PrepareInput,
	members []etcd.ReleaseTaskRenderMember,
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
