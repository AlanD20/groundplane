package composehelper

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/hierarchyexecution"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/infra/docker/environmentdirectoryhelper"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func ExecuteEnvironmentRemoval(ctx context.Context, taskRunner runner.Runner, volumeRoot string,
	request *agentpb.EnvironmentRemovalHelperRequest,
) (*agentpb.EnvironmentRemovalHelperResponse, error) {
	if ctx == nil || taskRunner == nil {
		return nil, errs.New(errs.KindInternal, "Environment removal helper dependencies are missing")
	}
	input, progress, err := hierarchyexecution.OpenEnvironmentHelperRequest(request)
	if err != nil {
		return nil, err
	}
	scope, err := environmentpath.Parse(volumeRoot, input.Removal.ExpectedVolumeDir)
	if err != nil {
		return nil, err
	}
	if scope.EnvironmentID != input.Removal.EnvironmentId {
		return nil, foreignEnvironmentRemoval()
	}
	bounded := ctx
	cancel := func() {}
	if !request.InspectOnly {
		bounded, cancel = context.WithDeadline(ctx, input.Attempt.Deadline)
	}
	defer cancel()
	if err := bounded.Err(); err != nil {
		return nil, err
	}
	directory := environmentdirectoryhelper.Creator{}
	switch progress.Phase {
	case hierarchyexecution.RemoveRuntime:
		if err := removeEnvironmentRuntime(bounded, taskRunner, input, request.InspectOnly); err != nil {
			return nil, err
		}
	case hierarchyexecution.RemoveDirectory:
		// Check live absence again before touching data. A previous runtime
		// checkpoint cannot authorize deletion beneath a recreated workload.
		if err := removeEnvironmentRuntime(bounded, taskRunner, input, true); err != nil {
			return nil, err
		}
		if !request.InspectOnly {
			if err := directory.Remove(bounded, volumeRoot, input.Removal.ExpectedVolumeDir); err != nil {
				return nil, err
			}
		}
	default:
		return nil, errs.New(errs.KindValidationFailed, "Environment removal helper stage is invalid")
	}
	absent, err := directory.Absent(bounded, volumeRoot, input.Removal.ExpectedVolumeDir)
	if err != nil {
		return nil, err
	}
	response := &agentpb.EnvironmentRemovalHelperResponse{
		EnvironmentId: input.Removal.EnvironmentId, CapturedSource: proto.CloneOf(input.Removal.CapturedSource),
		RuntimeAbsent: true, DirectoryAbsent: absent, ObservedAt: timestamppb.New(time.Now().UTC()), Inspection: request.InspectOnly,
	}
	if _, err := hierarchyexecution.DecodeEnvironmentHelperResponse(input, progress, request.InspectOnly, response); err != nil {
		return nil, err
	}
	return response, nil
}
