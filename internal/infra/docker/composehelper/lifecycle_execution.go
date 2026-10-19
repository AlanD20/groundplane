package composehelper

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/lifecycleexecution"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// ExecuteLifecycleMember owns only Compose members. Custom hooks execute through
// their recorded Docker-exec owner, not as an untracked helper subprocess.
func ExecuteLifecycleMember(
	ctx context.Context,
	taskRunner runner.Runner,
	volumeRoot string,
	request *agentpb.ServiceLifecycleHelperRequest,
) (*agentpb.ServiceLifecycleHelperResponse, error) {
	if ctx == nil || taskRunner == nil {
		return nil, errs.New(errs.KindInternal, "Service lifecycle helper dependencies are missing")
	}
	input, err := executionplan.OpenLifecycleHelperRequest(request)
	if err != nil {
		return nil, err
	}
	if input.Stage.Expected == lifecycleexecution.HookSucceeded {
		return nil, errs.New(errs.KindValidationFailed, "Custom hook execution requires its recorded Docker exec")
	}
	member, err := lifecycleMember(input)
	if err != nil {
		return nil, err
	}
	scope, err := environmentpath.Parse(volumeRoot, member.Artifact.AuthorizedVolumeDir)
	if err != nil || scope.EnvironmentID != input.Procedure.EnvironmentId {
		return nil, errs.New(errs.KindValidationFailed, "Service lifecycle directory belongs to another Environment")
	}
	bounded, cancel := context.WithTimeout(ctx, 15*time.Second)
	if !input.InspectOnly {
		cancel()
		bounded, cancel = context.WithDeadline(ctx, input.Attempt.Deadline)
	}
	defer cancel()
	if err := bounded.Err(); err != nil {
		return nil, err
	}
	response, err := inspectLifecycleMember(bounded, taskRunner, input)
	if err != nil {
		return nil, err
	}
	if input.InspectOnly || response.Satisfied {
		return response, executionplan.ValidateLifecycleHelperResponse(input, response)
	}
	if err := mutateLifecycleMember(bounded, taskRunner, input); err != nil {
		return nil, err
	}
	for {
		response, err = inspectLifecycleMember(bounded, taskRunner, input)
		if err != nil {
			return nil, err
		}
		if response.Satisfied {
			return response, executionplan.ValidateLifecycleHelperResponse(input, response)
		}
		if input.Stage.Expected != lifecycleexecution.Running {
			return nil, errs.New(
				errs.KindStateConflict,
				"Service containers still exist or run after the captured lifecycle action",
			)
		}
		// Waiting observes only; a slow healthcheck never triggers another up.
		timer := time.NewTimer(time.Second)
		select {
		case <-bounded.Done():
			timer.Stop()
			return nil, bounded.Err()
		case <-timer.C:
		}
	}
}
