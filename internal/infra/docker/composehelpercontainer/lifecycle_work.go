package composehelpercontainer

import (
	"bytes"
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/docker/composehelper"
	"github.com/AlanD20/groundplane/internal/infra/docker/environmentdirectoryhelper"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/moby/moby/api/types/mount"
)

// ExecuteLifecycleWork requires the caller's exclusive attempt/process ownership.
// Reconnection stops only this exact helper before read-only effect inspection.
func (executor *Executor) ExecuteLifecycleWork(ctx context.Context, volumeRoot string, request *agentpb.ServiceLifecycleHelperRequest) (*agentpb.ServiceLifecycleHelperResponse, error) {
	if executor == nil || executor.engine == nil || ctx == nil {
		return nil, errs.New(errs.KindInternal, "Service lifecycle helper container is not configured")
	}
	input, err := executionplan.OpenLifecycleHelperRequest(request)
	if err != nil {
		return nil, err
	}
	scope, err := environmentpath.Parse(volumeRoot, input.Procedure.Members[0].Artifact.AuthorizedVolumeDir)
	if err != nil || scope.EnvironmentID != input.Procedure.EnvironmentId {
		return nil, errs.New(errs.KindValidationFailed, "Service lifecycle helper directory belongs to another owner")
	}
	bounded, cancel := context.WithTimeout(ctx, 30*time.Second)
	if !input.InspectOnly {
		cancel()
		bounded, cancel = context.WithDeadline(ctx, input.Attempt.Deadline)
	}
	defer cancel()
	if err := bounded.Err(); err != nil {
		return nil, err
	}
	name := "gp-lifecycle-executor-" + input.Attempt.ID
	labels := releaseHelperLabels(input.Attempt)
	labels["io.groundplane.role"] = "lifecycle-executor"
	if input.InspectOnly {
		if input.Recovery != nil {
			forward := input.Recovery.Forward
			priorLabels := releaseHelperLabels(forward)
			priorLabels["io.groundplane.role"] = "lifecycle-executor"
			if err := executor.stopNamedHelper(bounded, "gp-lifecycle-executor-"+forward.ID, priorLabels); err != nil {
				return nil, err
			}
		}
		if err := executor.stopNamedHelper(bounded, name, labels); err != nil {
			return nil, err
		}
	}
	framed, err := composehelper.MarshalLifecycleWorkRequest(request)
	if err != nil {
		return nil, err
	}
	defer clear(framed)
	options := createOptions(executor.image, nil)
	options.Name, options.Config.Labels = name, labels
	options.Config.Cmd = []string{"lifecycle-work-helper"}
	options.Config.Env = []string{environmentdirectoryhelper.VolumeRootEnv + "=" + volumeRoot}
	options.HostConfig.Mounts = append(options.HostConfig.Mounts, mount.Mount{
		Type: mount.TypeBind, Source: volumeRoot, Target: volumeRoot,
	})
	raw, err := executor.executeFrame(bounded, options, framed)
	if err != nil {
		return nil, err
	}
	defer clear(raw)
	return composehelper.ReadLifecycleWorkResponse(bounded, bytes.NewReader(raw), input)
}
