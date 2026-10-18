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

// ExecuteVolumeWork requires the caller's exclusive attempt/process ownership.
// Reconnection stops only this exact helper before read-only effect inspection.
func (executor *Executor) ExecuteVolumeWork(ctx context.Context, volumeRoot string, request *agentpb.VolumeWorkHelperRequest) (*agentpb.VolumeWorkHelperResponse, error) {
	if executor == nil || executor.engine == nil || ctx == nil {
		return nil, errs.New(errs.KindInternal, "Volume helper container is not configured")
	}
	input, err := executionplan.OpenVolumeHelperRequest(request)
	if err != nil {
		return nil, err
	}
	scope, err := environmentpath.Parse(volumeRoot, input.Procedure.Candidate.AuthorizedVolumeDir)
	if err != nil || scope.EnvironmentID != input.Procedure.EnvironmentId {
		return nil, errs.New(errs.KindValidationFailed, "Volume helper directory belongs to another owner")
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
	name := "gp-volume-executor-" + input.Attempt.ID
	labels := releaseHelperLabels(input.Attempt)
	labels["io.groundplane.role"] = "volume-executor"
	if input.Recovery != nil {
		forward := input.Recovery.Forward
		priorLabels := releaseHelperLabels(forward)
		priorLabels["io.groundplane.role"] = "volume-executor"
		if err := executor.stopNamedHelper(bounded, "gp-volume-executor-"+forward.ID, priorLabels); err != nil {
			return nil, err
		}
	}
	if input.InspectOnly {
		if err := executor.stopNamedHelper(bounded, name, labels); err != nil {
			return nil, err
		}
	}
	framed, err := composehelper.MarshalVolumeWorkRequest(request)
	if err != nil {
		return nil, err
	}
	defer clear(framed)
	options := createOptions(executor.image, nil)
	options.Name, options.Config.Labels = name, labels
	options.Config.Cmd = []string{"volume-work-helper"}
	options.Config.Env = []string{environmentdirectoryhelper.VolumeRootEnv + "=" + volumeRoot}
	options.HostConfig.Mounts = append(options.HostConfig.Mounts, mount.Mount{
		Type: mount.TypeBind, Source: volumeRoot, Target: volumeRoot,
	})
	raw, err := executor.executeFrame(bounded, options, framed)
	if err != nil {
		return nil, err
	}
	defer clear(raw)
	return composehelper.ReadVolumeWorkResponse(bounded, bytes.NewReader(raw), input)
}
