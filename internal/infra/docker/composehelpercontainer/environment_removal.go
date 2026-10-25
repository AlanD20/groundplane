package composehelpercontainer

import (
	"bytes"
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/hierarchyexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/docker/composehelper"
	"github.com/AlanD20/groundplane/internal/infra/docker/environmentdirectoryhelper"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/moby/moby/api/types/mount"
)

func (executor *Executor) ExecuteEnvironmentRemoval(ctx context.Context, volumeRoot string,
	request *agentpb.EnvironmentRemovalHelperRequest,
) (*agentpb.EnvironmentRemovalHelperResponse, error) {
	if executor == nil || executor.engine == nil || ctx == nil {
		return nil, errs.New(errs.KindInternal, "Environment removal helper container is not configured")
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
		return nil, errs.New(
			errs.KindValidationFailed,
			"Environment removal helper directory belongs to another Environment",
		)
	}
	bounded, cancel := context.WithTimeout(ctx, 30*time.Second)
	if !request.InspectOnly {
		cancel()
		bounded, cancel = context.WithDeadline(ctx, input.Attempt.Deadline)
	}
	defer cancel()
	if err := bounded.Err(); err != nil {
		return nil, err
	}
	framed, err := composehelper.MarshalEnvironmentRemovalRequest(request)
	if err != nil {
		return nil, err
	}
	defer clear(framed)
	options := createOptions(executor.image, "environment-removal-helper", nil)
	options.Name = environmentRemovalHelperName(input.Attempt)
	options.Config.Labels = environmentRemovalHelperLabels(input.Attempt)
	options.Config.Env = []string{environmentdirectoryhelper.VolumeRootEnv + "=" + volumeRoot}
	options.HostConfig.Mounts = append(options.HostConfig.Mounts, mount.Mount{
		Type: mount.TypeBind, Source: volumeRoot, Target: volumeRoot,
	})
	if progress.Phase == hierarchyexecution.RemoveDirectory && !request.InspectOnly {
		// Workloads own their data under different UIDs. The descriptor-relative
		// remover still validates the captured root and never follows symlinks.
		options.HostConfig.CapAdd = []string{"DAC_OVERRIDE"}
	}
	raw, err := executor.executeFrame(bounded, options, framed)
	if err != nil {
		return nil, err
	}
	defer clear(raw)
	response, err := composehelper.ReadEnvironmentRemovalResponse(bounded, bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	if _, err := hierarchyexecution.DecodeEnvironmentHelperResponse(input, progress, request.InspectOnly, response); err != nil {
		return nil, err
	}
	return response, nil
}

func environmentRemovalHelperName(attempt workexecution.Attempt) string {
	if attempt.Direction == workexecution.Recovery {
		return "gp-environment-recovery-" + attempt.WorkItemID
	}
	return "gp-environment-removal-" + attempt.ID
}

func environmentRemovalHelperLabels(attempt workexecution.Attempt) map[string]string {
	labels := releaseHelperLabels(attempt)
	labels["io.groundplane.role"] = "environment-removal-executor"
	if attempt.Direction == workexecution.Recovery {
		labels["io.groundplane.role"] = "environment-recovery-executor"
	}
	return labels
}

// StopEnvironmentHelpers is restricted to the original forward helper and the
// stable helper for this captured cleanup. The caller holds the process lease
// and excludes any current execution of that work item before invoking it.
func (executor *Executor) StopEnvironmentHelpers(ctx context.Context, forward, recovery workexecution.Attempt) error {
	if executor == nil || executor.engine == nil || ctx == nil || forward.Validate() != nil ||
		recovery.Validate() != nil ||
		forward.Direction != workexecution.Forward ||
		forward.FinishedAt == nil ||
		forward.Outcome == workexecution.Succeeded ||
		recovery.Direction != workexecution.Recovery ||
		recovery.FinishedAt != nil ||
		forward.TaskID != recovery.TaskID ||
		forward.WorkItemID != recovery.WorkItemID ||
		forward.Input != recovery.Input ||
		forward.Number >= recovery.Number ||
		forward.Executor.HostID != recovery.Executor.HostID {
		return errs.New(
			errs.KindValidationFailed,
			"Environment helper cleanup requires its captured failed execution and recovery",
		)
	}
	if err := executor.stopNamedHelper(ctx, environmentRemovalHelperName(forward), environmentRemovalHelperLabels(forward)); err != nil {
		return err
	}
	return executor.stopNamedHelper(
		ctx,
		environmentRemovalHelperName(recovery),
		environmentRemovalHelperLabels(recovery),
	)
}
