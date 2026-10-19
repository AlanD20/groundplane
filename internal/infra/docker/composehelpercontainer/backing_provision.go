package composehelpercontainer

import (
	"bytes"
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backingprovision"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/docker/composehelper"
	"github.com/AlanD20/groundplane/internal/infra/docker/environmentdirectoryhelper"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/moby/moby/api/types/mount"
)

func backingProvisionHelperLabels(attempt workexecution.Attempt) map[string]string {
	labels := releaseHelperLabels(attempt)
	labels["io.groundplane.role"] = "backing-provision-executor"
	if attempt.Direction == workexecution.Recovery {
		labels["io.groundplane.role"] = "backing-provision-cleanup"
	}
	return labels
}

func backingProvisionHelperName(attempt workexecution.Attempt) string {
	if attempt.Direction == workexecution.Recovery {
		// Cleanup retries share exact work/input/host helper ownership, not
		// attempt execution identity. A new retry can join any older orphan.
		return "gp-backing-provision-cleanup-" + attempt.WorkItemID
	}
	return "gp-backing-provision-" + attempt.ID
}

func (executor *Executor) ExecuteBackingProvision(ctx context.Context, volumeRoot string,
	request *agentpb.BackingProvisionHelperRequest,
) (*agentpb.BackingProvisionHelperResponse, error) {
	if executor == nil || executor.engine == nil || ctx == nil {
		return nil, errs.New(errs.KindInternal, "backing creation helper container is not configured")
	}
	input, err := backingprovision.OpenHelperRequest(request)
	if err != nil {
		return nil, err
	}
	if input.Assignment.Procedure.VolumeRoot != volumeRoot {
		return nil, errs.New(errs.KindStateConflict, "backing creation root differs from host policy")
	}
	attempt := input.Assignment.Attempt
	bounded, cancel := context.WithDeadline(ctx, attempt.Deadline)
	if input.InspectOnly {
		cancel()
		bounded, cancel = context.WithTimeout(ctx, 30*time.Second)
	}
	defer cancel()
	if input.Assignment.Recovery != nil {
		if err := executor.StopBackingProvision(bounded, request); err != nil {
			return nil, err
		}
	}
	name, labels := backingProvisionHelperName(attempt), backingProvisionHelperLabels(attempt)
	if input.InspectOnly {
		if err := executor.stopNamedHelper(bounded, name, labels); err != nil {
			return nil, err
		}
	}
	framed, err := composehelper.MarshalBackingProvisionRequest(request)
	if err != nil {
		return nil, err
	}
	defer clear(framed)
	options := createOptions(executor.image, "backing-provision-helper", input.Assignment.Procedure.Runtime)
	if input.Assignment.Recovery != nil {
		// Cleanup needs only Docker inspection/termination. The directory
		// may never have been created; never mount or create it for cleanup.
		options = createOptions(executor.image, "backing-provision-helper", nil)
	}
	options.Name, options.Config.Labels = name, labels
	options.Config.Env = []string{environmentdirectoryhelper.VolumeRootEnv + "=" + volumeRoot}
	if input.Progress != nil && input.Progress.Stage == backingprovision.PrepareDirectories {
		// Directory-only helpers receive no Docker socket.
		options.HostConfig.Mounts = []mount.Mount{{Type: mount.TypeBind, Source: volumeRoot, Target: volumeRoot}}
	}
	raw, err := executor.executeFrame(bounded, options, framed)
	if err != nil {
		return nil, err
	}
	defer clear(raw)
	return composehelper.ReadBackingProvisionResponse(bounded, bytes.NewReader(raw), input)
}

// StopBackingProvision joins only helpers named by the captured attempt/source.
func (executor *Executor) StopBackingProvision(
	ctx context.Context,
	request *agentpb.BackingProvisionHelperRequest,
) error {
	if executor == nil || executor.engine == nil || ctx == nil {
		return errs.New(errs.KindInternal, "backing creation helper cleanup is not configured")
	}
	input, err := backingprovision.OpenHelperRequest(request)
	if err != nil {
		return err
	}
	attempts := []workexecution.Attempt{input.Assignment.Attempt}
	if source := input.Assignment.Recovery; source != nil {
		attempts = append(attempts, source.Forward)
	}
	for _, attempt := range attempts {
		if err := executor.stopNamedHelper(ctx, backingProvisionHelperName(attempt), backingProvisionHelperLabels(attempt)); err != nil {
			return err
		}
	}
	return nil
}
