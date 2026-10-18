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
	return labels
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
	name, labels := "gp-backing-provision-"+attempt.ID, backingProvisionHelperLabels(attempt)
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
	options := createOptions(executor.image, input.Assignment.Procedure.Runtime)
	options.Name, options.Config.Labels = name, labels
	options.Config.Cmd = []string{"backing-provision-helper"}
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
func (executor *Executor) StopBackingProvision(ctx context.Context, request *agentpb.BackingProvisionHelperRequest) error {
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
		if err := executor.stopNamedHelper(ctx, "gp-backing-provision-"+attempt.ID, backingProvisionHelperLabels(attempt)); err != nil {
			return err
		}
	}
	return nil
}
