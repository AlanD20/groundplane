package composehelpercontainer

import (
	"bytes"
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/environmentexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/docker/composehelper"
	"github.com/AlanD20/groundplane/internal/infra/docker/environmentdirectoryhelper"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/moby/moby/api/types/mount"
)

func provisionHelperName(attempt workexecution.Attempt) string {
	if attempt.Direction == workexecution.Recovery {
		return "gp-environment-provision-recovery-" + attempt.WorkItemID
	}
	return "gp-environment-provision-" + attempt.ID
}

func provisionHelperLabels(attempt workexecution.Attempt) map[string]string {
	labels := releaseHelperLabels(attempt)
	labels["io.groundplane.role"] = "environment-provision-executor"
	if attempt.Direction == workexecution.Recovery {
		labels["io.groundplane.role"] = "environment-provision-recovery"
	}
	return labels
}

// ExecuteEnvironmentProvision runs a bounded helper with only the configured
// volume root mounted. Inspection first stops the exact captured orphan; no
// application container, data Volume or unrelated helper is addressed.
func (executor *Executor) ExecuteEnvironmentProvision(
	ctx context.Context,
	volumeRoot string,
	request *agentpb.EnvironmentProvisionHelperRequest,
) (*agentpb.EnvironmentProvisionHelperResponse, error) {
	if executor == nil || executor.engine == nil || ctx == nil {
		return nil, errs.New(errs.KindInternal, "environment provisioning helper is unavailable")
	}
	input, err := environmentexecution.OpenHelper(request)
	if err != nil {
		return nil, err
	}
	if input.Creation.VolumeRoot != volumeRoot {
		return nil, errs.New(errs.KindStateConflict, "environment provisioning root differs from host policy")
	}
	bounded, cancel := context.WithTimeout(ctx, 60*time.Second)
	if !request.InspectOnly {
		cancel()
		bounded, cancel = context.WithDeadline(ctx, input.Attempt.Deadline)
	}
	defer cancel()
	if request.InspectOnly {
		if source := input.RecoverySource; source != nil {
			if err := executor.stopNamedHelper(bounded, provisionHelperName(*source), provisionHelperLabels(*source)); err != nil {
				return nil, err
			}
		}
		if err := executor.stopNamedHelper(bounded, provisionHelperName(input.Attempt), provisionHelperLabels(input.Attempt)); err != nil {
			return nil, err
		}
	}
	framed, err := composehelper.MarshalEnvironmentProvisionRequest(request)
	if err != nil {
		return nil, err
	}
	defer clear(framed)
	options := createOptions(executor.image, "environment-provision-helper", nil)
	options.Name, options.Config.Labels = provisionHelperName(input.Attempt), provisionHelperLabels(input.Attempt)
	options.Config.Env = []string{environmentdirectoryhelper.VolumeRootEnv + "=" + volumeRoot}
	// Directory provisioning does not require Docker access inside its helper.
	options.HostConfig.Mounts = []mount.Mount{{Type: mount.TypeBind, Source: volumeRoot, Target: volumeRoot}}
	raw, err := executor.executeFrame(bounded, options, framed)
	if err != nil {
		return nil, err
	}
	defer clear(raw)
	response, err := composehelper.ReadEnvironmentProvisionResponse(bounded, bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	if err := environmentexecution.ValidateHelperResponse(input, request.InspectOnly, response); err != nil {
		return nil, err
	}
	return response, nil
}
