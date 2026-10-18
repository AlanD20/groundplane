package composehelpercontainer

import (
	"bytes"
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/removalexecution"
	"github.com/AlanD20/groundplane/internal/infra/docker/composehelper"
	"github.com/AlanD20/groundplane/internal/infra/docker/environmentdirectoryhelper"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/moby/moby/api/types/mount"
)

func (executor *Executor) ExecuteRemovalRecovery(ctx context.Context, root string, request *agentpb.ServiceRemovalRecoveryHelperRequest) (*agentpb.ServiceRemovalRecoveryHelperResponse, error) {
	if executor == nil || executor.engine == nil || ctx == nil {
		return nil, errs.New(errs.KindInternal, "Service removal recovery helper is not configured")
	}
	input, err := executionplan.OpenRemovalRecoveryHelperRequest(request)
	if err != nil {
		return nil, err
	}
	if input.Progress.Phase != removalexecution.InspectRuntime && input.Progress.Phase != removalexecution.RestoreRuntime && input.Progress.Phase != removalexecution.VerifyRestoration {
		return nil, errs.New(errs.KindValidationFailed, "Service removal runtime helper cannot execute this recovery phase")
	}
	for _, member := range input.Procedure.Members {
		scope, err := environmentpath.Parse(root, member.Artifact.AuthorizedVolumeDir)
		if err != nil || scope.EnvironmentID != input.Procedure.EnvironmentId {
			return nil, errs.New(errs.KindValidationFailed, "Service removal recovery directory belongs to another Environment")
		}
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
	// The work/input owner is stable across cleanup attempts. A subsequent
	// recovery can therefore join its exact orphan helper without enumerating
	// diagnostic history or adopting the previous attempt identity.
	name := "gp-removal-recovery-" + input.Attempt.WorkItemID
	labels := releaseHelperLabels(input.Attempt)
	labels["io.groundplane.role"] = "service-removal-recovery"
	// A reconnect inspection first joins the exact helper from this attempt.
	// The independently acknowledged quiescence phase handles prior attempts.
	if input.InspectOnly {
		if err := executor.stopNamedHelper(bounded, name, labels); err != nil {
			return nil, err
		}
	}
	framed, err := composehelper.MarshalRemovalRecoveryRequest(request)
	if err != nil {
		return nil, err
	}
	defer clear(framed)
	options := createOptions(executor.image, nil)
	options.Name, options.Config.Labels = name, labels
	options.Config.Cmd = []string{"service-removal-recovery-helper"}
	options.Config.Env = []string{environmentdirectoryhelper.VolumeRootEnv + "=" + root}
	options.HostConfig.Mounts = append(options.HostConfig.Mounts, mount.Mount{Type: mount.TypeBind, Source: root, Target: root})
	raw, err := executor.executeFrame(bounded, options, framed)
	if err != nil {
		return nil, err
	}
	defer clear(raw)
	return composehelper.ReadRemovalRecoveryResponse(bounded, bytes.NewReader(raw), input)
}
