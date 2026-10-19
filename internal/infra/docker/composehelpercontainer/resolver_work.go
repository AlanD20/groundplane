package composehelpercontainer

import (
	"bytes"
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/common/resolverexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/docker/composehelper"
	"github.com/AlanD20/groundplane/internal/infra/docker/hostresolutionhelper"
	"github.com/AlanD20/groundplane/internal/infra/docker/managedconfighelper"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
)

func resolverHelperName(attempt workexecution.Attempt) string {
	if attempt.Direction == workexecution.Recovery {
		return "gp-resolver-cleanup-" + attempt.WorkItemID
	}
	return "gp-resolver-" + attempt.ID
}

func resolverHelperLabels(attempt workexecution.Attempt) map[string]string {
	labels := releaseHelperLabels(attempt)
	labels["io.groundplane.role"] = "resolver-executor"
	if attempt.Direction == workexecution.Recovery {
		labels["io.groundplane.role"] = "resolver-cleanup"
	}
	return labels
}

func (executor *Executor) ExecuteResolver(
	ctx context.Context,
	request *agentpb.ResolverWorkHelperRequest,
) (*agentpb.ResolverWorkHelperResponse, error) {
	if executor == nil || executor.engine == nil || ctx == nil {
		return nil, errs.New(errs.KindInternal, "DNS resolver helper container is not configured")
	}
	input, err := resolverexecution.OpenHelperRequest(request)
	if err != nil {
		return nil, err
	}
	bounded, cancel := context.WithDeadline(ctx, input.Assignment.Attempt.Deadline)
	if input.InspectOnly {
		cancel()
		bounded, cancel = context.WithTimeout(ctx, 60*time.Second)
	}
	defer cancel()
	attempt := input.Assignment.Attempt
	if input.Assignment.Recovery != nil {
		if err := executor.StopResolver(bounded, request); err != nil {
			return nil, err
		}
	} else if input.InspectOnly {
		if err := executor.stopNamedHelper(bounded, resolverHelperName(attempt), resolverHelperLabels(attempt)); err != nil {
			return nil, err
		}
	}
	framed, err := composehelper.MarshalResolverRequest(request)
	if err != nil {
		return nil, err
	}
	defer clear(framed)
	options := createOptions(executor.image, "resolver-helper", nil)
	options.Name, options.Config.Labels = resolverHelperName(attempt), resolverHelperLabels(attempt)
	recovery := input.Assignment.Recovery != nil
	writeConfig := recovery ||
		!input.InspectOnly &&
			(input.Progress.Stage == resolverexecution.WriteConfiguration || input.Progress.Stage == resolverexecution.ApplyHostResolver)
	options.HostConfig.Mounts = append(options.HostConfig.Mounts, mount.Mount{Type: mount.TypeBind,
		Source: managedconfighelper.ManagedRoot, Target: managedconfighelper.ManagedRoot, ReadOnly: !writeConfig})
	hostDNS := recovery || input.Progress.Stage == resolverexecution.ApplyHostResolver ||
		input.Progress.Stage == resolverexecution.RestoreHostResolver
	if hostDNS {
		options.HostConfig.Mounts = append(
			options.HostConfig.Mounts,
			mount.Mount{
				Type:     mount.TypeBind,
				Source:   hostresolutionhelper.ResolverSourcePath,
				Target:   hostresolutionhelper.ResolverMountPath,
				ReadOnly: input.InspectOnly,
			},
			mount.Mount{
				Type:     mount.TypeBind,
				Source:   agentprotocol.StatePath,
				Target:   agentprotocol.StatePath,
				ReadOnly: input.InspectOnly,
			},
		)
		options.HostConfig.CapAdd = []string{"DAC_OVERRIDE"}
	}
	if recovery || input.Progress.Stage == resolverexecution.VerifyDNS ||
		input.Progress.Stage == resolverexecution.ApplyHostResolver {
		options.Config.NetworkDisabled = false
		options.HostConfig.NetworkMode = container.NetworkMode("host")
	}
	raw, err := executor.executeFrame(bounded, options, framed)
	if err != nil {
		return nil, err
	}
	defer clear(raw)
	return composehelper.ReadResolverResponse(bounded, bytes.NewReader(raw), input)
}

func (executor *Executor) StopResolver(ctx context.Context, request *agentpb.ResolverWorkHelperRequest) error {
	if executor == nil || executor.engine == nil || ctx == nil {
		return errs.New(errs.KindInternal, "DNS resolver helper cleanup is not configured")
	}
	input, err := resolverexecution.OpenHelperRequest(request)
	if err != nil {
		return err
	}
	attempts := []workexecution.Attempt{input.Assignment.Attempt}
	if source := input.Assignment.Recovery; source != nil {
		attempts = append(attempts, source.Forward)
	}
	for _, attempt := range attempts {
		if err := executor.stopNamedHelper(ctx, resolverHelperName(attempt), resolverHelperLabels(attempt)); err != nil {
			return err
		}
	}
	return nil
}
