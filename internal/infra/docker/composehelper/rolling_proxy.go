package composehelper

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func executeRollingProxy(ctx context.Context, taskRunner runner.Runner,
	request *agentpb.ComposeHelperRequest, artifact *agentpb.ComposeArtifact, step *agentpb.ExecutionStep,
) (*agentpb.ComposeHelperResponse, error) {
	serviceID := step.GetRollingTrafficSwitch().GetServiceId()
	final := step.GetRollingReleaseAcknowledge() != nil
	if final {
		serviceID = step.GetRollingReleaseAcknowledge().GetServiceId()
	}
	member, err := executionplan.RollingMember(request.GetPlan(), serviceID)
	if err != nil {
		return nil, err
	}
	ordinal := step.GetRollingTrafficSwitch().GetChangeOrdinal()
	if final {
		ordinal = uint32(len(member.GetRolling().GetChanges()))
	}
	if !member.GetRolling().GetHasProxy() || ordinal == 0 || int(ordinal) > len(member.GetRolling().GetChanges()) {
		return nil, errs.New(
			errs.KindValidationFailed,
			"rolling proxy switch does not identify a captured traffic change",
		)
	}
	proxy, err := servingPredecessorProxy(artifact, member.GetServiceId())
	if err != nil || proxy == nil {
		return nil, errs.New(errs.KindValidationFailed, "rolling stable proxy is missing from the candidate artifact")
	}
	if proxy.GetProxyImplementation() != member.GetRolling().GetProxyImplementation() {
		return nil, errs.New(errs.KindValidationFailed, "captured rolling proxy has no compiled activation procedure")
	}
	beforeDigest := proxy.GetProxyConfigSha256()
	if len(member.GetRolling().GetPrior()) != 0 {
		prior := composeArtifactByID(request.GetPlan(), member.GetRolling().GetPrior()[0].GetArtifactId())
		priorProxy, err := servingPredecessorProxy(prior, member.GetServiceId())
		if err != nil {
			return nil, err
		}
		if priorProxy != nil {
			if priorProxy.GetComposeName() != proxy.GetComposeName() ||
				priorProxy.GetProxyImplementation() != proxy.GetProxyImplementation() {
				return nil, errs.New(errs.KindStateConflict, "rolling deployment must retain the same stable proxy")
			}
			beforeDigest = priorProxy.GetProxyConfigSha256()
			artifact, proxy = prior, priorProxy
		}
	}
	if ordinal > 1 {
		beforeDigest = member.GetRolling().GetChanges()[ordinal-2].GetProxyConfigSha256()
	}
	executionCtx, cancel := context.WithTimeout(ctx, time.Duration(request.GetTimeoutSeconds())*time.Second)
	defer cancel()
	containerID, err := ownedProxyContainer(executionCtx, taskRunner, artifact, proxy, nil)
	if err != nil {
		return nil, err
	}
	change := member.GetRolling().GetChanges()[ordinal-1]
	if !final {
		proven, err := observeRestorationProxy(
			executionCtx,
			taskRunner,
			containerID,
			proxy.GetProxyImplementation(),
			beforeDigest,
		)
		if err != nil || !proven {
			return nil, errs.New(
				errs.KindStateConflict,
				"rolling proxy does not have the captured preceding configuration",
			)
		}
		if failure, err := activateCapturedProxy(executionCtx, taskRunner, containerID, proxy.GetProxyImplementation(), change.GetProxyConfigJson()); err != nil ||
			failure != nil {
			return failure, err
		}
	}
	proven, err := awaitRestorationProxy(
		executionCtx,
		taskRunner,
		containerID,
		proxy.GetProxyImplementation(),
		change.GetProxyConfigSha256(),
	)
	if err != nil || !proven {
		return nil, errs.New(
			errs.KindStateConflict,
			"rolling proxy activation or restart configuration was not confirmed",
		)
	}
	response := completedResponse()
	response.RollingProxyEvidence = &agentpb.RollingProxyEvidence{
		ServiceId: member.GetServiceId(), ChangeOrdinal: ordinal, ProxyGeneration: change.GetProxyGeneration(),
		ConfigSha256: slices.Clone(change.GetProxyConfigSha256()),
	}
	return response, nil
}

// Resolve Compose's exact singleton, then check ownership and actual image
// before using its immutable container ID for activation and both observations.
func ownedProxyContainer(ctx context.Context, taskRunner runner.Runner,
	artifact *agentpb.ComposeArtifact, proxy *agentpb.ComposeService, priorArtifact *agentpb.ComposeArtifact,
) (string, error) {
	base, cleanup, err := releaseComposeBase(artifact)
	if err != nil {
		return "", err
	}
	defer cleanup()
	listed, failure, err := runWithComposeBase(
		ctx,
		taskRunner,
		base,
		nil,
		"ps",
		"--all",
		"--quiet",
		proxy.GetComposeName(),
	)
	if err != nil || failure != nil {
		return "", errs.New(errs.KindRequestFailed, "captured proxy container could not be located")
	}
	identities := strings.Fields(string(listed.Stdout))
	if len(identities) != 1 || !validComponentContainerID(identities[0]) {
		return "", errs.New(errs.KindStateConflict, "activation requires exactly one captured stable proxy container")
	}
	id := identities[0]
	result, err := taskRunner.Run(ctx, runner.RunCmdOpts{
		Name: DockerExecutable, Args: []string{"container", "inspect", "--format", "{{json .Config.Labels}}", id},
		Dir: WorkDirectory, Env: slices.Clone(fixedEnvironment), ReplaceEnv: true, CaptureLimitBytes: maximumComponentConfig,
	})
	labels := map[string]string{}
	if err != nil || result.ExitCode != 0 || json.Unmarshal(result.Stdout, &labels) != nil ||
		labels["com.docker.compose.project"] != artifact.GetProjectName() ||
		labels["com.docker.compose.service"] != proxy.GetComposeName() || labels["com.groundplane.release-id"] != "" {
		return "", errs.New(errs.KindStateConflict, "proxy ownership differs from the captured Service")
	}
	selected := proxy
	if !hasAllExpectedLabels(labels, selected.GetExpectedLabels()) {
		// A target-only switch retains the original proxy container and its
		// ownership labels. Only that exact sealed predecessor is admissible;
		// matching the name alone must never authorize execution in a container.
		prior, priorErr := servingPredecessorProxy(priorArtifact, proxy.GetServiceId())
		if priorErr != nil || prior == nil || priorArtifact.GetProjectName() != artifact.GetProjectName() ||
			prior.GetComposeName() != proxy.GetComposeName() || prior.GetProxyImplementation() != proxy.GetProxyImplementation() ||
			!hasAllExpectedLabels(labels, prior.GetExpectedLabels()) {
			return "", errs.New(
				errs.KindStateConflict,
				"proxy is neither the captured candidate nor its exact predecessor",
			)
		}
		selected = prior
	}
	if err := inspectRestorationImage(ctx, taskRunner, id, selected); err != nil {
		return "", err
	}
	return id, nil
}
