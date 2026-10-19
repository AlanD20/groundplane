package composehelper

import (
	"context"
	"encoding/json"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

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
