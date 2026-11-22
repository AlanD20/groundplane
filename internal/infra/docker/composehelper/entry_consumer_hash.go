package composehelper

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Compose config --hash deliberately skips env_file resolution. Resolve first,
// then hash that model so verification includes the values used by compose up.
// The resolved model can contain credentials: keep it bounded and in memory.
func entryConsumerHash(ctx context.Context, taskRunner runner.Runner, artifact *agentpb.ComposeArtifact,
	service string,
) (runner.Result, error) {
	input, err := composeCLIInput(artifact.CanonicalYaml)
	if err != nil {
		return runner.Result{}, err
	}
	defer clear(input)
	resolved, err := runEntryCommand(ctx, taskRunner, runner.RunCmdOpts{
		Name: DockerExecutable,
		Args: []string{"compose", "--project-name", artifact.ProjectName,
			"--project-directory", composeProjectDirectory(artifact), "--file", "-",
			"config", "--format", "json"},
		Stdin: input, CaptureLimitBytes: 2 * executionplan.MaximumPlanBytes,
	})
	if err != nil {
		return runner.Result{}, err
	}
	defer clear(resolved.Stdout)
	model := &agentpb.ComposeArtifact{ProjectName: artifact.ProjectName,
		AuthorizedVolumeDir: artifact.AuthorizedVolumeDir, CanonicalYaml: resolved.Stdout}
	return runEntryCompose(ctx, taskRunner, model, "config", "--hash", service)
}
