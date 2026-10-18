package composehelper

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/componentexecution"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func reconcileComponentRuntime(ctx context.Context, taskRunner runner.Runner, input componentexecution.HelperInput) error {
	inventory, err := inspectComponentWork(ctx, taskRunner, input)
	if err != nil {
		return err
	}
	if componentWorkReady(inventory.Candidate) && len(inventory.Prior) == 0 {
		return nil
	}
	// Switching providers can change the Compose name. Stop only the exact
	// captured predecessor, never `down` the Environment's Compose project.
	for _, container := range inventory.Prior {
		if err := removeComponentContainer(ctx, taskRunner, container.ID); err != nil {
			return err
		}
	}
	return applySingletonArtifact(ctx, taskRunner, input.Procedure.Candidate)
}

func removeComponentRuntime(ctx context.Context, taskRunner runner.Runner, input componentexecution.HelperInput) error {
	inventory, err := inspectComponentWork(ctx, taskRunner, input)
	if err != nil {
		return err
	}
	for _, container := range append(inventory.Candidate, inventory.Prior...) {
		if err := removeComponentContainer(ctx, taskRunner, container.ID); err != nil {
			return err
		}
	}
	remaining, err := inspectComponentWork(ctx, taskRunner, input)
	if err != nil {
		return err
	}
	if len(remaining.Candidate)+len(remaining.Prior) != 0 {
		return errs.New(errs.KindStateConflict, "Component containers remain after removal")
	}
	return nil
}

func applySingletonArtifact(ctx context.Context, taskRunner runner.Runner, artifact *agentpb.ComposeArtifact) error {
	if artifact == nil || len(artifact.Services) != 1 || artifact.Services[0] == nil {
		return errs.New(errs.KindValidationFailed, "runtime operation requires one captured Compose service")
	}
	input, err := composeCLIInput(artifact.CanonicalYaml)
	if err != nil {
		return err
	}
	base := []string{"compose", "--project-name", artifact.ProjectName, "--project-directory", composeProjectDirectory(artifact), "--file", "-"}
	for _, suffix := range [][]string{{"config", "--quiet"}, {"up", "--detach", "--no-deps", "--pull", "never", "--", artifact.Services[0].ComposeName}} {
		result, err := taskRunner.Run(ctx, runner.RunCmdOpts{Name: DockerExecutable, Args: append(slices.Clone(base), suffix...),
			Dir: composeProjectDirectory(artifact), Env: slices.Clone(fixedEnvironment), ReplaceEnv: true, Stdin: input, CaptureLimitBytes: 32 * 1024})
		clear(result.Stdout)
		clear(result.Stderr)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			return errs.Wrap(errs.KindInternal, err)
		}
		if result.ExitCode != 0 {
			return errs.New(errs.KindRequestFailed, "captured Compose configuration or startup failed")
		}
	}
	return nil
}

func removeComponentContainer(ctx context.Context, taskRunner runner.Runner, id string) error {
	if !validComponentContainerID(id) {
		return invalidComponentInventory()
	}
	for _, args := range [][]string{{"container", "stop", "--time", "30", "--", id}, {"container", "rm", "--", id}} {
		if _, err := runComponentWork(ctx, taskRunner, args...); err != nil {
			return err
		}
	}
	return nil
}
