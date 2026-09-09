package composehelper

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func volumeRemovalArtifact(
	plan *agentpb.ExecutionPlan,
	remove *agentpb.ManagedVolumeRemove,
) (*agentpb.ComposeArtifact, *agentpb.ComposeVolume, error) {
	var source *agentpb.ComposeArtifact
	var target *agentpb.ComposeVolume
	for _, artifact := range plan.GetArtifacts() {
		for _, volume := range artifact.GetVolumes() {
			if volume.GetVolumeId() != remove.GetVolumeId() {
				continue
			}
			if source != nil || volume.DockerName != remove.DockerName || artifact.AuthorizedVolumeDir == "" ||
				artifact.OwnerKind != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT {
				return nil, nil, errs.New(errs.KindValidationFailed, "managed Volume removal artifact is ambiguous")
			}
			source, target = artifact, volume
		}
	}
	if source == nil {
		return nil, nil, errs.New(errs.KindValidationFailed, "managed Volume removal artifact is absent")
	}
	return source, target, nil
}

// Check every retained managed container in the Environment, including stopped
// containers and old deployment slots. Docker volume absence alone says nothing
// about a container retaining the same filesystem source as a direct bind.
func proveVolumeConsumersDetached(ctx context.Context, taskRunner runner.Runner,
	artifact *agentpb.ComposeArtifact, volume *agentpb.ComposeVolume,
) error {
	listed, err := taskRunner.Run(ctx, runner.RunCmdOpts{
		Name: DockerExecutable, Args: []string{"container", "ls", "--all", "--quiet", "--no-trunc",
			"--filter", "label=com.groundplane.environment-id=" + artifact.OwnerId},
		Dir: WorkDirectory, Env: append([]string(nil), fixedEnvironment...), ReplaceEnv: true, CaptureLimitBytes: 1024 * 1024,
	})
	if err != nil || listed.ExitCode != 0 {
		return errs.New(errs.KindRequestFailed, "Volume consumer inventory is unavailable")
	}
	device := filepath.Join(artifact.AuthorizedVolumeDir, volume.ComposeName)
	seen := make(map[string]bool)
	for _, id := range strings.Fields(string(listed.Stdout)) {
		if !validComponentContainerID(id) || seen[id] {
			return errs.New(errs.KindStateConflict, "Volume consumer inventory is invalid")
		}
		seen[id] = true
		inspected, err := taskRunner.Run(ctx, runner.RunCmdOpts{
			Name: DockerExecutable, Args: []string{"container", "inspect", "--format", "{{json .Mounts}}", id},
			Dir: WorkDirectory, Env: append([]string(nil), fixedEnvironment...), ReplaceEnv: true, CaptureLimitBytes: 1024 * 1024,
		})
		var mounts []struct{ Name, Source string }
		if err != nil || inspected.ExitCode != 0 || json.Unmarshal(inspected.Stdout, &mounts) != nil || mounts == nil {
			return errs.New(errs.KindRequestFailed, "Volume consumer mounts could not be inspected")
		}
		for _, mount := range mounts {
			if mount.Source != "" && (!filepath.IsAbs(mount.Source) || filepath.Clean(mount.Source) != mount.Source) {
				return errs.New(errs.KindStateConflict, "Volume consumer mount source is invalid")
			}
			if mount.Name == volume.DockerName || (mount.Source != "" && (mount.Source == device ||
				strings.HasPrefix(mount.Source, device+"/") || strings.HasPrefix(device, strings.TrimSuffix(mount.Source, "/")+"/"))) {
				return errs.New(errs.KindResourceInUse, "a retained Environment container still mounts the Volume")
			}
		}
	}
	return nil
}
