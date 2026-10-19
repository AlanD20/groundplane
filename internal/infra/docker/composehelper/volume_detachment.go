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

// Check every retained managed container in the Environment, including stopped
// containers and old deployment slots. Docker volume absence alone says nothing
// about a container retaining the same filesystem source as a direct bind.
func proveVolumeConsumersDetached(ctx context.Context, taskRunner runner.Runner,
	artifact *agentpb.ComposeArtifact, volume *agentpb.ComposeVolume,
) error {
	detached, err := volumeConsumersDetached(ctx, taskRunner, artifact, volume)
	if err != nil {
		return err
	}
	if !detached {
		return errs.New(errs.KindResourceInUse, "a retained Environment container still mounts the Volume")
	}
	return nil
}

func volumeConsumersDetached(ctx context.Context, taskRunner runner.Runner,
	artifact *agentpb.ComposeArtifact, volume *agentpb.ComposeVolume,
) (bool, error) {
	listed, err := taskRunner.Run(ctx, runner.RunCmdOpts{
		Name: DockerExecutable, Args: []string{"container", "ls", "--all", "--quiet", "--no-trunc",
			"--filter", "label=com.groundplane.environment-id=" + artifact.OwnerId},
		Dir: WorkDirectory, Env: append([]string(nil), fixedEnvironment...), ReplaceEnv: true, CaptureLimitBytes: 1024 * 1024,
	})
	if err != nil || listed.ExitCode != 0 {
		return false, errs.New(errs.KindRequestFailed, "Volume consumer inventory is unavailable")
	}
	device := filepath.Join(artifact.AuthorizedVolumeDir, volume.ComposeName)
	seen := make(map[string]bool)
	for _, id := range strings.Fields(string(listed.Stdout)) {
		if !validComponentContainerID(id) || seen[id] {
			return false, errs.New(errs.KindStateConflict, "Volume consumer inventory is invalid")
		}
		seen[id] = true
		inspected, err := taskRunner.Run(ctx, runner.RunCmdOpts{
			Name: DockerExecutable, Args: []string{"container", "inspect", "--format", "{{json .Mounts}}", id},
			Dir: WorkDirectory, Env: append([]string(nil), fixedEnvironment...), ReplaceEnv: true, CaptureLimitBytes: 1024 * 1024,
		})
		var mounts []struct{ Name, Source string }
		if err != nil || inspected.ExitCode != 0 || json.Unmarshal(inspected.Stdout, &mounts) != nil || mounts == nil {
			return false, errs.New(errs.KindRequestFailed, "Volume consumer mounts could not be inspected")
		}
		for _, mount := range mounts {
			if mount.Source != "" && (!filepath.IsAbs(mount.Source) || filepath.Clean(mount.Source) != mount.Source) {
				return false, errs.New(errs.KindStateConflict, "Volume consumer mount source is invalid")
			}
			if mount.Name == volume.DockerName || (mount.Source != "" && (mount.Source == device ||
				strings.HasPrefix(mount.Source, device+"/") || strings.HasPrefix(device, strings.TrimSuffix(mount.Source, "/")+"/"))) {
				return false, nil
			}
		}
	}
	return true, nil
}
