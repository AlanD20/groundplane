package composehelper

import (
	"bytes"
	"context"
	"crypto/sha256"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// restorationProxyProven uses the exact container whose owner and actual
// managed image were checked by the inventory. It never returns authored
// configuration as though it were an observation of Caddy's active state.
func restorationProxyProven(
	ctx context.Context,
	taskRunner runner.Runner,
	containerID string,
	proxy *agentpb.ComposeService,
	compensate bool,
) (bool, error) {
	if !validComponentContainerID(containerID) || proxy == nil || len(proxy.GetProxyConfigSha256()) != sha256.Size {
		return false, errs.New(errs.KindValidationFailed, "restoration proxy identity is incomplete")
	}
	canonical, err := canonicalProxyJSON(proxy.GetProxyConfigJson())
	if err != nil || !bytes.Equal(canonical, proxy.GetProxyConfigJson()) {
		return false, errs.New(errs.KindValidationFailed, "restoration proxy configuration is not canonical")
	}
	digest := sha256.Sum256(canonical)
	if !bytes.Equal(digest[:], proxy.GetProxyConfigSha256()) {
		return false, errs.New(errs.KindValidationFailed, "restoration proxy configuration digest diverges")
	}
	proven, err := observeRestorationProxy(ctx, taskRunner, containerID, digest[:])
	if err != nil || proven || !compensate {
		return proven, err
	}
	result, err := taskRunner.Run(ctx, runner.RunCmdOpts{
		Name: DockerExecutable, Args: []string{"exec", "--interactive", containerID, "caddy", "reload", "--config", "-"},
		Dir: WorkDirectory, Env: slices.Clone(fixedEnvironment), ReplaceEnv: true,
		Stdin: slices.Clone(canonical), CaptureLimitBytes: maximumComponentConfig,
	})
	if err != nil || result.ExitCode != 0 {
		return false, errs.New(errs.KindRequestFailed, "restoration proxy reload failed")
	}
	return observeRestorationProxy(ctx, taskRunner, containerID, digest[:])
}

func observeRestorationProxy(
	ctx context.Context,
	taskRunner runner.Runner,
	containerID string,
	digest []byte,
) (bool, error) {
	result, err := taskRunner.Run(ctx, runner.RunCmdOpts{
		Name: DockerExecutable, Args: []string{"exec", containerID, "wget", "--quiet", "--output-document=-", "http://127.0.0.1:2019/config/"},
		Dir: WorkDirectory, Env: slices.Clone(fixedEnvironment), ReplaceEnv: true, CaptureLimitBytes: maximumComponentConfig,
	})
	if err != nil || result.ExitCode != 0 {
		return false, errs.New(errs.KindRequestFailed, "restoration proxy observation failed")
	}
	canonical, err := canonicalProxyJSON(result.Stdout)
	if err != nil {
		return false, errs.New(errs.KindStateConflict, "restoration proxy observation is invalid")
	}
	observed := sha256.Sum256(canonical)
	return bytes.Equal(observed[:], digest), nil
}
