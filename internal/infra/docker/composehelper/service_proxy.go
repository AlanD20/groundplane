package composehelper

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func releaseComposeBase(artifact *agentpb.ComposeArtifact) ([]string, func(), error) {
	input, err := composeCLIInput(artifact.GetCanonicalYaml())
	if err != nil {
		return nil, nil, err
	}
	file, err := os.CreateTemp(WorkDirectory, "release-topology-*.yaml")
	if err != nil {
		return nil, nil, errs.Wrap(errs.KindInternal, err)
	}
	path := file.Name()
	cleanup := func() { _ = os.Remove(path) }
	if err := file.Chmod(0o600); err != nil {
		file.Close()
		cleanup()
		return nil, nil, errs.Wrap(errs.KindInternal, err)
	}
	if _, err := file.Write(input); err != nil {
		file.Close()
		cleanup()
		return nil, nil, errs.Wrap(errs.KindInternal, err)
	}
	if err := file.Close(); err != nil {
		cleanup()
		return nil, nil, errs.Wrap(errs.KindInternal, err)
	}
	base := []string{
		"compose",
		"--project-name",
		artifact.GetProjectName(),
		"--project-directory",
		WorkDirectory,
		"--file",
		filepath.Clean(path),
	}
	return base, cleanup, nil
}

func runWithComposeBase(
	ctx context.Context,
	taskRunner runner.Runner,
	base []string,
	stdin []byte,
	args ...string,
) (runner.Result, *agentpb.ComposeHelperResponse, error) {
	result, runErr := taskRunner.Run(ctx, runner.RunCmdOpts{
		Name: DockerExecutable, Args: append(slices.Clone(base), args...), Dir: WorkDirectory,
		Env: slices.Clone(
			fixedEnvironment,
		), ReplaceEnv: true, Stdin: slices.Clone(stdin), CaptureLimitBytes: maximumComponentConfig,
	})
	if ctx.Err() != nil {
		return runner.Result{}, nil, ctx.Err()
	}
	if result.ExitCode < 0 || result.ExitCode > math.MaxInt32 {
		return runner.Result{}, nil, errs.New(
			errs.KindInternal,
			"release topology command returned an invalid exit code",
		)
	}
	if runErr != nil || result.ExitCode != 0 {
		if runErr != nil && result.ExitCode == 0 {
			return runner.Result{}, nil, errs.Wrap(errs.KindInternal, runErr)
		}
		exitCode := result.ExitCode
		if exitCode == 0 {
			exitCode = 1
		}
		return result, &agentpb.ComposeHelperResponse{
			Schema:     SchemaVersion,
			Outcome:    agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_FAILED,
			ExitCode:   int32(exitCode),
			Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPOSE_FAILED,
		}, nil
	}
	return result, nil, nil
}

func canonicalProxyJSON(value []byte) ([]byte, error) {
	return jcs.Canonicalize(value)
}

func failedProxyResponse() *agentpb.ComposeHelperResponse {
	return &agentpb.ComposeHelperResponse{
		Schema: SchemaVersion, Outcome: agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_FAILED,
		ExitCode: 1, Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPONENT_ACTIVATION_FAILED,
	}
}
