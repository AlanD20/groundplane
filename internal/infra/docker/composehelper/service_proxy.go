package composehelper

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/common/serviceproxy"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func executeServiceProxy(
	ctx context.Context,
	taskRunner runner.Runner,
	timeout uint32,
	plan *agentpb.ExecutionPlan,
	artifact *agentpb.ComposeArtifact,
	step *agentpb.ExecutionStep,
) (*agentpb.ComposeHelperResponse, error) {
	executionCtx, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
	defer cancel()
	serviceID := proxyStepServiceID(step)
	proxyName := releaseProxyName(artifact, serviceID)
	if serviceID == "" || proxyName == "" {
		return nil, errs.New(errs.KindValidationFailed, "release proxy artifact is inconsistent")
	}
	if err := os.MkdirAll(WorkDirectory, 0o700); err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	file, err := os.CreateTemp(WorkDirectory, "release-*.yaml")
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	path := file.Name()
	defer os.Remove(path)
	if err := file.Chmod(0o600); err != nil {
		file.Close()
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	if _, err := file.Write(artifact.CanonicalYaml); err != nil {
		file.Close()
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	if err := file.Close(); err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	base := []string{
		"compose",
		"--project-name",
		artifact.ProjectName,
		"--project-directory",
		WorkDirectory,
		"--file",
		filepath.Clean(path),
	}
	run := func(stdin []byte, args ...string) (runner.Result, *agentpb.ComposeHelperResponse, error) {
		result, runErr := taskRunner.Run(executionCtx, runner.RunCmdOpts{
			Name: DockerExecutable, Args: append(slices.Clone(base), args...), Dir: WorkDirectory,
			Env: slices.Clone(
				fixedEnvironment,
			), ReplaceEnv: true, Stdin: slices.Clone(stdin), CaptureLimitBytes: maximumComponentConfig,
		})
		if executionCtx.Err() != nil {
			return runner.Result{}, nil, executionCtx.Err()
		}
		if result.ExitCode < 0 || result.ExitCode > math.MaxInt32 {
			return runner.Result{}, nil, errs.New(
				errs.KindInternal,
				"release proxy command returned an invalid exit code",
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
				Schema: SchemaVersion, Outcome: agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_FAILED,
				ExitCode: int32(
					exitCode,
				), Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPONENT_ACTIVATION_FAILED,
			}, nil
		}
		return result, nil, nil
	}
	if compensate := step.GetServiceProxyCompensate(); compensate != nil && !compensate.Enabled {
		return completedResponse(), nil
	}
	config, digest, target, generation, releaseID, compensated, alternate := proxyStepExpectation(step)
	if compensate := step.GetServiceProxyCompensate(); compensate != nil && compensate.PriorArtifactId != "" {
		prior := composeArtifactByID(plan, compensate.PriorArtifactId)
		if prior == nil {
			return nil, errs.New(errs.KindValidationFailed, "release prior topology artifact is missing")
		}
		priorBase, cleanup, err := releaseComposeBase(prior)
		if err != nil {
			return nil, err
		}
		defer cleanup()
		priorNames := serviceNames(prior, []string{serviceID})
		if _, failure, err := runWithComposeBase(executionCtx, taskRunner, priorBase, nil, "config", "--quiet", "--no-interpolate"); err != nil ||
			failure != nil {
			return failure, err
		}
		args := []string{"up", "--detach", "--force-recreate", "--no-deps"}
		args = append(args, priorNames...)
		if _, failure, err := runWithComposeBase(executionCtx, taskRunner, priorBase, nil, args...); err != nil ||
			failure != nil {
			return failure, err
		}
	}
	if step.GetServiceProxySwitch() != nil || step.GetServiceProxyCompensate() != nil {
		if _, failure, err := run(config, "exec", "--no-TTY", proxyName, "sh", "-ec", serviceproxy.Activate); err != nil ||
			failure != nil {
			return failure, err
		}
	}
	observed, failure, err := run(
		nil,
		"exec",
		"--no-TTY",
		proxyName,
		"wget",
		"--quiet",
		"--output-document=-",
		"http://127.0.0.1:2019/config/",
	)
	if err != nil || failure != nil {
		return failure, err
	}
	actual, err := canonicalProxyJSON(observed.Stdout)
	if err != nil {
		return failedProxyResponse(), nil
	}
	actualDigest := sha256.Sum256(actual)
	if !bytes.Equal(actualDigest[:], digest) && alternate != nil {
		alternateDigest := sha256.Sum256(alternate.config)
		if bytes.Equal(actualDigest[:], alternateDigest[:]) && bytes.Equal(alternateDigest[:], alternate.digest) {
			target, generation, releaseID, digest = alternate.target, alternate.generation, alternate.releaseID, alternate.digest
		}
	}
	if !bytes.Equal(actualDigest[:], digest) {
		return failedProxyResponse(), nil
	}
	command, failure, err := run(nil, "exec", "--no-TTY", proxyName, "cat", "/proc/1/cmdline")
	if err != nil || failure != nil {
		return failure, err
	}
	startupPath, recognized := serviceproxy.StartupConfigPath(command.Stdout)
	if !recognized {
		return failedProxyResponse(), nil
	}
	stored, failure, err := run(nil, "exec", "--no-TTY", proxyName, "cat", startupPath)
	if err != nil || failure != nil {
		return failure, err
	}
	if !proxyBytesMatch(stored.Stdout, digest) {
		return failedProxyResponse(), nil
	}
	if compensate := step.GetServiceProxyCompensate(); compensate != nil {
		if _, failure, err := run(nil, "rm", "--stop", "--force", releaseWorkloadName(artifact, serviceID, compensate.CandidateTarget)); err != nil ||
			failure != nil {
			return failure, err
		}
	}
	if value := step.GetServiceProxySwitch(); value != nil && value.PriorArtifactId != "" {
		prior := composeArtifactByID(plan, value.PriorArtifactId)
		if prior == nil {
			return nil, errs.New(errs.KindValidationFailed, "release prior topology artifact is missing")
		}
		priorBase, cleanup, err := releaseComposeBase(prior)
		if err != nil {
			return nil, err
		}
		defer cleanup()
		if _, failure, err := runWithComposeBase(executionCtx, taskRunner, priorBase, nil, "rm", "--stop", "--force", releaseWorkloadName(prior, serviceID, value.FromTarget)); err != nil ||
			failure != nil {
			return failure, err
		}
	}
	return &agentpb.ComposeHelperResponse{
		Schema: SchemaVersion, Outcome: agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED,
		Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE,
		ProxyEvidence: &agentpb.ServiceProxyEvidence{
			ServiceId: serviceID, Target: target, ProxyGeneration: generation, ConfigSha256: slices.Clone(digest),
			ReleaseId: releaseID, Compensated: compensated,
		},
	}, nil
}

type proxyAlternate struct {
	config     []byte
	digest     []byte
	target     string
	generation uint64
	releaseID  string
}

func proxyStepExpectation(step *agentpb.ExecutionStep) ([]byte, []byte, string, uint64, string, bool, *proxyAlternate) {
	if value := step.GetServiceProxySwitch(); value != nil {
		return value.ConfigJson, value.ConfigSha256, value.ToTarget, value.ProxyGeneration, value.ReleaseId, false, nil
	}
	if value := step.GetServiceProxyProbe(); value != nil {
		return value.ConfigJson, value.ConfigSha256, value.ExpectedTarget, value.ProxyGeneration, value.ReleaseId, false,
			&proxyAlternate{
				config:     value.AlternateConfigJson,
				digest:     value.AlternateConfigSha256,
				target:     value.AlternateTarget,
				generation: value.AlternateProxyGeneration,
				releaseID:  value.AlternateReleaseId,
			}
	}
	value := step.GetServiceProxyCompensate()
	return value.ConfigJson, value.ConfigSha256, value.PriorTarget, value.ProxyGeneration, value.PriorReleaseId, true, nil
}

func composeArtifactByID(plan *agentpb.ExecutionPlan, artifactID string) *agentpb.ComposeArtifact {
	for _, artifact := range plan.GetArtifacts() {
		if artifact.GetArtifactId() == artifactID {
			return artifact
		}
	}
	return nil
}

func releaseComposeBase(artifact *agentpb.ComposeArtifact) ([]string, func(), error) {
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
	if _, err := file.Write(artifact.GetCanonicalYaml()); err != nil {
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

func proxyStepServiceID(step *agentpb.ExecutionStep) string {
	if value := step.GetServiceProxySwitch(); value != nil {
		return value.ServiceId
	}
	if value := step.GetServiceProxyProbe(); value != nil {
		return value.ServiceId
	}
	if value := step.GetServiceProxyCompensate(); value != nil {
		return value.ServiceId
	}
	return ""
}

func releaseProxyName(artifact *agentpb.ComposeArtifact, serviceID string) string {
	for _, service := range artifact.GetServices() {
		if service.GetServiceId() == serviceID &&
			service.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
			return service.GetComposeName()
		}
	}
	return ""
}

func canonicalProxyJSON(value []byte) ([]byte, error) {
	var decoded map[string]any
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.UseNumber()
	if err := decoder.Decode(&decoded); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&struct{}{}); err == nil || !errors.Is(err, io.EOF) {
		return nil, errors.New("release proxy JSON has trailing data")
	}
	return json.Marshal(decoded)
}

func failedProxyResponse() *agentpb.ComposeHelperResponse {
	return &agentpb.ComposeHelperResponse{
		Schema: SchemaVersion, Outcome: agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_FAILED,
		ExitCode: 1, Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPONENT_ACTIVATION_FAILED,
	}
}
