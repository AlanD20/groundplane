package composehelper

import (
	"bytes"
	"context"
	"crypto/sha256"
	"math"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/runner"
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
	proxy, err := servingPredecessorProxy(artifact, serviceID)
	if err != nil || proxy == nil {
		return nil, errs.New(errs.KindValidationFailed, "captured Service proxy has no compiled activation procedure")
	}
	if _, err := executionplan.OpenProxyConfiguration(proxy); err != nil {
		return nil, err
	}
	input, err := composeCLIInput(artifact.GetCanonicalYaml())
	if err != nil {
		return nil, err
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
	if _, err := file.Write(input); err != nil {
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
	if switching := step.GetServiceProxySwitch(); switching != nil && switching.GetFromTarget() == "rolling" {
		if err := verifyRollingProxyHandoff(executionCtx, taskRunner, plan, artifact, switching); err != nil {
			return nil, err
		}
	}
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
	priorID := proxyStepPriorArtifact(step)
	priorArtifact := composeArtifactByID(plan, priorID)
	if priorID != "" && priorArtifact == nil {
		return nil, errs.New(errs.KindValidationFailed, "captured proxy predecessor artifact is missing")
	}
	containerID, err := ownedProxyContainer(executionCtx, taskRunner, artifact, proxy, priorArtifact)
	if err != nil {
		return nil, err
	}
	mutating := step.GetServiceProxySwitch() != nil || step.GetServiceProxyCompensate() != nil
	if mutating {
		if failure, err := activateCapturedProxy(executionCtx, taskRunner, containerID, proxy.GetProxyImplementation(), config); err != nil ||
			failure != nil {
			return failure, err
		}
	}
	observe := observeRestorationProxy
	if mutating {
		observe = awaitRestorationProxy
	}
	proven, err := observe(executionCtx, taskRunner, containerID, proxy.GetProxyImplementation(), digest)
	if err != nil {
		return nil, err
	}
	if !proven && alternate != nil && len(alternate.config) != 0 {
		alternateDigest := sha256.Sum256(alternate.config)
		if bytes.Equal(alternateDigest[:], alternate.digest) {
			proven, err = observe(
				executionCtx,
				taskRunner,
				containerID,
				proxy.GetProxyImplementation(),
				alternate.digest,
			)
			if err != nil {
				return nil, err
			}
			if proven {
				target, generation, releaseID, digest = alternate.target, alternate.generation, alternate.releaseID, alternate.digest
			}
		}
	}
	if !proven {
		return failedProxyResponse(), nil
	}
	if compensate := step.GetServiceProxyCompensate(); compensate != nil {
		if _, failure, err := run(nil, "rm", "--stop", "--force", releaseWorkloadName(artifact, serviceID, compensate.CandidateTarget)); err != nil ||
			failure != nil {
			return failure, err
		}
		for _, member := range plan.GetCandidateReleaseProcedure().GetMembers() {
			serving := member.GetServingPredecessor()
			if member.GetServiceId() != serviceID || serving.GetCompensateStepId() != step.GetStepId() ||
				serving.GetRetainedPriorArtifactId() == "" {
				continue
			}
			retained := composeArtifactByID(plan, serving.GetRetainedPriorArtifactId())
			if retained == nil {
				return nil, errs.New(errs.KindValidationFailed, "release retained topology artifact is missing")
			}
			retainedBase, cleanup, err := releaseComposeBase(retained)
			if err != nil {
				return nil, err
			}
			defer cleanup()
			if _, failure, err := runWithComposeBase(executionCtx, taskRunner, retainedBase, nil, "config", "--quiet", "--no-interpolate"); err != nil ||
				failure != nil {
				return failure, err
			}
			args := append([]string{"up", "--detach", "--no-deps"}, serviceNames(retained, []string{serviceID})...)
			if _, failure, err := runWithComposeBase(executionCtx, taskRunner, retainedBase, nil, args...); err != nil ||
				failure != nil {
				return failure, err
			}
		}
	}
	// Non-slot predecessors retire after the switch. A blue-green predecessor
	// stays in its slot until the next candidate replaces that inactive slot.
	if value := step.GetServiceProxySwitch(); value != nil && value.PriorArtifactId != "" &&
		(value.FromTarget == "singleton" || value.FromTarget == "rolling") {
		if failure, err := retireSwitchedPredecessor(executionCtx, taskRunner, plan, artifact, value); err != nil ||
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

func proxyStepPriorArtifact(step *agentpb.ExecutionStep) string {
	if value := step.GetServiceProxySwitch(); value != nil {
		return value.GetPriorArtifactId()
	}
	if value := step.GetServiceProxyProbe(); value != nil {
		return value.GetPriorArtifactId()
	}
	return step.GetServiceProxyCompensate().GetPriorArtifactId()
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
	return jcs.Canonicalize(value)
}

func failedProxyResponse() *agentpb.ComposeHelperResponse {
	return &agentpb.ComposeHelperResponse{
		Schema: SchemaVersion, Outcome: agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_FAILED,
		ExitCode: 1, Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPONENT_ACTIVATION_FAILED,
	}
}
